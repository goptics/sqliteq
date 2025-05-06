package sqliteq

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/lucsky/cuid"
	_ "github.com/mattn/go-sqlite3"
)

// Queue implements the Queue interface using SQLite as the storage backend
type Queue struct {
	client           *sql.DB
	tableName        string
	removeOnComplete bool
	closed           atomic.Bool
}

// newQueue creates a new SQLite-based queue
func newQueue(db *sql.DB, tableName string, opts ...Option) (*Queue, error) {
	q := &Queue{
		client:           db,
		tableName:        tableName,
		removeOnComplete: true, // Default to removing completed items
	}

	// Apply any provided options
	for _, opt := range opts {
		opt(q)
	}

	if err := q.initTable(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize table: %w", err)
	}

	return q, nil
}

// initTable initializes the queue table if it doesn't exist
func (q *Queue) initTable() error {
	createTableSQL := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		data BLOB NOT NULL,
		status TEXT NOT NULL,
		ack_id TEXT UNIQUE,
		created_at TIMESTAMP,
		updated_at TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS %s_status_idx ON %s (status, created_at);
	`, q.tableName, q.tableName, q.tableName)

	_, err := q.client.Exec(createTableSQL)
	return err
}

// Enqueue adds an item to the queue
// It serializes the item to JSON and stores it in the database
// Returns true if the operation was successful
func (q *Queue) Enqueue(item any) bool {
	if q.closed.Load() {
		return false
	}

	data, err := json.Marshal(item)
	if err != nil {
		return false
	}

	now := time.Now().UTC()
	tx, err := q.client.Begin()
	if err != nil {
		return false
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	_, err = tx.Exec(
		fmt.Sprintf("INSERT INTO %s (data, status, created_at, updated_at) VALUES (?, ?, ?, ?)", q.tableName),
		data, "pending", now, now,
	)
	if err != nil {
		return false
	}

	err = tx.Commit()
	return err == nil
}

// dequeueInternal is a helper function for both Dequeue and DequeueWithAckId
// It handles the common operations of finding and retrieving an item from the queue
// If withAckId is true, it will generate and store an ack ID
func (q *Queue) dequeueInternal(withAckId bool) (item any, success bool, ackID string) {
	if q.closed.Load() {
		return nil, false, ""
	}

	tx, err := q.client.Begin()
	if err != nil {
		return nil, false, ""
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Get the oldest pending item
	var id int64
	var data []byte
	row := tx.QueryRow(fmt.Sprintf(
		"SELECT id, data FROM %s WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1",
		q.tableName,
	))
	err = row.Scan(&id, &data)
	if err != nil {
		if err == sql.ErrNoRows {
			tx.Rollback()
			return nil, false, ""
		}
		tx.Rollback()
		return nil, false, ""
	}

	// Update the status to 'processing', with or without ack ID
	now := time.Now().UTC()
	if withAckId {
		ackID = cuid.New()

		_, err = tx.Exec(
			fmt.Sprintf("UPDATE %s SET status = 'processing', ack_id = ?, updated_at = ? WHERE id = ?", q.tableName),
			ackID, now, id,
		)
	} else {
		// remove the row if there is no ack
		_, err = tx.Exec(
			fmt.Sprintf("DELETE FROM %s WHERE id = ?", q.tableName),
			id,
		)
	}

	if err != nil {
		tx.Rollback()
		return nil, false, ""
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		return nil, false, ""
	}

	// Unmarshal the item
	var unmarshaledItem any
	err = json.Unmarshal(data, &unmarshaledItem)
	if err != nil {
		return nil, false, ""
	}

	return unmarshaledItem, true, ackID
}

// Dequeue removes and returns the next item from the queue
// Returns the item and a boolean indicating if the operation was successful
func (q *Queue) Dequeue() (any, bool) {
	item, success, _ := q.dequeueInternal(false)
	return item, success
}

// DequeueWithAckId removes and returns the next item from the queue with an acknowledgment ID
// Returns the item, a boolean indicating if the operation was successful, and the acknowledgment ID
func (q *Queue) DequeueWithAckId() (any, bool, string) {
	return q.dequeueInternal(true)
}

// Acknowledge marks an item as completed
// Returns true if the item was successfully acknowledged, false otherwise
func (q *Queue) Acknowledge(ackID string) bool {
	tx, err := q.client.Begin()
	if err != nil {
		return false
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	var result sql.Result

	if q.removeOnComplete {
		// If removeOnComplete is true, delete the acknowledged item
		result, err = tx.Exec(
			fmt.Sprintf("DELETE FROM %s WHERE ack_id = ? AND status = 'processing'", q.tableName),
			ackID,
		)
	} else {
		// Otherwise, just mark it as completed
		now := time.Now().UTC()
		result, err = tx.Exec(
			fmt.Sprintf("UPDATE %s SET status = 'completed', updated_at = ? WHERE ack_id = ? AND status = 'processing'", q.tableName),
			now, ackID,
		)
	}

	if err != nil {
		tx.Rollback()
		return false
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil || rowsAffected == 0 {
		tx.Rollback()
		return false
	}

	return tx.Commit() == nil
}

// Len returns the number of pending items in the queue
func (q *Queue) Len() int {
	var count int
	row := q.client.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = 'pending'", q.tableName))
	err := row.Scan(&count)
	if err != nil {
		return 0
	}
	return count
}

// Values returns all pending items in the queue
func (q *Queue) Values() []any {
	rows, err := q.client.Query(fmt.Sprintf("SELECT data FROM %s WHERE status = 'pending' ORDER BY created_at ASC", q.tableName))
	if err != nil {
		return nil
	}
	defer rows.Close()

	var items []any
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			continue
		}

		var item any
		if err := json.Unmarshal(data, &item); err != nil {
			continue
		}
		items = append(items, item)
	}

	return items
}

// Purge removes all items from the queue
func (q *Queue) Purge() {
	tx, err := q.client.Begin()
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	_, err = tx.Exec(fmt.Sprintf("DELETE FROM %s", q.tableName))
	if err != nil {
		tx.Rollback()
		return
	}

	err = tx.Commit()
}

// Close closes the queue and its database connection
func (q *Queue) Close() error {
	q.closed.Store(true)

	return nil
}
