package sqliteq

import (
	"database/sql"
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

	q.RequeueNoAckRows()

	return q, nil
}

// initTable initializes the queue table if it doesn't exist
func (q *Queue) initTable() error {
	createTableSQL := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %[1]s (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		data BLOB NOT NULL,
		status TEXT NOT NULL,
		ack_id TEXT UNIQUE,
		ack BOOLEAN DEFAULT 0,
		created_at TIMESTAMP,
		updated_at TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS %[2]s ON %[1]s (status, created_at);
	CREATE INDEX IF NOT EXISTS %[3]s ON %[1]s (status, ack);
	CREATE INDEX IF NOT EXISTS %[4]s ON %[1]s (ack_id);
	`,
		quoteIdent(q.tableName),
		quoteIdent(q.tableName+"_status_idx"),
		quoteIdent(q.tableName+"_status_ack_idx"),
		quoteIdent(q.tableName+"_ack_id_idx"))

	_, err := q.client.Exec(createTableSQL)
	return err
}

func (q *Queue) RequeueNoAckRows() {
	tx, err := q.client.Begin()

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	_, err = tx.Exec(
		fmt.Sprintf("UPDATE %s SET status = 'pending', updated_at = ? WHERE  status = 'processing' AND ack = 0",
			quoteIdent(q.tableName)),
		time.Now().UTC(),
	)

	err = tx.Commit()
}

// Enqueue adds an item to the queue
// It serializes the item to JSON and stores it in the database
// Returns true if the operation was successful
func (q *Queue) Enqueue(item any) bool {
	if q.closed.Load() {
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
		fmt.Sprintf("INSERT INTO %s (data, status, ack, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
			quoteIdent(q.tableName)), item, "pending", 0, now, now)
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

	// Only dequeue pending items in FIFO order
	row := tx.QueryRow(fmt.Sprintf(
		"SELECT id, data, ack_id FROM %s WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1",
		quoteIdent(q.tableName),
	))

	// Use NullString to handle NULL values from database
	var nullAckID sql.NullString

	// Scan the row data
	err = row.Scan(&id, &data, &nullAckID) // ackID may be NULL for pending items
	// Extract the string value if valid
	if nullAckID.Valid {
		ackID = nullAckID.String
	}

	if err != nil {
		return nil, false, ""
	}

	// Update the status to 'processing' or delete the item, based on withAckId
	now := time.Now().UTC()

	if withAckId {
		if ackID == "" {
			ackID = cuid.New()
		}

		// Update the item to processing status
		_, err = tx.Exec(
			fmt.Sprintf("UPDATE %s SET status = 'processing', ack_id = ?, updated_at = ? WHERE id = ?",
				quoteIdent(q.tableName)),
			ackID, now, id,
		)
	} else {
		// For regular Dequeue, just delete the item immediately
		_, err = tx.Exec(
			fmt.Sprintf("DELETE FROM %s WHERE id = ?", quoteIdent(q.tableName)),
			id,
		)
	}

	if err != nil {
		return nil, false, ""
	}

	err = tx.Commit()

	if err != nil {
		return nil, false, ""
	}

	return data, true, ackID
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
	var rowsAffected int64

	defer func() {
		if err != nil || rowsAffected == 0 {
			tx.Rollback()
		}
	}()

	var result sql.Result

	if q.removeOnComplete {
		// If removeOnComplete is true, delete the acknowledged item
		result, err = tx.Exec(
			fmt.Sprintf("DELETE FROM %s WHERE ack_id = ? ", quoteIdent(q.tableName)),
			ackID,
		)
	} else {
		// Otherwise, mark it as completed and set ack to 1 (true in SQLite)
		result, err = tx.Exec(
			fmt.Sprintf("UPDATE %s SET status = 'completed', ack = 1, updated_at = ? WHERE ack_id = ?", quoteIdent(q.tableName)),
			time.Now().UTC(), ackID,
		)
	}

	if err != nil {
		return false
	}

	rowsAffected, err = result.RowsAffected()

	if err != nil || rowsAffected == 0 {
		return false
	}

	err = tx.Commit()

	return err == nil
}

// Len returns the number of pending items in the queue
func (q *Queue) Len() int {
	var count int
	row := q.client.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = 'pending'", quoteIdent(q.tableName)))
	err := row.Scan(&count)
	if err != nil {
		return 0
	}
	return count
}

// Values returns all pending items in the queue
func (q *Queue) Values() []any {
	rows, err := q.client.Query(fmt.Sprintf("SELECT data FROM %s WHERE status = 'pending' ORDER BY created_at ASC", quoteIdent(q.tableName)))
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

		// Now we just add the byte array directly as we're storing byte arrays
		// instead of JSON-serialized data
		items = append(items, data)
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

	_, err = tx.Exec(fmt.Sprintf("DELETE FROM %s", quoteIdent(q.tableName)))
	if err != nil {
		return
	}

	err = tx.Commit()
}

// Close closes the queue and its database connection
func (q *Queue) Close() error {
	q.closed.Store(true)

	return nil
}
