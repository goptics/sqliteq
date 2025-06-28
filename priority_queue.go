package sqliteq

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/lucsky/cuid"
)

// PriorityQueue extends Queue with priority-based dequeuing
type PriorityQueue struct {
	*Queue
}

// newPriorityQueue creates a new SQLite-based priority queue
func newPriorityQueue(db *sql.DB, tableName string, opts ...Option) (*PriorityQueue, error) {
	baseQueue, err := newQueue(db, tableName, opts...)
	if err != nil {
		return nil, err
	}

	pq := &PriorityQueue{
		Queue: baseQueue,
	}

	// Add the priority column if it doesn't exist
	if err := pq.initPriorityColumn(); err != nil {
		return nil, fmt.Errorf("failed to initialize priority column: %w", err)
	}

	return pq, nil
}

// initPriorityColumn adds the priority column to the table if it doesn't exist
func (pq *PriorityQueue) initPriorityColumn() error {
	// Check if priority column exists
	var name string
	err := pq.client.QueryRow(fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(pq.tableName))).Scan(nil, &name, nil, nil, nil, nil)

	if err != nil || name != "priority" {
		// Add priority column with default value 0
		_, err := pq.client.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN priority INTEGER NOT NULL DEFAULT 0", quoteIdent(pq.tableName)))
		if err != nil {
			return err
		}

		// Create index on priority (ASC for lower numbers = higher priority)
		_, err = pq.client.Exec(fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (priority ASC, created_at ASC)", quoteIdent(pq.tableName+"_priority_idx"), quoteIdent(pq.tableName)))
		if err != nil {
			return err
		}
	}

	return nil
}

// Enqueue adds an item to the queue with a specified priority
// Lower priority numbers will be dequeued first (0 is highest priority)
// Returns true if the operation was successful
func (pq *PriorityQueue) Enqueue(item any, priority int) bool {
	if pq.closed.Load() {
		return false
	}

	now := time.Now().UTC()
	tx, err := pq.client.Begin()

	if err != nil {
		return false
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	_, err = tx.Exec(
		fmt.Sprintf("INSERT INTO %s (data, status, created_at, updated_at, priority) VALUES (?, ?, ?, ?, ?)", quoteIdent(pq.tableName)),
		item, "pending", now, now, priority,
	)

	if err != nil {
		return false
	}

	err = tx.Commit()

	return err == nil
}

// dequeueInternal overrides the base dequeueInternal method to consider priority
func (pq *PriorityQueue) dequeueInternal(withAckId bool) (any, bool, string) {
	if pq.closed.Load() {
		return nil, false, ""
	}

	tx, err := pq.client.Begin()

	if err != nil {
		return nil, false, ""
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Get the highest priority pending item (lower priority numbers come first)
	var id int64
	var data []byte
	row := tx.QueryRow(fmt.Sprintf(
		"SELECT id, data FROM %s WHERE status = 'pending' ORDER BY priority ASC, created_at ASC LIMIT 1",
		quoteIdent(pq.tableName),
	))
	err = row.Scan(&id, &data)
	if err != nil {
		return nil, false, ""
	}

	// Update the status to 'processing' with ack ID or remove directly if no ack ID
	now := time.Now().UTC()
	var ackID string

	if withAckId {
		ackID = cuid.New()

		_, err = tx.Exec(
			fmt.Sprintf("UPDATE %s SET status = 'processing', ack_id = ?, updated_at = ? WHERE id = ?", quoteIdent(pq.tableName)),
			ackID, now, id,
		)
	} else {
		// remove the row if there is no ack
		_, err = tx.Exec(
			fmt.Sprintf("DELETE FROM %s WHERE id = ?", quoteIdent(pq.tableName)),
			id,
		)
	}

	if err != nil {
		return nil, false, ""
	}

	// Commit transaction
	err = tx.Commit()

	if err != nil {
		return nil, false, ""
	}

	return data, true, ackID
}

// Dequeue overrides the base Dequeue method to use priority-based dequeuing
func (pq *PriorityQueue) Dequeue() (any, bool) {
	item, success, _ := pq.dequeueInternal(false)
	return item, success
}

// DequeueWithAckId overrides the base DequeueWithAckId method to use priority-based dequeuing
func (pq *PriorityQueue) DequeueWithAckId() (any, bool, string) {
	return pq.dequeueInternal(true)
}
