package sqliteq

import (
	"database/sql"
	"fmt"
)

type queues struct {
	client *sql.DB
}

type Queues interface {
	NewQueue(queueKey string, opts ...Option) (*Queue, error)
	NewPriorityQueue(queueKey string, opts ...Option) (*PriorityQueue, error)
	Close() error
}

func New(dbPath string) Queues {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		panic(fmt.Sprintf("failed to open database: %v", err))
	}

	// Enable WAL mode for better concurrency
	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		db.Close()
		panic(fmt.Sprintf("failed to enable WAL mode: %v", err))
	}

	return &queues{
		client: db,
	}
}

func (q *queues) NewQueue(queueKey string, opts ...Option) (*Queue, error) {
	return newQueue(q.client, queueKey, opts...)
}

func (q *queues) NewPriorityQueue(queueKey string, opts ...Option) (*PriorityQueue, error) {
	return newPriorityQueue(q.client, queueKey, opts...)
}

func (q *queues) Close() error {
	return q.client.Close()
}
