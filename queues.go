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

// New creates a queue manager for the database at path. Invalid options are
// rejected before anything is opened; the file itself is opened lazily, so a
// bad path surfaces from NewQueue / NewPriorityQueue.
func New(path string, opts ...StoreOption) (Queues, error) {
	cfg := defaultStoreConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.checkpointPages < 1 {
		return nil, fmt.Errorf("sqliteq: checkpoint pages must be at least 1, got %d", cfg.checkpointPages)
	}

	db, err := sql.Open(driverName(cfg.checkpointPages), dsn(path))
	if err != nil {
		return nil, fmt.Errorf("sqliteq: failed to open database: %w", err)
	}

	return &queues{
		client: db,
	}, nil
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
