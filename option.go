package sqliteq

import "errors"

// errNilOption is returned by New, NewQueue and NewPriorityQueue for a nil
// option, which they would otherwise call
var errNilOption = errors.New("sqliteq: option must not be nil")

// Option is a function type that can be used to configure a Queue
type Option func(*Queue)

// WithRemoveOnComplete sets whether acknowledged items should be deleted
// from the database when true, or just marked as completed when false
func WithRemoveOnComplete(remove bool) Option {
	return func(q *Queue) {
		q.removeOnComplete = remove
	}
}

// storeConfig holds the settings shared by every queue of one store.
type storeConfig struct {
	singleFile      bool
	checkpointPages int
}

func defaultStoreConfig() storeConfig {
	return storeConfig{checkpointPages: 1000}
}

// StoreOption is a function type that can be used to configure a store passed to New
type StoreOption func(*storeConfig)

// WithSingleFile keeps every queue in one SQLite database file at the given path
func WithSingleFile() StoreOption {
	return func(c *storeConfig) {
		c.singleFile = true
	}
}

// WithCheckpointPages sets the WAL size, in pages, at which SQLite checkpoints
// automatically. It must be at least 1; New rejects anything lower.
func WithCheckpointPages(n int) StoreOption {
	return func(c *storeConfig) {
		c.checkpointPages = n
	}
}
