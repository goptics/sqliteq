package sqliteq

import (
	"errors"
	"time"
)

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
	heartbeat       time.Duration
}

func defaultStoreConfig() storeConfig {
	return storeConfig{checkpointPages: 1000, heartbeat: defaultHeartbeat}
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

// WithHeartbeatTimeout sets how long this process may go without renewing
// its heartbeat in a database file before other processes on the file take
// it for dead and requeue the items it dequeued and has not acknowledged
// (default 30s, at least 5s; New rejects anything lower). The heartbeat is
// renewed every sixth of the timeout, so it runs out only when the process
// dies or freezes. It does not limit how long a job may run.
//
// A renewal waits up to 5s (the busy timeout) behind another process's
// write, so keep the timeout well above that on a file other processes
// write to. A job still running when its process's heartbeat runs out runs
// again elsewhere. Processes with different timeouts can share a file.
func WithHeartbeatTimeout(d time.Duration) StoreOption {
	return func(c *storeConfig) {
		c.heartbeat = d
	}
}
