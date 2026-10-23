package sqliteq

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type queues struct {
	cfg    storeConfig
	root   string             // folder, or the resolved file under WithSingleFile
	files  map[*file]struct{} // one reference held per file
	closed bool
	mu     sync.Mutex
}

type Queues interface {
	NewQueue(queueKey string, opts ...Option) (*Queue, error)
	NewPriorityQueue(queueKey string, opts ...Option) (*PriorityQueue, error)
	Close() error
}

// New creates a queue manager. By default path is a folder, created if
// missing, holding one <name>.db file per queue. With WithSingleFile, path is
// one database file holding every queue as a table. Files open lazily in
// NewQueue / NewPriorityQueue.
func New(path string, opts ...StoreOption) (Queues, error) {
	cfg := defaultStoreConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.checkpointPages < 1 {
		return nil, fmt.Errorf("sqliteq: checkpoint pages must be at least 1, got %d", cfg.checkpointPages)
	}

	root, err := layout(path, cfg.singleFile)
	if err != nil {
		return nil, err
	}

	return &queues{
		cfg:   cfg,
		root:  root,
		files: map[*file]struct{}{},
	}, nil
}

// layout checks path against the chosen layout and returns the store root.
func layout(path string, singleFile bool) (string, error) {
	info, err := os.Stat(path)
	exists := err == nil

	if singleFile {
		if exists && info.IsDir() {
			return "", fmt.Errorf("sqliteq: %s is a folder; WithSingleFile needs a file path", path)
		}
		return resolve(path)
	}

	if exists && !info.IsDir() {
		return "", fmt.Errorf("sqliteq: %s is a file; pass WithSingleFile() to open it as one database", path)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("sqliteq: %w", err)
	}
	// Names cannot hold ? or #, so only the folder can; resolve re-checks per file
	if _, err := checkDSN(abs); err != nil {
		return "", err
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return "", fmt.Errorf("sqliteq: failed to create folder: %w", err)
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", fmt.Errorf("sqliteq: %w", err)
	}
	if _, err := checkDSN(real); err != nil {
		return "", err
	}
	return abs, nil
}

// filePath returns the resolved database file for a queue name.
func (qs *queues) filePath(name string) (string, error) {
	if qs.cfg.singleFile {
		return qs.root, nil
	}
	if err := validName(name); err != nil {
		return "", err
	}
	return resolve(filepath.Join(qs.root, foldName(name)+".db"))
}

// open finds or creates the handle for name, taking a reference on its file
// the first time this Queues reaches that file.
func (qs *queues) open(name string, priority bool, opts []Option) (*handle, error) {
	path, err := qs.filePath(name)
	if err != nil {
		return nil, err
	}

	qs.mu.Lock()
	defer qs.mu.Unlock()

	if qs.closed {
		return nil, errors.New("sqliteq: queues are closed")
	}

	var f *file
	for held := range qs.files {
		if held.key == path {
			f = held
			break
		}
	}

	fresh := f == nil
	if fresh {
		if f, err = acquire(path, qs.cfg.checkpointPages); err != nil {
			return nil, err
		}
	}

	h, err := f.open(name, priority, opts)
	if err != nil {
		if fresh {
			err = errors.Join(err, f.release())
		}
		return nil, err
	}

	qs.files[f] = struct{}{}
	return h, nil
}

func (qs *queues) NewQueue(queueKey string, opts ...Option) (*Queue, error) {
	h, err := qs.open(queueKey, false, opts)
	if err != nil {
		return nil, err
	}
	return h.q, nil
}

func (qs *queues) NewPriorityQueue(queueKey string, opts ...Option) (*PriorityQueue, error) {
	h, err := qs.open(queueKey, true, opts)
	if err != nil {
		return nil, err
	}
	return h.pq, nil
}

// Close releases every file this Queues opened; a file closes when no other
// Queues in the process holds it.
func (qs *queues) Close() error {
	qs.mu.Lock()
	defer qs.mu.Unlock()

	if qs.closed {
		return nil
	}
	qs.closed = true

	var errs []error
	for f := range qs.files {
		errs = append(errs, f.release())
	}
	qs.files = nil
	return errors.Join(errs...)
}
