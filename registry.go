package sqliteq

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/mattn/go-sqlite3"
)

// file is one open SQLite database file, shared by every Queues in the
// process that reaches it.
type file struct {
	key       string // resolved path, the registry key
	db        *sql.DB
	pages     int                // checkpoint pages it was opened with
	refs      int                // Queues values holding it; under registry.mu
	mu        sync.Mutex         // write mutex (used from Task 6)
	handles   map[string]*handle // lower-cased queue name -> handle; under registry.mu
	recovered map[string]bool    // lower-cased table -> recovery done; under registry.mu
}

// handle is the queue opened under one name in a file; exactly one is set.
type handle struct {
	q  *Queue
	pq *PriorityQueue
}

var (
	driversMu sync.Mutex
	drivers   = map[int]string{}
)

// driverName returns the name of a registered mattn driver whose ConnectHook
// runs "PRAGMA wal_autocheckpoint=<pages>" on every new connection. The
// driver is registered once per pages value, because sql.Register panics on
// a duplicate name.
func driverName(pages int) string {
	driversMu.Lock()
	defer driversMu.Unlock()

	if name, ok := drivers[pages]; ok {
		return name
	}

	name := fmt.Sprintf("sqliteq_ckpt_%d", pages)
	pragma := fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", pages)
	sql.Register(name, &sqlite3.SQLiteDriver{
		ConnectHook: func(c *sqlite3.SQLiteConn) error {
			_, err := c.Exec(pragma, nil)
			return err
		},
	})
	drivers[pages] = name
	return name
}

// dsn returns the connection string for a database file: WAL, relaxed
// fsync (safe under WAL) and a busy timeout so writers wait for each other.
func dsn(path string) string {
	return path + "?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000"
}

// registry holds every open file in the process, keyed by resolved path.
// Lock order is queues.mu, then registry.mu, then file.mu.
var registry = struct {
	mu    sync.Mutex
	files map[string]*file
}{files: map[string]*file{}}

// acquire returns the registry entry for the resolved path, opening the pool
// on first use, and takes one reference on it.
func acquire(path string, pages int) (*file, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if f, ok := registry.files[path]; ok {
		if f.pages != pages {
			return nil, fmt.Errorf("sqliteq: %s is already open with checkpoint pages %d, not %d", path, f.pages, pages)
		}
		f.refs++
		return f, nil
	}

	// sql.Open only validates its arguments; no connection is made here
	db, err := sql.Open(driverName(pages), dsn(path))
	if err != nil {
		return nil, fmt.Errorf("sqliteq: failed to open database: %w", err)
	}

	f := &file{
		key:       path,
		db:        db,
		pages:     pages,
		refs:      1,
		handles:   map[string]*handle{},
		recovered: map[string]bool{},
	}
	registry.files[path] = f
	return f, nil
}

// release drops one reference; the last one closes the pool and the entry.
func (f *file) release() error {
	registry.mu.Lock()
	f.refs--
	if f.refs > 0 {
		registry.mu.Unlock()
		return nil
	}
	if registry.files[f.key] == f {
		delete(registry.files, f.key)
	}
	registry.mu.Unlock()

	// Close waits for running queries, so it runs outside the lock
	return f.db.Close()
}

// open returns the handle for name, creating it if there is none or the
// existing one was closed. Creation holds registry.mu across table init and
// recovery, so no handle on a table is handed out before its recovery ends.
func (f *file) open(name string, priority bool, opts []Option) (*handle, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	want := Queue{removeOnComplete: true}
	for _, opt := range opts {
		opt(&want)
	}

	key := foldName(name)
	if h, ok := f.handles[key]; ok {
		if (h.pq != nil) != priority {
			return nil, fmt.Errorf("sqliteq: %q is already open as a %s", name, h.kind())
		}
		q := h.queue()
		if !q.closed.Load() {
			if q.removeOnComplete != want.removeOnComplete {
				return nil, fmt.Errorf("sqliteq: %q is already open with WithRemoveOnComplete(%t)", name, q.removeOnComplete)
			}
			return h, nil
		}
		// A closed handle is replaced on the same table, as first named
		name = q.tableName
	}

	h := &handle{}
	if priority {
		pq, err := newPriorityQueue(f, name, opts...)
		if err != nil {
			return nil, err
		}
		h.pq = pq
	} else {
		q, err := newQueue(f, name, opts...)
		if err != nil {
			return nil, err
		}
		h.q = q
	}

	if !f.recovered[key] {
		if err := h.queue().requeueNoAck(); err != nil {
			return nil, fmt.Errorf("failed to requeue unacknowledged rows: %w", err)
		}
		f.recovered[key] = true
	}

	f.handles[key] = h
	return h, nil
}

// queue returns the handle's Queue, the base of a PriorityQueue.
func (h *handle) queue() *Queue {
	if h.pq != nil {
		return h.pq.Queue
	}
	return h.q
}

func (h *handle) kind() string {
	if h.pq != nil {
		return "priority queue"
	}
	return "queue"
}

// resolve returns the registry key for a database path: the absolute path
// with symlinks evaluated (the file's own too, once it exists), so every
// spelling of one file meets one entry. The folder must exist.
func resolve(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("sqliteq: %w", err)
	}
	if real, err := filepath.EvalSymlinks(abs); err == nil {
		return checkDSN(real)
	}
	dir, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return "", fmt.Errorf("sqliteq: %w", err)
	}
	return checkDSN(filepath.Join(dir, filepath.Base(abs)))
}

// checkDSN rejects paths that dsn would turn into a broken connection string.
func checkDSN(path string) (string, error) {
	if strings.ContainsAny(path, "?#") {
		return "", fmt.Errorf("sqliteq: path %q must not contain ? or #", path)
	}
	return path, nil
}
