package sqliteq

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mattn/go-sqlite3"
)

// file is one open SQLite database file, shared by every Queues in the
// process that reaches it.
type file struct {
	key       string // resolved path, the registry key
	scope     string // "sqliteq:" + store root: the folder, or the file under WithSingleFile
	db        *sql.DB
	pages     int                // checkpoint pages it was opened with
	refs      int                // Queues values holding it; under registry.mu
	mu        sync.Mutex         // write mutex: every write on the file holds it, reads do not
	b         *batcher           // group commit for enqueues and acks
	rounds    atomic.Int64       // rounds committed by b; read by tests
	handles   map[string]*handle // lower-cased queue name -> handle; under registry.mu
	recovered map[string]bool    // lower-cased table -> recovery done; under registry.mu

	// Owner and heartbeat (see owner.go)
	owner     string                   // this process's id on the file; on the rows it claims
	hb        time.Duration            // heartbeat timeout
	claimed   bool                     // the owner row is written; under mu
	lastBeat  int64                    // Unix ms of the last owner row write; under mu
	closing   bool                     // the last release began: no claim, no sweep; under mu
	sweeping  bool                     // sweepLoop runs; under registry.mu
	swept     atomic.Pointer[[]*Queue] // the handles' queues, for sweepLoop; stored under registry.mu
	seen      bool                     // seenMoved is set; under mu
	seenMoved int64                    // the file's requeue count at the last sweep; under mu
	stop      chan struct{}            // closed by the last release
	done      chan struct{}            // closed when sweepLoop returns
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
// Transactions begin IMMEDIATE: one that reads before it writes (CancelJob,
// a gated pop) would otherwise fail with SQLITE_BUSY, not wait, when another
// process wrote in between.
func dsn(path string) string {
	return path + "?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000&_txlock=immediate"
}

// registry holds every open file in the process, keyed by resolved path.
// Lock order is queues.mu, then registry.mu, then file.mu. A file's
// batcher.mu is never held across a commit, so never together with file.mu.
// A workflow store's write transaction holds file.mu, so it never takes
// queues.mu: queues.handleOf reads an atomic copy-on-write map.
var registry = struct {
	mu    sync.Mutex
	files map[string]*file
}{files: map[string]*file{}}

// acquire returns the registry entry for the resolved path, opening the pool
// on first use, and takes one reference on it.
func acquire(path, root string, pages int, hb time.Duration) (*file, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if f, ok := registry.files[path]; ok {
		if f.scope != "sqliteq:"+root {
			return nil, fmt.Errorf("sqliteq: %s is already open under scope %s, not sqliteq:%s", path, f.scope, root)
		}
		if f.pages != pages {
			return nil, fmt.Errorf("sqliteq: %s is already open with checkpoint pages %d, not %d", path, f.pages, pages)
		}
		if f.hb != hb {
			return nil, fmt.Errorf("sqliteq: %s is already open with heartbeat timeout %s, not %s", path, f.hb, hb)
		}
		f.refs++
		return f, nil
	}

	f, err := newFile(path, root, pages, hb)
	if err != nil {
		return nil, err
	}
	registry.files[path] = f
	return f, nil
}

// newFile opens the pool of a database file under a new owner id, holding
// one reference. It is not in the registry.
func newFile(path, root string, pages int, hb time.Duration) (*file, error) {
	// sql.Open only validates its arguments; no connection is made here
	db, err := sql.Open(driverName(pages), dsn(path))
	if err != nil {
		return nil, fmt.Errorf("sqliteq: failed to open database: %w", err)
	}
	// Idle connections are kept, so none reopens, reruns the hook and re-prepares
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)

	f := &file{
		key:       path,
		scope:     "sqliteq:" + root,
		db:        db,
		pages:     pages,
		refs:      1,
		handles:   map[string]*handle{},
		recovered: map[string]bool{},
		owner:     newOwnerID(),
		hb:        hb,
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	f.b = &batcher{commit: f.commitRound}
	f.swept.Store(&[]*Queue{})
	return f, nil
}

// release drops one reference; the last one stops the sweep, hands this
// owner's unacknowledged rows back and closes the pool and the entry.
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
	// Stop the queues' polls before the pool closes under them
	tables := make([]string, 0, len(f.handles))
	for _, h := range f.handles {
		h.queue().Close()
		tables = append(tables, h.queue().table)
	}
	sweeping := f.sweeping
	f.sweeping = false // a repeated release stops nothing twice
	registry.mu.Unlock()

	f.mu.Lock()
	f.closing = true
	f.mu.Unlock()
	if sweeping {
		close(f.stop)
		<-f.done
	}
	f.handBack(tables)

	// Close waits for running queries, so it runs outside the lock
	return f.db.Close()
}

// open returns the handle for name, creating it if there is none or the
// existing one was closed. Creation holds registry.mu across table init and
// the table's first sweep, so no handle on a table is handed out before it
// ends. The first table starts the file's sweep loop.
func (f *file) open(name string, priority bool, opts []Option) (*handle, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	want := Queue{removeOnComplete: true}
	for _, opt := range opts {
		if opt == nil {
			return nil, errNilOption
		}
		opt(&want)
	}

	key := foldName(name)
	// A queue's indexes, sidecar tables and trigger are named <name>__sqliteq_*
	if strings.Contains(key, "__sqliteq_") {
		return nil, fmt.Errorf("sqliteq: queue name %q must not contain __sqliteq_", name)
	}
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
		name = q.name
	}

	h := &handle{}
	if priority {
		pq, err := newPriorityQueue(f, name, opts...)
		if err != nil {
			return nil, err
		}
		h.pq = pq
	} else {
		q, err := newQueue(f, name, false, opts...)
		if err != nil {
			return nil, err
		}
		h.q = q
	}

	// First open: rows of dead owners, and rows an older version left
	// without an owner, go back to pending
	if !f.recovered[key] {
		if _, err := f.sweep([]*Queue{h.queue()}, true); err != nil {
			h.queue().Close() // stops a pause poll
			return nil, fmt.Errorf("failed to requeue unacknowledged rows: %w", err)
		}
		f.recovered[key] = true
	}

	f.handles[key] = h
	qs := make([]*Queue, 0, len(f.handles))
	for _, h := range f.handles {
		qs = append(qs, h.queue())
	}
	f.swept.Store(&qs)
	if !f.sweeping {
		f.sweeping = true
		go f.sweepLoop()
	}
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
