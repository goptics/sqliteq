package sqliteq

import (
	"database/sql"
	"sync"
	"sync/atomic"
)

// lazyStmt is a statement prepared on first use. db.Prepare prepares on one
// pool connection at once, and a prepared statement holds lookaside slots
// there for good; with the lookaside full, every small alloc of that
// connection goes to malloc under SQLite's global mem mutex, which all
// files' connections share. A statement no call runs costs nothing.
type lazyStmt struct {
	db    *sql.DB
	query string
	s     atomic.Pointer[sql.Stmt]
	mu    sync.Mutex
}

// get returns the statement, preparing it on the first call; a failed
// prepare is tried again on the next call.
func (l *lazyStmt) get() (*sql.Stmt, error) {
	if s := l.s.Load(); s != nil {
		return s, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if s := l.s.Load(); s != nil {
		return s, nil
	}
	s, err := l.db.Prepare(l.query)
	if err != nil {
		return nil, err
	}
	l.s.Store(s)
	return s, nil
}

// in returns the statement in tx; outside one with a nil tx.
func (l *lazyStmt) in(tx *sql.Tx) (*sql.Stmt, error) {
	s, err := l.get()
	if err != nil || tx == nil {
		return s, err
	}
	return tx.Stmt(s), nil
}

// close closes the statement if it was prepared.
func (l *lazyStmt) close() {
	if s := l.s.Swap(nil); s != nil {
		s.Close()
	}
}
