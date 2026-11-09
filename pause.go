package sqliteq

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Queue pause (varmq QueuePauser and PauseNotifier; spec redis-storage.md
// 6.7, 6.8) on SQLite. The flag is row paused_at (ms) of
// <name>__sqliteq_meta: every pop statement selects nothing while it
// exists, so a Pause from any process on the file stops hand-outs at once,
// and it outlives restarts. Paused is this object's cache: set by its own
// Pause and Resume, by a pop the row refused, and, while it is true, by
// the poll, which reads the row every pollEvery, so a Resume from another
// process reaches it within pollEvery.

// pauseState is a Queue's Paused cache and OnPauseChange hooks. mu makes
// each gen check and cache swap one step, so a stale read never overwrites
// a newer Pause or Resume of this object.
type pauseState struct {
	paused   atomic.Bool   // written under mu
	gen      atomic.Uint64 // moves at each Pause and Resume, after its write
	refusals atomic.Uint64 // pops refused for a pause
	mu       sync.Mutex
	hooks    map[*func(bool)]struct{}
}

// Pause stops every pop of the queue, in every process on the file, until
// Resume. Adds, acks, rejects and recovery work as before; Purge keeps the
// pause. Idempotent: the first pause time is kept. It returns once the row
// is stored.
func (q *Queue) Pause() error {
	return q.setPauseRow(true, fmt.Sprintf("INSERT OR IGNORE INTO %s (key, value) VALUES ('paused_at', ?)", q.meta), time.Now().UnixMilli())
}

// Resume undoes Pause. Idempotent.
func (q *Queue) Resume() error {
	return q.setPauseRow(false, fmt.Sprintf("DELETE FROM %s WHERE key = 'paused_at'", q.meta))
}

// setPauseRow runs a Pause or Resume write and sets the cache to p.
func (q *Queue) setPauseRow(p bool, query string, args ...any) error {
	if q.closed.Load() {
		return errClosed
	}
	q.f.mu.Lock()
	_, err := q.f.db.Exec(query, args...)
	q.f.mu.Unlock()
	if err != nil {
		return err
	}
	q.setPaused(p, q.pz.gen.Add(1))
	return nil
}

// Paused reports whether the queue is paused, as this object last saw it.
// No I/O.
func (q *Queue) Paused() bool {
	return q.pz.paused.Load()
}

// PauseRefusals counts this object's pops refused for a pause (varmq's
// optional QueuePauser method), so varmq tells a refusal racing a Resume
// from a failed pop.
func (q *Queue) PauseRefusals() uint64 {
	return q.pz.refusals.Load()
}

// OnPauseChange implements varmq.PauseNotifier: fn gets the cache each time
// this object sees it change (its own Pause or Resume, a refused pop, the
// poll), until cancel. fn runs on the goroutine that saw the change, never
// under a lock of this package, and must not block; racing changes can
// reach fn out of order, never with a value the cache did not hold.
func (q *Queue) OnPauseChange(fn func(paused bool)) (cancel func()) {
	h := &fn
	q.pz.mu.Lock()
	if q.pz.hooks == nil {
		q.pz.hooks = map[*func(bool)]struct{}{}
	}
	q.pz.hooks[h] = struct{}{}
	q.pz.mu.Unlock()

	return func() {
		q.pz.mu.Lock()
		delete(q.pz.hooks, h)
		q.pz.mu.Unlock()
	}
}

// setPaused sets the cache to p if gen is still g: the gen of the call
// that read p (a pop or the poll; a Pause or Resume since may be newer than
// p), or the one this object's own Pause or Resume moved it to. On a
// change, after mu, it starts the poll (paused) and calls the hooks.
func (q *Queue) setPaused(p bool, g uint64) {
	q.pz.mu.Lock()
	if q.pz.gen.Load() != g {
		q.pz.mu.Unlock()
		return
	}
	if q.pz.paused.Swap(p) == p {
		q.pz.mu.Unlock()
		return
	}
	fns := make([]func(bool), 0, len(q.pz.hooks))
	for h := range q.pz.hooks {
		fns = append(fns, *h)
	}
	q.pz.mu.Unlock()

	if p {
		w := &q.w
		w.mu.Lock()
		if !w.closed {
			w.startLocked(q)
		}
		w.mu.Unlock()
	}
	for _, fn := range fns {
		fn(p)
	}
}

// refused records a pop refused for a pause, by a call that began at gen g.
func (q *Queue) refused(g uint64) {
	q.pz.refusals.Add(1)
	q.setPaused(true, g)
}

// pollPause reads the pause row for the poll.
func (q *Queue) pollPause() {
	g := q.pz.gen.Load()
	if paused, err := readPaused(q.pausedAt); err == nil {
		q.setPaused(paused, g)
	}
}

// readPaused reports whether the pause row exists, through stmt
// (q.pausedAt, maybe in a tx).
func readPaused(stmt *sql.Stmt) (bool, error) {
	var at int64
	err := stmt.QueryRow().Scan(&at)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}
