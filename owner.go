package sqliteq

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// Owners and heartbeats (redisq's model, on SQLite). Each file in a process
// has a random owner id, stored on every row it claims. An owner that
// claims keeps a row in __sqliteq_owners, renewed by the sweep; a process
// that never claims writes none. The sweep runs on a table's first open and
// then every hb/6 for every table the process has open: it requeues the
// processing rows whose owner has no live row. SQLite has no presence
// signal, so the heartbeat is the only liveness: a killed owner's rows come
// back one timeout after its last beat.
//
// Times are the host's wall clock in Unix ms. __sqliteq_clock holds when
// any process last swept; see sweep for the gap guard.

const (
	defaultHeartbeat = 30 * time.Second
	// minHeartbeat is the shortest timeout New takes. A beat can wait the
	// 5s busy timeout behind another process's write, so on a file with
	// writers in other processes the timeout must stay well above 5s.
	minHeartbeat = 5 * time.Second

	processingIndexSuffix = "__sqliteq_processing"

	// The time is read by SQLite once the statement holds the write lock,
	// so a wait for another process does not shorten the heartbeat
	beatQuery = `INSERT INTO __sqliteq_owners (id, expires_at) VALUES (?, CAST(unixepoch('subsec') * 1000 AS INTEGER) + ?)
		ON CONFLICT (id) DO UPDATE SET expires_at = excluded.expires_at`
)

func newOwnerID() string {
	var b [8]byte
	rand.Read(b[:]) // never fails
	return hex.EncodeToString(b[:])
}

// sweepEvery is the sweep interval; gapLimit the longest gap between two
// sweeps of the file that counts as normal.
// ponytail: the limit comes from this process's own timeout. A process with
// a short timeout that opens a file swept at a longer interval moves the
// heartbeats on once, which delays a reap by that gap. Upgrade path: keep
// the sweepers' interval in the clock row.
func (f *file) sweepEvery() time.Duration { return f.hb / 6 }
func (f *file) gapLimit() int64           { return (f.hb / 3).Milliseconds() }

// initOwners creates the file's owner and clock tables. The caller holds
// the write mutex.
func (f *file) initOwners() error {
	for _, s := range []string{
		`CREATE TABLE IF NOT EXISTS __sqliteq_owners (id TEXT PRIMARY KEY, expires_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS __sqliteq_clock (id INTEGER PRIMARY KEY CHECK (id = 1), swept_at INTEGER NOT NULL)`,
	} {
		if _, err := f.db.Exec(s); err != nil {
			return err
		}
	}
	return nil
}

// addOwner adds the owner column a claim fills, when it is missing, the
// index the sweep reads, and the file's owner tables. Pending rows and rows
// of older versions are NULL.
func (q *Queue) addOwner() error {
	exists, err := hasColumn(q.f.db, q.name, "owner")
	if err != nil {
		return err
	}
	if !exists {
		if _, err := q.f.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN owner TEXT", q.table)); err != nil {
			return err
		}
	}
	// Without it a sweep scans the whole table, pending backlog included.
	// Keyed by id: the smallest entry for a claim and an ack to maintain.
	if _, err := q.f.db.Exec(fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (id) WHERE status = 'processing'",
		quoteIdent(q.name+processingIndexSuffix), q.table)); err != nil {
		return err
	}
	return q.f.initOwners()
}

// beat writes this process's owner row before a claim when it has none, or
// when its last write is older than two sweep intervals: a process that
// froze must not hand out rows under an owner that already looks dead. The
// caller holds the write mutex.
func (f *file) beat() error {
	if f.closing {
		return errClosed
	}
	if f.claimed && time.Now().UnixMilli()-f.lastBeat <= 2*f.sweepEvery().Milliseconds() {
		return nil
	}
	if _, err := f.db.Exec(beatQuery, f.owner, f.hb.Milliseconds()); err != nil {
		return err
	}
	f.claimed, f.lastBeat = true, time.Now().UnixMilli()
	return nil
}

// sweep is one write transaction: it moves the clock, renews this process's
// owner row if it has one, and puts back to pending the unacknowledged
// processing rows of qs whose owner has no live row; with legacy also those
// without an owner, which an older version claimed. It returns the queues
// whose rows moved.
//
// Gap guard: a gap since the last sweep by any process longer than gapLimit
// means no process could beat (a suspended host, a stopped process), and a
// gap below -gapLimit that the clock was set back. Every heartbeat moves on
// by the gap first, so a stall takes no owner for dead at once. With no
// clock row yet, no owner is taken for dead.
func (f *file) sweep(qs []*Queue, legacy bool) (moved []*Queue, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closing {
		return nil, errClosed
	}

	tx, err := f.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Read after Begin, which can wait for another process's write
	now := time.Now().UnixMilli()
	var swept int64
	reap := true
	switch err := tx.QueryRow("SELECT swept_at FROM __sqliteq_clock").Scan(&swept); {
	case errors.Is(err, sql.ErrNoRows):
		reap = false
	case err != nil:
		return nil, err
	default:
		if gap := now - swept; gap > f.gapLimit() || gap < -f.gapLimit() {
			if _, err := tx.Exec("UPDATE __sqliteq_owners SET expires_at = expires_at + ?", gap); err != nil {
				return nil, err
			}
		}
	}
	if _, err := tx.Exec(`INSERT INTO __sqliteq_clock (id, swept_at) VALUES (1, ?)
		ON CONFLICT (id) DO UPDATE SET swept_at = excluded.swept_at`, now); err != nil {
		return nil, err
	}
	if f.claimed {
		if _, err := tx.Exec(beatQuery, f.owner, f.hb.Milliseconds()); err != nil {
			return nil, err
		}
	}

	// NULL NOT IN an empty set is true, so the ticker sweep names NOT NULL
	const dead = "owner NOT IN (SELECT id FROM __sqliteq_owners WHERE expires_at > ?)"
	var where string
	var args []any
	switch {
	case reap && legacy:
		where, args = "(owner IS NULL OR "+dead+")", []any{now}
	case reap:
		where, args = "owner IS NOT NULL AND "+dead, []any{now}
	case legacy:
		where = "owner IS NULL"
	}
	if where != "" {
		at := time.UnixMilli(now).UTC()
		for _, q := range qs {
			res, err := tx.Exec(fmt.Sprintf("UPDATE %s SET status = 'pending', owner = NULL, updated_at = ? WHERE status = 'processing' AND ack = 0 AND %s",
				q.table, where), append([]any{at}, args...)...)
			if err != nil {
				return nil, err
			}
			if n, _ := res.RowsAffected(); n > 0 {
				moved = append(moved, q)
			}
		}
	}
	if reap {
		if _, err := tx.Exec("DELETE FROM __sqliteq_owners WHERE expires_at <= ?", now); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if f.claimed {
		f.lastBeat = now
	}
	return moved, nil
}

// sweepLoop sweeps every table the process has open on the file, every
// sweepEvery until the last release, and wakes the queues whose rows moved.
// It runs in every process with a queue open, also one that never claims:
// VarMQ pops only when Len is above 0, so a consumer that waits on an empty
// queue would never start it, and a dead owner's rows would stay.
func (f *file) sweepLoop() {
	defer close(f.done)
	t := time.NewTicker(f.sweepEvery())
	defer t.Stop()
	for {
		select {
		case <-f.stop:
			return
		case <-t.C:
		}

		registry.mu.Lock()
		qs := make([]*Queue, 0, len(f.handles))
		for _, h := range f.handles {
			qs = append(qs, h.queue())
		}
		registry.mu.Unlock()

		// A failed sweep (a busy file) is tried again at the next tick
		moved, _ := f.sweep(qs, false)
		for _, q := range moved {
			q.wake()
		}
	}
}

// handBack puts this owner's unacknowledged rows in tables back to pending
// and drops its owner row, on the last release: a clean exit returns its
// jobs at once. If it fails, they return one timeout later.
func (f *file) handBack(tables []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.claimed {
		return
	}
	tx, err := f.db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	for _, t := range tables {
		if _, err := tx.Exec(fmt.Sprintf("UPDATE %s SET status = 'pending', owner = NULL, updated_at = ? WHERE status = 'processing' AND ack = 0 AND owner = ?", t),
			now, f.owner); err != nil {
			return
		}
	}
	if _, err := tx.Exec("DELETE FROM __sqliteq_owners WHERE id = ?", f.owner); err != nil {
		return
	}
	tx.Commit()
}
