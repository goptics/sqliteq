package sqliteq

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"time"
)

// Owners and heartbeats (redisq's model, on SQLite). Each file in a process
// has a random owner id, stored on every row it claims. An owner that
// claims keeps a row in __sqliteq_owners, renewed by the sweep; a process
// that never claims writes none. The sweep runs on a table's first open and
// then every hb/6: it drops the owner rows that ran out and requeues the
// processing rows whose owner has no live row. SQLite has no presence
// signal, so the heartbeat is the only liveness: a killed owner's rows come
// back one timeout after its last beat.
//
// Queue tables have no index on processing rows: a claim and an ack would
// pay for it on every call. So a sweep reads a queue table only on its
// first open, as recovery always did, and when an owner died: the sweep
// that drops a dead owner's row moves the count in __sqliteq_clock, and
// every process that sees the count move reads its open tables once and
// wakes its queues. Each process also reads them once per timeout, for a
// row whose owner row went without the count moving.
//
// Times are the host's wall clock in Unix ms. __sqliteq_clock also holds
// when any process last swept (see sweepLocked for the gap guard).

const (
	defaultHeartbeat = 30 * time.Second
	// minHeartbeat is the shortest timeout New takes. A beat can wait the
	// 5s busy timeout behind another process's write, so on a file with
	// writers in other processes the timeout must stay well above 5s.
	minHeartbeat = 5 * time.Second

	// A beat clears shifted: the owner is alive, so the next stall may
	// move its heartbeat on again
	beatQuery = `INSERT INTO __sqliteq_owners (id, expires_at) VALUES (?, CAST(unixepoch('subsec') * 1000 AS INTEGER) + ?)
		ON CONFLICT (id) DO UPDATE SET expires_at = excluded.expires_at, shifted = 0`
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
		// shifted: the gap guard moved the heartbeat on since the owner's last beat
		`CREATE TABLE IF NOT EXISTS __sqliteq_owners (id TEXT PRIMARY KEY NOT NULL, expires_at INTEGER NOT NULL, shifted INTEGER NOT NULL DEFAULT 0)`,
		// moved: counts the dead owners dropped and the requeues, by any process
		`CREATE TABLE IF NOT EXISTS __sqliteq_clock (id INTEGER PRIMARY KEY CHECK (id = 1), swept_at INTEGER NOT NULL, moved INTEGER NOT NULL DEFAULT 0)`,
	} {
		if _, err := f.db.Exec(s); err != nil {
			return err
		}
	}
	return nil
}

// addOwner adds the owner column a claim fills, when it is missing, and the
// file's owner tables. Pending rows and rows of older versions are NULL.
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
	return q.f.initOwners()
}

// beat makes this process's owner row live before a claim when it has
// none, or when its last write is older than two sweep intervals: a process
// that froze must not hand out rows under an owner that already looks dead.
// It is a sweep of no table, so the gap guard runs before the row is
// written and a later sweep does not move a fresh heartbeat. The caller
// holds the write mutex.
func (f *file) beat() error {
	if f.closing {
		return errClosed
	}
	if f.claimed && time.Now().UnixMilli()-f.lastBeat <= 2*f.sweepEvery().Milliseconds() {
		return nil
	}
	first := !f.claimed
	f.claimed = true
	if _, err := f.sweepLocked(nil, false, false, false); err != nil {
		f.claimed = !first
		return err
	}
	return nil
}

// sweep runs sweepLocked under the write mutex; a nil qs is every queue the
// process has open, read under the mutex so no open falls between. full
// reads the tables whatever the count says. wake reports that some process
// dropped an owner or requeued rows in the file since this one last looked:
// its queues may have pending rows no local add announced.
func (f *file) sweep(qs []*Queue, legacy, full bool) (wake bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closing {
		return false, errClosed
	}
	if qs == nil {
		qs = *f.swept.Load()
	}
	moved, err := f.sweepLocked(qs, legacy || full, legacy, f.seen)
	if moved < 0 {
		return false, err // nothing committed
	}
	// A first open has no one to wake for its own table, and must not use
	// up a count the loop has not seen: the next tick reads the other
	// tables and wakes for it
	if legacy && f.seen {
		return false, err
	}
	wake = f.seen && moved != f.seenMoved
	if err == nil { // a table that failed is read again at the next tick
		f.seen, f.seenMoved = true, moved
	}
	return wake, err
}

// sweepLocked is one write transaction: it moves the clock, renews this
// process's owner row if it has one and drops the owner rows that ran out.
// It reads the tables of qs, a scan each, only when it has to: when scan is
// set (a first open, the loop's read once per timeout), when it dropped an
// owner row, or when track is set and the file's count moved since
// f.seenMoved (another process dropped one). The read puts back to pending the unacknowledged processing rows whose owner
// has no live row; with legacy also those without an owner, which an older
// version claimed. It returns the file's count, -1 when nothing was
// committed. A table whose requeue fails does not stop the rest; its error
// is returned.
//
// Gap guard: a gap since the last sweep by any process longer than gapLimit
// means no process could beat (a suspended host, a stopped process), and a
// gap below -gapLimit that the clock was set back. Every heartbeat moves on
// by the gap first, so a stall takes no owner for dead at once. A heartbeat
// moves once between two beats of its owner: a dead owner's time runs out
// even when every process that opens the file is gone before its second
// sweep. With no clock row yet, no owner is taken for dead.
func (f *file) sweepLocked(qs []*Queue, scan, legacy, track bool) (moved int64, err error) {
	tx, err := f.db.Begin()
	if err != nil {
		return -1, err
	}
	defer tx.Rollback()

	// Read after Begin, which can wait for another process's write
	now := time.Now().UnixMilli()
	var swept int64
	reap := true
	// CAST: a value of another type must not fail every sweep for good
	switch err := tx.QueryRow("SELECT CAST(swept_at AS INTEGER), CAST(moved AS INTEGER) FROM __sqliteq_clock").Scan(&swept, &moved); {
	case errors.Is(err, sql.ErrNoRows):
		reap = false
	case err != nil:
		return -1, err
	default:
		if gap := now - swept; gap > f.gapLimit() || gap < -f.gapLimit() {
			if _, err := tx.Exec("UPDATE __sqliteq_owners SET expires_at = expires_at + ?, shifted = 1 WHERE shifted = 0", gap); err != nil {
				return -1, err
			}
		}
	}
	if f.claimed {
		if _, err := tx.Exec(beatQuery, f.owner, f.hb.Milliseconds()); err != nil {
			return -1, err
		}
	}

	scan = scan || track && moved != f.seenMoved
	if reap {
		res, err := tx.Exec("DELETE FROM __sqliteq_owners WHERE expires_at <= ?", now)
		if err != nil {
			return -1, err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			scan = true
			moved++ // the other processes read their tables too
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
	var errs []error
	if where != "" && scan {
		at, some := time.UnixMilli(now).UTC(), false
		for _, q := range qs {
			res, err := tx.Exec(fmt.Sprintf("UPDATE %s SET status = 'pending', owner = NULL, updated_at = ? WHERE status = 'processing' AND ack = 0 AND %s",
				q.table, where), append([]any{at}, args...)...)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", q.name, err))
				continue
			}
			if n, _ := res.RowsAffected(); n > 0 {
				some = true
			}
		}
		if some {
			moved++
		}
	}
	if _, err := tx.Exec(`INSERT INTO __sqliteq_clock (id, swept_at, moved) VALUES (1, ?, ?)
		ON CONFLICT (id) DO UPDATE SET swept_at = excluded.swept_at, moved = excluded.moved`, now, moved); err != nil {
		return -1, err
	}
	if err := tx.Commit(); err != nil {
		return -1, err
	}
	if f.claimed {
		f.lastBeat = now
	}
	return moved, errors.Join(errs...)
}

// sweepLoop sweeps the file every sweepEvery until the last release, for
// every table the process has open on it, and wakes its queues when any
// process dropped a dead owner or requeued rows. It runs in every process with a queue open, also one that
// never claims: VarMQ pops only when Len is above 0, so a consumer that
// waits on an empty queue would never start it, and a dead owner's rows
// would stay. It takes no registry lock: an open of another file that waits
// for SQLite must not hold up this file's heartbeat.
func (f *file) sweepLoop() {
	defer close(f.done)
	t := time.NewTicker(f.sweepEvery())
	defer t.Stop()
	for n := 1; ; n++ {
		select {
		case <-f.stop:
			return
		case <-t.C:
		}

		// Once per timeout the tables are read whatever the count says: a
		// row can carry an owner whose row went without the count moving
		// (claimed after a stall, just as the row was dropped).
		// ponytail: one scan of each open table per timeout. Upgrade path:
		// the claim checks its owner row in the same statement.
		// A failed sweep (a busy file) is tried again at the next tick
		wake, err := f.sweep(nil, false, n%6 == 0)
		if err != nil && !errors.Is(err, errClosed) {
			log.Printf("sqliteq: sweep of %s: %v", f.key, err)
		}
		if wake {
			for _, q := range *f.swept.Load() {
				q.wake()
			}
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
	now, some := time.Now().UTC(), false
	for _, t := range tables {
		res, err := tx.Exec(fmt.Sprintf("UPDATE %s SET status = 'pending', owner = NULL, updated_at = ? WHERE status = 'processing' AND ack = 0 AND owner = ?", t),
			now, f.owner)
		if err != nil {
			return
		}
		if n, _ := res.RowsAffected(); n > 0 {
			some = true
		}
	}
	if some { // the other processes wake their queues
		if _, err := tx.Exec("UPDATE __sqliteq_clock SET moved = moved + 1"); err != nil {
			return
		}
	}
	if _, err := tx.Exec("DELETE FROM __sqliteq_owners WHERE id = ?", f.owner); err != nil {
		return
	}
	tx.Commit()
}
