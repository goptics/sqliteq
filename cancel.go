package sqliteq

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/goptics/varmq/contract"
)

// Job cancel (varmq spec/redis-storage.md 1.7.0, 6.14), on SQLite.
// CancelJob writes a mark to <name>__sqliteq_cancelled (expires_at: ms).
// Every pop drops a marked waiting job with outcome cancelled; OnCancel
// learns of marks by polling seq; AcknowledgeCancelled finishes a running
// job that stopped for it. Any finish, and an EnqueueOnce that stores the
// id, clears the mark. A valid workflow item (workflowTag) whose job ID is its tag's is never dropped: the
// workflow decides its end, and its mark stays.

var (
	_ contract.JobCanceller          = (*Queue)(nil)
	_ contract.JobCanceller          = (*PriorityQueue)(nil)
	_ contract.CancelNotifier        = (*Queue)(nil)
	_ contract.CancelNotifier        = (*PriorityQueue)(nil)
	_ contract.CancelAcknowledgeable = (*Queue)(nil)
	_ contract.CancelAcknowledgeable = (*PriorityQueue)(nil)
)

const (
	// cancelTTL is how long one mark lasts.
	cancelTTL = 24 * time.Hour
	// maxDrops caps the marked items one pop drops; at the cap it returns
	// what it has and the next pop goes on.
	maxDrops = 1000
)

var (
	errEmptyJobID = errors.New("sqliteq: empty job id")
	errClosed     = errors.New("sqliteq: queue is closed")
)

// CancelJob implements contract.JobCanceller: it marks job id cancelled for
// cancelTTL. A waiting job is dropped by the pop that would hand it out
// (outcome cancelled); OnCancel callbacks of every queue object on the file
// learn of a running one, which ends only if its worker stops it.
// contract.ErrJobEnded when its outcome record shows it ended. An unknown
// id is marked too: a later Enqueue of it is dropped, unless EnqueueOnce
// stores it (that clears the mark).
func (q *Queue) CancelJob(id string) error {
	if id == "" {
		return errEmptyJobID
	}
	if q.closed.Load() {
		return errClosed
	}

	now := time.Now().UnixMilli()
	q.f.mu.Lock()
	defer q.f.mu.Unlock()
	tx, err := q.f.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var state string
	switch err := tx.QueryRow(fmt.Sprintf("SELECT state FROM %s WHERE job_id = ?", q.jobs), id).Scan(&state); {
	case err == nil:
		return contract.ErrJobEnded // every record is an end
	case !errors.Is(err, sql.ErrNoRows):
		return err
	}
	if _, err := tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE expires_at <= ?", q.cancelled), now); err != nil {
		return err
	}
	// REPLACE deletes an older mark of id, so the new one takes a new seq
	// and reaches OnCancel again
	if _, err := tx.Exec(fmt.Sprintf("INSERT OR REPLACE INTO %s (job_id, expires_at) VALUES (?, ?)", q.cancelled),
		id, now+cancelTTL.Milliseconds()); err != nil {
		return err
	}
	return tx.Commit()
}

// AcknowledgeCancelled implements contract.CancelAcknowledgeable: Reject
// with outcome cancelled and reason as its error. A kept row
// (WithRemoveOnComplete(false)) gets status 'cancelled'.
func (q *Queue) AcknowledgeCancelled(ackID, reason string) bool {
	return q.finish(ackID, contract.JobCancelled, reason, nil)
}

// OnCancel implements contract.CancelNotifier: fn gets the job ID of each
// cancel request made after the call, through any queue object on this
// database file, until stop. Marks are polled every pollEvery; each call
// runs on its own goroutine. A mark gone by the poll (the job finished or
// was re-added) is never reported. A closed queue, or a failed first read,
// reports nothing.
func (q *Queue) OnCancel(fn func(jobID string)) (stop func()) {
	// Read before the lock: a mark made after this read has a higher seq
	var last sql.NullInt64
	if err := q.f.db.QueryRow(fmt.Sprintf("SELECT MAX(seq) FROM %s", q.cancelled)).Scan(&last); err != nil {
		return func() {} // 0 would replay every live mark
	}

	h := &cancelHook{fn: fn, from: last.Int64}
	w := &q.w
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return func() {}
	}
	if len(w.hooks) == 0 {
		w.lastSeq = h.from // no poll read marks for anyone since
	}
	if w.hooks == nil {
		w.hooks = map[*cancelHook]struct{}{}
	}
	w.hooks[h] = struct{}{}
	w.startLocked(q)

	return func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		delete(w.hooks, h)
	}
}

// cancelHook is one OnCancel callback; it gets marks with seq above from.
type cancelHook struct {
	fn   func(string)
	from int64
}

// pollCancels reads the live marks newer than seq and passes each to the
// hooks live after the read whose from is below it, so a hook added during
// the read still gets its marks, and a stopped one gets none.
func (q *Queue) pollCancels(seq int64) {
	rows, err := q.f.db.Query(fmt.Sprintf("SELECT seq, job_id FROM %s WHERE seq > ? AND expires_at > ? ORDER BY seq LIMIT %d",
		q.cancelled, maxDrops), seq, time.Now().UnixMilli())
	if err != nil {
		return
	}
	type mark struct {
		seq int64
		id  string
	}
	var marks []mark
	for rows.Next() {
		var m mark
		if rows.Scan(&m.seq, &m.id) == nil {
			marks = append(marks, m)
		}
	}
	if rows.Close() != nil || rows.Err() != nil || len(marks) == 0 {
		return // read again from the old seq
	}

	w := &q.w
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	w.lastSeq = max(w.lastSeq, marks[len(marks)-1].seq)
	for h := range w.hooks {
		for _, m := range marks {
			if m.seq > h.from {
				go h.fn(m.id)
			}
		}
	}
}

// anyMark reports whether a live mark exists, so a pop without marks pays
// one read; an error counts as a mark (the gated pop then reports it).
func (q *Queue) anyMark(now time.Time) bool {
	var n int
	if err := q.hasMark.QueryRow(now.UnixMilli()).Scan(&n); err != nil {
		return true
	}
	return n == 1
}

// marked reports whether job id has a live mark; isMarked is q.isMarked
// in the caller's tx.
func marked(isMarked *sql.Stmt, id string, now time.Time) (bool, error) {
	if id == "" {
		return false, nil
	}
	var n int
	err := isMarked.QueryRow(id, now.UnixMilli()).Scan(&n)
	return n == 1, err
}

// markedDrop is marked for a pop: a valid workflow item is never dropped.
func markedDrop(isMarked *sql.Stmt, id string, data []byte, now time.Time) (bool, error) {
	drop, err := marked(isMarked, id, now)
	if err != nil || !drop {
		return false, err
	}
	tag, ok := workflowTag(data)
	return !(ok && id == tag.jobID()), nil
}
