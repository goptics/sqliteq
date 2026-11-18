package sqliteq

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goptics/varmq/contract"
)

// Job outcome (varmq spec/redis-storage.md 1.6.0, 6.13), on SQLite. Each
// finish of a job with a Job ID writes one row to <name>__sqliteq_jobs in
// the finish's transaction, under a new AUTOINCREMENT seq. SQLite has no
// pub/sub, so a watcher is answered by polling that table by seq (records
// newer than the last read): one goroutine per queue object, alive only
// while it has watches or OnCancel hooks. It also sweeps retention and
// expired marks.

var (
	_ contract.OutcomeWatcher        = (*Queue)(nil)
	_ contract.OutcomeWatcher        = (*PriorityQueue)(nil)
	_ contract.ResultAcknowledgeable = (*Queue)(nil)
	_ contract.ResultAcknowledgeable = (*PriorityQueue)(nil)
)

const (
	// pollEvery is how often a queue object with watches or hooks reads
	// the outcome and cancel tables.
	pollEvery = 50 * time.Millisecond
	// pollBatch caps the rows one cursor read returns; a poll reads again
	// until it gets fewer.
	pollBatch = 1000
	// sweepEvery is how often a polling queue object trims retention and
	// drops expired marks.
	sweepEvery = time.Minute

	// Retention (spec 5 defaults): cancelled records count as failed.
	keepCompleted    = 1000
	keepCompletedAge = 24 * time.Hour
	keepFailed       = 10000
	keepFailedAge    = 7 * 24 * time.Hour
	// trimEvery is how many records a queue object writes between trims.
	trimEvery = 100
	// trimFloor: the count limit never drops a record younger than this,
	// so a burst can't trim one before a poll reads it.
	trimFloor = time.Minute

	jobsSuffix      = "__sqliteq_jobs"
	cancelledSuffix = "__sqliteq_cancelled"
	readdSuffix     = "__sqliteq_readd"
)

// initSidecars creates the outcome and cancel tables, and the trigger that
// clears both for an id EnqueueOnce stores (spec 6.13 and 6.14 Re-add): an
// ON CONFLICT DO NOTHING insert of a repeat id fires no trigger. The caller
// holds the write mutex.
func (q *Queue) initSidecars() error {
	for _, s := range []string{
		// AUTOINCREMENT: seq never reuses a deleted row's, so a seq > last
		// cursor sees every new record and mark; REPLACE gives a new seq
		`CREATE TABLE IF NOT EXISTS %[2]s (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id TEXT NOT NULL UNIQUE,
			state TEXT NOT NULL,
			error TEXT,
			result BLOB,
			finished_at INTEGER NOT NULL
		)`,
		// Retention index: per state group, by end time. Replaces the
		// finished_at index of earlier builds (dropped below)
		`CREATE INDEX IF NOT EXISTS %[7]s ON %[2]s ((state <> 'completed'), finished_at)`,
		`DROP INDEX IF EXISTS %[4]s`,
		`CREATE TABLE IF NOT EXISTS %[3]s (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id TEXT NOT NULL UNIQUE,
			expires_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS %[6]s ON %[3]s (expires_at)`,
		`CREATE TRIGGER IF NOT EXISTS %[5]s AFTER INSERT ON %[1]s WHEN NEW.job_id IS NOT NULL BEGIN
			DELETE FROM %[2]s WHERE job_id = NEW.job_id;
			DELETE FROM %[3]s WHERE job_id = NEW.job_id;
		END`,
	} {
		query := fmt.Sprintf(s, q.table, q.jobs, q.cancelled,
			quoteIdent(q.name+jobsSuffix+"_finished"), quoteIdent(q.name+readdSuffix),
			quoteIdent(q.name+cancelledSuffix+"_expires"), quoteIdent(q.name+jobsSuffix+"_group"))
		if _, err := q.f.db.Exec(query); err != nil {
			return err
		}
	}
	return nil
}

var noIDPrefix = []byte(`{"id":""`)

// mayBeIDKey reports whether b has a string that encoding/json could
// match to field "id": one starting i, I, an escape, or UTF-8 lead byte
// 0xC4 (dotless i and dotted I fold to i). Conservative: false is exact.
func mayBeIDKey(b []byte) bool {
	for {
		i := bytes.IndexByte(b, '"')
		if i < 0 || i+1 >= len(b) {
			return false
		}
		switch b[i+1] {
		case 'i', 'I', '\\', 0xC4:
			return true
		}
		b = b[i+1:]
	}
}

// jobIDOf is a row's Job ID: its EnqueueOnce id, else the envelope "id" of
// a JSON object item (varmq's generated ids come with plain Enqueue). ""
// when it has neither: such a job has no outcome and is never cancelled.
func jobIDOf(onceID sql.NullString, data []byte) string {
	if onceID.Valid {
		return onceID.String
	}
	if len(data) == 0 || data[0] != '{' {
		return ""
	}
	// varmq's envelope of a job with no ID: "id" first and empty. A parse
	// gives "" too (or fails: "") unless a later key folds to "id"
	if bytes.HasPrefix(data, noIDPrefix) && !mayBeIDKey(data[len(noIDPrefix):]) {
		return ""
	}
	var env struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(data, &env) != nil {
		return ""
	}
	return env.ID
}

// record writes job id's outcome and drops its cancel mark, in tx; every
// trimEvery-th record also trims both states.
func (q *Queue) record(tx *sql.Tx, id string, state contract.JobState, reason string, result []byte, now time.Time) error {
	var errText any
	if state != contract.JobCompleted {
		errText = reason
	}
	if _, err := tx.Stmt(q.putJob).Exec(id, string(state), errText, result, now.UnixMilli()); err != nil {
		return err
	}
	if _, err := tx.Stmt(q.unmark).Exec(id); err != nil {
		return err
	}
	// The first record of a queue object trims too: a short-lived process
	// may write fewer than trimEvery
	if q.records.Add(1)%trimEvery == 1 {
		return q.trim(tx, now)
	}
	return nil
}

// trim applies retention: per state group, records past the age limit and
// those past the newest N that are older than trimFloor. Each group is a
// range of the group index: a group whose oldest record is younger than
// trimFloor is skipped; the count limit finds the N-th newest record by
// (finished_at, seq) and deletes what is older, so cost is O(log n + N),
// never a walk of the whole table.
func (q *Queue) trim(tx *sql.Tx, now time.Time) error {
	floor := now.Add(-trimFloor).UnixMilli()
	for _, g := range []struct {
		grp  int // (state <> 'completed'): cancelled records count as failed
		keep int
		age  time.Duration
	}{
		{0, keepCompleted, keepCompletedAge},
		{1, keepFailed, keepFailedAge},
	} {
		// The group expression is written as the index's, or SQLite
		// does not use it
		where := fmt.Sprintf("(state <> 'completed') = %d", g.grp)
		var oldest sql.NullInt64
		if err := tx.QueryRow(fmt.Sprintf("SELECT MIN(finished_at) FROM %s WHERE %s", q.jobs, where)).Scan(&oldest); err != nil {
			return err
		}
		if !oldest.Valid || oldest.Int64 > floor {
			continue // ages >= trimFloor: nothing can go
		}
		if _, err := tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE %s AND finished_at <= ?", q.jobs, where),
			now.Add(-g.age).UnixMilli()); err != nil {
			return err
		}
		// The keep-th newest; ties on finished_at break by seq
		var at, seq int64
		switch err := tx.QueryRow(fmt.Sprintf("SELECT finished_at, seq FROM %s WHERE %s ORDER BY finished_at DESC, seq DESC LIMIT 1 OFFSET %d",
			q.jobs, where, g.keep-1)).Scan(&at, &seq); {
		case errors.Is(err, sql.ErrNoRows):
			continue // keep or fewer
		case err != nil:
			return err
		}
		if _, err := tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE %s AND finished_at <= ? AND (finished_at, seq) < (?, ?)", q.jobs, where),
			min(at, floor), at, seq); err != nil {
			return err
		}
	}
	return nil
}

// AcknowledgeResult implements contract.ResultAcknowledgeable: Acknowledge
// that keeps result, which must be valid JSON, in the job's outcome. Not
// JSON: false, and nothing changes.
func (q *Queue) AcknowledgeResult(ackID string, result []byte) bool {
	if !json.Valid(result) {
		return false
	}
	return q.finish(ackID, contract.JobCompleted, "", result)
}

type outcomeWatch struct {
	fn   func(contract.JobOutcome)
	gen  uint64      // w.gen at its start
	done atomic.Bool // called or cancelled
}

// watchers are a queue object's outcome watches and OnCancel hooks, and the
// state of its one poll goroutine, which also polls the pause. mu is never
// held across a query.
type watchers struct {
	mu      sync.Mutex
	byID    map[string]map[*outcomeWatch]struct{}
	hooks   map[*cancelHook]struct{}
	lastSeq int64  // newest cancel mark seq a poll has read
	outSeq  int64  // newest outcome record seq a poll has read
	gen     uint64 // watches started; a read reaches only watches started before it
	// pending are the ids a by-id read must cover: a WatchOutcome's (its
	// record may predate the cursor), or one a cursor read skipped for a
	// watch newer than it. The poll reads them all in one query.
	pending map[string]struct{}
	wake    chan struct{} // a pending id waits: poll reads now, not at its tick
	polling bool
	closed  bool
	stop    chan struct{} // closed by close
}

// startLocked starts the poll goroutine if none runs; w.mu is held.
func (w *watchers) startLocked(q *Queue) {
	if w.wake == nil {
		w.wake = make(chan struct{}, 1)
	}
	if !w.polling {
		w.polling = true
		go q.poll()
	}
}

// pendLocked queues id for the next by-id read and wakes the poll; w.mu is
// held, and the poll was started.
func (w *watchers) pendLocked(id string) {
	if w.pending == nil {
		w.pending = map[string]struct{}{}
	}
	w.pending[id] = struct{}{}
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// close drops every watch and hook without calling it, and stops polling.
func (w *watchers) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	w.closed = true
	close(w.stop)
	for _, set := range w.byID {
		for o := range set {
			o.done.Store(true)
		}
	}
	w.byID, w.hooks, w.pending = nil, nil, nil
}

// WatchOutcome implements contract.OutcomeWatcher: fn runs once, on its own
// goroutine, when job id completes, fails or is cancelled through any
// queue object on this database file, in this process or another. A job
// whose record is still kept is reported even if it ended before the call
// (the poll's next by-id read, woken at once). New records are polled every
// pollEvery. An empty id, or a closed queue, watches nothing. Close drops
// watches without calling fn.
func (q *Queue) WatchOutcome(id string, fn func(contract.JobOutcome)) (cancel func()) {
	o := &outcomeWatch{fn: fn}
	w := &q.w
	w.mu.Lock()
	if id == "" || w.closed {
		w.mu.Unlock()
		return func() {}
	}
	if len(w.byID) == 0 {
		// No poll moved the cursor while nothing was watched: start it at
		// the newest record. Older ones are this watch's by-id read.
		w.mu.Unlock()
		var last sql.NullInt64
		err := q.f.db.QueryRow(fmt.Sprintf("SELECT MAX(seq) FROM %s", q.jobs)).Scan(&last)
		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return func() {}
		}
		if err == nil && len(w.byID) == 0 {
			w.outSeq = last.Int64
		}
	}
	if w.byID == nil {
		w.byID = map[string]map[*outcomeWatch]struct{}{}
	}
	if w.byID[id] == nil {
		w.byID[id] = map[*outcomeWatch]struct{}{}
	}
	w.byID[id][o] = struct{}{}
	w.startLocked(q)
	w.gen++
	o.gen = w.gen
	w.pendLocked(id)
	w.mu.Unlock()

	return func() {
		if o.done.CompareAndSwap(false, true) {
			w.mu.Lock()
			defer w.mu.Unlock()
			if set := w.byID[id]; set != nil {
				delete(set, o)
				if len(set) == 0 {
					delete(w.byID, id)
				}
			}
		}
	}
}

// poll reads new outcome records, cancel marks and, while the cache says
// paused, the pause row every pollEvery, and sweeps every sweepEvery,
// until Close or until nothing is watched and the cache says running. A
// wake does the pending by-id read alone.
func (q *Queue) poll() {
	t := time.NewTicker(pollEvery)
	defer t.Stop()
	w := &q.w
	swept := time.Now()
	for {
		tick := true
		select {
		case <-w.stop:
			return
		case <-t.C:
		case <-w.wake:
			tick = false
		}

		w.mu.Lock()
		paused := q.pz.paused.Load()
		if len(w.byID) == 0 && len(w.hooks) == 0 && !paused {
			w.polling = false
			w.mu.Unlock()
			return
		}
		watching, hooks := len(w.byID) > 0, len(w.hooks) > 0
		outSeq, seq, gen := w.outSeq, w.lastSeq, w.gen
		w.mu.Unlock()

		if !tick {
			q.readPending()
			continue
		}
		if watching {
			q.pollOutcomes(outSeq, gen)
			q.readPending() // ids the cursor skipped, at once
		}
		if hooks {
			q.pollCancels(seq)
		}
		if paused {
			q.pollPause()
		}
		if time.Since(swept) >= sweepEvery {
			swept = time.Now()
			q.sweep()
		}
	}
}

// pollOutcomes reads the records newer than seq, in pollBatch reads, and
// delivers the watched ones to watches up to gen; a newer watch of one is
// left to readPending (see deliver).
func (q *Queue) pollOutcomes(seq int64, gen uint64) {
	for {
		rows, err := q.f.db.Query(fmt.Sprintf("SELECT seq, job_id, state, error, result FROM %s WHERE seq > ? ORDER BY seq LIMIT %d", q.jobs, pollBatch), seq)
		if err != nil {
			return
		}
		outs, n := q.scanOutcomes(rows, &seq)
		if rows.Err() != nil || n == 0 {
			return // read again from the old seq
		}

		w := &q.w
		w.mu.Lock()
		w.outSeq = max(w.outSeq, seq)
		watched := outs[:0]
		for _, out := range outs {
			if w.byID[out.ID] != nil {
				watched = append(watched, out)
			}
		}
		w.mu.Unlock()
		for _, out := range watched {
			q.deliver(out, gen)
		}
		if n < pollBatch {
			return
		}
	}
}

// scanOutcomes reads rows of (seq?, job_id, state, error, result) and
// closes them; seq, if not nil, gets the last seq. n counts rows read.
func (q *Queue) scanOutcomes(rows *sql.Rows, seq *int64) (outs []contract.JobOutcome, n int) {
	defer rows.Close()
	for rows.Next() {
		var (
			id, state string
			errText   sql.NullString
			result    []byte
			err       error
		)
		if seq != nil {
			err = rows.Scan(seq, &id, &state, &errText, &result)
		} else {
			err = rows.Scan(&id, &state, &errText, &result)
		}
		n++
		if err != nil {
			continue
		}
		outs = append(outs, contract.JobOutcome{ID: id, State: contract.JobState(state), Error: errText.String, Result: result})
	}
	return outs, n
}

// sweep applies retention and drops expired cancel marks (spec 5, 6.14
// Prune), so an idle queue ages out too.
func (q *Queue) sweep() {
	q.f.mu.Lock()
	defer q.f.mu.Unlock()
	tx, err := q.f.db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	now := time.Now()
	if q.trim(tx, now) != nil {
		return
	}
	if _, err := tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE expires_at <= ?", q.cancelled), now.UnixMilli()); err != nil {
		return
	}
	tx.Commit()
}

// readPending reads the records of the pending ids still watched, in
// pollBatch chunks, and delivers them to the watches started before the
// read. A failed read keeps its ids pending for the next poll. An id with
// no record is dropped: its record, when written, is newer than the cursor.
func (q *Queue) readPending() {
	w := &q.w
	w.mu.Lock()
	ids := make([]string, 0, len(w.pending))
	for id := range w.pending {
		if w.byID[id] != nil {
			ids = append(ids, id)
		}
	}
	clear(w.pending)
	gen := w.gen
	w.mu.Unlock()

	for len(ids) > 0 {
		chunk := ids[:min(len(ids), pollBatch)]
		ids = ids[len(chunk):]
		args := make([]any, len(chunk))
		for i, id := range chunk {
			args[i] = id
		}
		rows, err := q.f.db.Query(fmt.Sprintf("SELECT job_id, state, error, result FROM %s WHERE job_id IN (%s)",
			q.jobs, strings.Repeat("?, ", len(chunk)-1)+"?"), args...)
		var outs []contract.JobOutcome
		if err == nil {
			outs, _ = q.scanOutcomes(rows, nil)
			err = rows.Err()
		}
		if err != nil { // retried at the next tick, not woken: no hot loop
			w.mu.Lock()
			if w.pending == nil {
				w.pending = map[string]struct{}{}
			}
			for _, id := range append(chunk, ids...) {
				w.pending[id] = struct{}{}
			}
			w.mu.Unlock()
			return
		}
		for _, out := range outs {
			q.deliver(out, gen)
		}
	}
}

// deliver ends the watches of out.ID started up to gen: each fn runs once,
// on its own goroutine, so a slow fn holds up no poll. A watch started
// after the read stays: the record read may be an earlier run's, which a
// re-add since deleted (spec 6.13 Re-add). Its id goes pending, so a by-id
// read made after it covers it even once the cursor is past the record.
func (q *Queue) deliver(out contract.JobOutcome, gen uint64) {
	w := &q.w
	w.mu.Lock()
	set := w.byID[out.ID]
	var ended []*outcomeWatch
	for o := range set {
		if o.gen <= gen {
			ended = append(ended, o)
			delete(set, o)
		}
	}
	if len(set) == 0 {
		delete(w.byID, out.ID)
	} else if !w.closed {
		w.pendLocked(out.ID)
	}
	w.mu.Unlock()

	for _, o := range ended {
		go func() {
			if o.done.CompareAndSwap(false, true) {
				o.fn(out)
			}
		}()
	}
}
