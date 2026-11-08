package sqliteq

import (
	"database/sql"
	"encoding/json"
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
		`CREATE INDEX IF NOT EXISTS %[4]s ON %[2]s (finished_at)`,
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
			quoteIdent(q.name+cancelledSuffix+"_expires"))
		if _, err := q.f.db.Exec(query); err != nil {
			return err
		}
	}
	return nil
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
// those past the newest N that are older than trimFloor.
func (q *Queue) trim(tx *sql.Tx, now time.Time) error {
	for _, g := range []struct {
		where string
		keep  int
		age   time.Duration
	}{
		{"state = 'completed'", keepCompleted, keepCompletedAge},
		{"state <> 'completed'", keepFailed, keepFailedAge},
	} {
		if _, err := tx.Exec(fmt.Sprintf("DELETE FROM %s WHERE %s AND finished_at <= ?", q.jobs, g.where),
			now.Add(-g.age).UnixMilli()); err != nil {
			return err
		}
		if _, err := tx.Exec(fmt.Sprintf("DELETE FROM %[1]s WHERE finished_at <= ? AND job_id IN (SELECT job_id FROM %[1]s WHERE %[2]s ORDER BY finished_at DESC LIMIT -1 OFFSET %[3]d)",
			q.jobs, g.where, g.keep), now.Add(-trimFloor).UnixMilli()); err != nil {
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
	polling bool
	closed  bool
	stop    chan struct{} // closed by close
}

// startLocked starts the poll goroutine if none runs; w.mu is held.
func (w *watchers) startLocked(q *Queue) {
	if !w.polling {
		w.polling = true
		go q.poll()
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
	w.byID, w.hooks = nil, nil
}

// WatchOutcome implements contract.OutcomeWatcher: fn runs once, on its own
// goroutine, when job id completes, fails or is cancelled through any
// queue object on this database file, in this process or another. A job
// whose record is still kept is reported even if it ended before the call.
// It is polled every pollEvery. An empty id, or a closed queue, watches
// nothing. Close drops watches without calling fn.
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
		// the newest record. Older ones are this watch's catchUp below.
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
	w.gen++
	o.gen = w.gen
	if w.byID == nil {
		w.byID = map[string]map[*outcomeWatch]struct{}{}
	}
	if w.byID[id] == nil {
		w.byID[id] = map[*outcomeWatch]struct{}{}
	}
	w.byID[id][o] = struct{}{}
	w.startLocked(q)
	w.mu.Unlock()

	go q.catchUp([]string{id}, o.gen)

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
// until Close or until nothing is watched and the cache says running.
func (q *Queue) poll() {
	t := time.NewTicker(pollEvery)
	defer t.Stop()
	w := &q.w
	swept := time.Now()
	for {
		select {
		case <-w.stop:
			return
		case <-t.C:
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

		if watching {
			q.pollOutcomes(outSeq, gen)
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
// delivers the watched ones to watches up to gen.
func (q *Queue) pollOutcomes(seq int64, gen uint64) {
	for {
		rows, err := q.f.db.Query(fmt.Sprintf("SELECT seq, job_id FROM %s WHERE seq > ? ORDER BY seq LIMIT %d", q.jobs, pollBatch), seq)
		if err != nil {
			return
		}
		var ids []string
		for rows.Next() {
			var id string
			if rows.Scan(&seq, &id) == nil {
				ids = append(ids, id)
			}
		}
		if rows.Close() != nil || rows.Err() != nil || len(ids) == 0 {
			return // read again from the old seq
		}

		w := &q.w
		w.mu.Lock()
		w.outSeq = max(w.outSeq, seq)
		watched := ids[:0]
		for _, id := range ids {
			if w.byID[id] != nil {
				watched = append(watched, id)
			}
		}
		w.mu.Unlock()
		if len(watched) > 0 {
			q.catchUp(watched, gen)
		}
		if len(ids) < pollBatch {
			return
		}
	}
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

// catchUp delivers the ids whose outcome record exists to their watches
// up to gen, those started before the read. A failed read is retried by
// the next poll.
func (q *Queue) catchUp(ids []string, gen uint64) {
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := q.f.db.Query(fmt.Sprintf("SELECT job_id, state, error, result FROM %s WHERE job_id IN (%s)",
		q.jobs, strings.Repeat("?, ", len(ids)-1)+"?"), args...)
	if err != nil {
		return
	}
	var outs []contract.JobOutcome
	for rows.Next() {
		var (
			id, state string
			errText   sql.NullString
			result    []byte
		)
		if rows.Scan(&id, &state, &errText, &result) != nil {
			continue
		}
		outs = append(outs, contract.JobOutcome{ID: id, State: contract.JobState(state), Error: errText.String, Result: result})
	}
	rows.Close()

	for _, out := range outs {
		q.deliver(out, gen)
	}
}

// deliver ends the watches of out.ID started up to gen: each fn runs once,
// on its own goroutine, so a slow fn holds up no poll. A watch started
// after the read stays: the record read may be an earlier run's, which a
// re-add since deleted (spec 6.13 Re-add).
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
