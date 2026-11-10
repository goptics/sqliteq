package sqliteq

import (
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/goptics/varmq/contract"
	_ "github.com/mattn/go-sqlite3"
)

// remainingCap bounds the pending count a batch pop reports
const remainingCap = 256

// afterEmptyPop runs between an empty pop outside a tx and its read of the
// pause row; tests only.
var afterEmptyPop = func() {}

// Queue implements the Queue interface using SQLite as the storage backend
type Queue struct {
	f                *file
	name, table      string // table is quoteIdent(name)
	priority         bool
	order            string // "id" or "priority, id"
	removeOnComplete bool
	closed           atomic.Bool

	jobs, cancelled string       // quoted outcome and cancel mark tables
	meta            string       // quoted totals and pause table
	id              string       // QueueID
	w               watchers     // outcome watches, OnCancel hooks, pause poll
	pz              pauseState   // Paused cache and OnPauseChange hooks
	records         atomic.Int64 // outcome records written, for trims

	// Prepared once per handle; they close with the file's pool
	insert, claim, pop, fin, count, countCapped, values *sql.Stmt
	putJob, unmark, hasMark, isMarked, pausedAt, tally  *sql.Stmt
}

// claimed is one row a claim moved to processing
type claimed struct {
	id       int64
	priority int
	data     []byte
	once     sql.NullString // job_id
}

// newQueue creates a SQLite-based queue on its table: the table itself, the
// index migration and the prepared statements. Recovery runs from file.open,
// once per table per file.
func newQueue(f *file, name string, priority bool, opts ...Option) (*Queue, error) {
	q := &Queue{
		f:                f,
		name:             name,
		table:            quoteIdent(name),
		priority:         priority,
		order:            "id",
		removeOnComplete: true, // Default to removing completed items
		jobs:             quoteIdent(name + jobsSuffix),
		cancelled:        quoteIdent(name + cancelledSuffix),
		meta:             quoteIdent(name + metaSuffix),
		id:               queueID(f.key, name, priority),
	}
	q.w.stop = make(chan struct{})
	if priority {
		q.order = "priority, id"
	}

	// Apply any provided options
	for _, opt := range opts {
		if opt == nil {
			return nil, errNilOption
		}
		opt(q)
	}

	// The db is shared by every queue of the file, so it is not closed on errors
	if err := q.initSchema(); err != nil {
		return nil, err
	}
	if err := q.prepare(); err != nil {
		return nil, fmt.Errorf("failed to prepare statements: %w", err)
	}
	// A pause kept in the file holds this object from its first pop
	if paused, err := readPaused(q.pausedAt); err != nil {
		q.closeStmts()
		return nil, fmt.Errorf("failed to read pause: %w", err)
	} else if paused {
		q.setPaused(true, q.pz.gen.Load())
	}

	return q, nil
}

// initSchema creates or migrates the table and its index under the write mutex
func (q *Queue) initSchema() error {
	q.f.mu.Lock()
	defer q.f.mu.Unlock()

	if err := q.initTable(); err != nil {
		return fmt.Errorf("failed to initialize table: %w", err)
	}
	if err := q.migrate(); err != nil {
		return fmt.Errorf("failed to migrate indexes: %w", err)
	}
	if err := q.addJobID(); err != nil {
		return fmt.Errorf("failed to add job_id: %w", err)
	}
	if err := q.initSidecars(); err != nil {
		return fmt.Errorf("failed to create outcome tables: %w", err)
	}
	if err := q.initMeta(); err != nil {
		return fmt.Errorf("failed to create meta table: %w", err)
	}
	return nil
}

// initTable creates the table as v0.2 defines it, so old tables need no
// rebuild, and adds the priority column to a priority queue's table when
// it is missing
func (q *Queue) initTable() error {
	_, err := q.f.db.Exec(fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		data BLOB NOT NULL,
		status TEXT NOT NULL,
		ack_id TEXT UNIQUE,
		ack BOOLEAN DEFAULT 0,
		created_at TIMESTAMP,
		updated_at TIMESTAMP
	)`, q.table))
	if err != nil || !q.priority {
		return err
	}

	exists, err := hasColumn(q.f.db, q.name, "priority")
	if err != nil || exists {
		return err
	}
	// Add priority column with default value 0
	_, err = q.f.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN priority INTEGER NOT NULL DEFAULT 0", q.table))
	return err
}

// pendingIndexSuffix and jobIDIndexSuffix name a queue's partial indexes.
// Tables and indexes share one namespace, so the suffixes are ones no queue
// name would take.
const (
	pendingIndexSuffix = "__sqliteq_pending"
	jobIDIndexSuffix   = "__sqliteq_job_id"
)

// migrate drops the v0.2 indexes and makes sure the one partial index on
// pending rows matches the queue order. On a migrated table it changes nothing.
func (q *Queue) migrate() error {
	for _, suffix := range []string{"_status_idx", "_status_ack_idx", "_ack_id_idx", "_priority_idx"} {
		if _, err := q.f.db.Exec("DROP INDEX IF EXISTS " + quoteIdent(q.name+suffix)); err != nil {
			return err
		}
	}

	index := q.name + pendingIndexSuffix
	cols, err := indexColumns(q.f.db, index)
	if err != nil {
		return err
	}
	if slices.Equal(cols, strings.Split(q.order, ", ")) {
		return nil
	}

	// The table was last used as the other kind of queue
	if len(cols) > 0 {
		if _, err := q.f.db.Exec("DROP INDEX " + quoteIdent(index)); err != nil {
			return err
		}
	}
	_, err = q.f.db.Exec(fmt.Sprintf("CREATE INDEX %s ON %s (%s) WHERE status = 'pending'", quoteIdent(index), q.table, q.order))
	return err
}

// addJobID adds the job_id column EnqueueOnce fills, when it is missing, and
// its unique index over the rows that have one. Rows without an id, old ones
// and those of Enqueue, are NULL and outside the index.
func (q *Queue) addJobID() error {
	exists, err := hasColumn(q.f.db, q.name, "job_id")
	if err != nil {
		return err
	}
	if !exists {
		if _, err := q.f.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN job_id TEXT", q.table)); err != nil {
			return err
		}
	}
	_, err = q.f.db.Exec(fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (job_id) WHERE job_id IS NOT NULL",
		quoteIdent(q.name+jobIDIndexSuffix), q.table))
	return err
}

// indexColumns lists an index's columns in order; none if it does not exist
func indexColumns(db *sql.DB, index string) ([]string, error) {
	rows, err := db.Query("SELECT name FROM pragma_index_info(?) ORDER BY seqno", index)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// prepare prepares every statement of the queue once. Each read keeps
// status = 'pending' literal, or SQLite cannot use the partial index. Pops
// select nothing while the pause row exists.
func (q *Queue) prepare() (err error) {
	defer func() {
		if err != nil {
			q.closeStmts()
		}
	}()

	cols, vals, returning := "data, status, ack, created_at, updated_at, job_id", "?, 'pending', 0, ?, ?, ?", "id, data, job_id"
	if q.priority {
		cols, vals, returning = cols+", priority", vals+", ?", "id, priority, data, job_id"
	}
	pending := fmt.Sprintf("SELECT id FROM %s WHERE status = 'pending' AND NOT EXISTS (SELECT 1 FROM %s WHERE key = 'paused_at') ORDER BY %s",
		q.table, q.meta, q.order)

	// fin ends a processing row: deleted, or kept with its end as status
	fin := fmt.Sprintf("DELETE FROM %s WHERE id = ? AND status = 'processing' RETURNING job_id, data", q.table)
	if !q.removeOnComplete {
		fin = fmt.Sprintf("UPDATE %s SET status = ?, ack = 1, updated_at = ? WHERE id = ? AND status = 'processing' RETURNING job_id, data", q.table)
	}

	for _, s := range []struct {
		stmt  **sql.Stmt
		query string
	}{
		// The conflict target is the job_id index alone, so a NOT NULL or any
		// other failure still errors; it must repeat the index's WHERE
		{&q.insert, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (job_id) WHERE job_id IS NOT NULL DO NOTHING", q.table, cols, vals)},
		{&q.claim, fmt.Sprintf("UPDATE %s SET status = 'processing', updated_at = ? WHERE id IN (%s LIMIT ?) RETURNING %s", q.table, pending, returning)},
		{&q.pop, fmt.Sprintf("DELETE FROM %s WHERE id IN (%s LIMIT 1) RETURNING data, job_id", q.table, pending)},
		{&q.fin, fin},
		{&q.putJob, fmt.Sprintf("INSERT OR REPLACE INTO %s (job_id, state, error, result, finished_at) VALUES (?, ?, ?, ?, ?)", q.jobs)},
		{&q.unmark, fmt.Sprintf("DELETE FROM %s WHERE job_id = ?", q.cancelled)},
		{&q.hasMark, fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE expires_at > ?)", q.cancelled)},
		{&q.isMarked, fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE job_id = ? AND expires_at > ?)", q.cancelled)},
		{&q.count, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = 'pending'", q.table)},
		{&q.countCapped, fmt.Sprintf("SELECT COUNT(*) FROM (SELECT 1 FROM %s WHERE status = 'pending' LIMIT %d)", q.table, remainingCap)},
		{&q.values, fmt.Sprintf("SELECT data FROM %s WHERE status = 'pending' ORDER BY %s", q.table, q.order)},
		{&q.pausedAt, fmt.Sprintf("SELECT value FROM %s WHERE key = 'paused_at'", q.meta)},
		{&q.tally, fmt.Sprintf("UPDATE %s SET value = value + 1 WHERE key = ?", q.meta)},
	} {
		if *s.stmt, err = q.f.db.Prepare(s.query); err != nil {
			return err
		}
	}

	return nil
}

// closeStmts closes the statements prepared so far
func (q *Queue) closeStmts() {
	for _, s := range []*sql.Stmt{q.insert, q.claim, q.pop, q.fin, q.count, q.countCapped, q.values, q.putJob, q.unmark, q.hasMark, q.isMarked, q.pausedAt, q.tally} {
		if s != nil {
			s.Close()
		}
	}
}

// requeueNoAck puts rows left in processing without an ack back to pending
func (q *Queue) requeueNoAck() error {
	q.f.mu.Lock()
	defer q.f.mu.Unlock()

	_, err := q.f.db.Exec(
		fmt.Sprintf("UPDATE %s SET status = 'pending', updated_at = ? WHERE status = 'processing' AND ack = 0", q.table),
		time.Now().UTC(),
	)
	return err
}

// hasColumn reports whether the table has a column with the given name
func hasColumn(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(table)))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid     int
			name    string
			typ     string
			notNull int
			dflt    sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}

	return false, rows.Err()
}

// parseAckID turns an ack id back into its row id. Only the canonical decimal
// of a positive id is accepted, so a v0.2 cuid or any other spelling acks nothing.
func parseAckID(s string) (int64, bool) {
	if s == "" || s[0] < '1' || s[0] > '9' {
		return 0, false
	}
	id, err := strconv.ParseInt(s, 10, 64)
	return id, err == nil && id > 0
}

// ScopeID implements contract.Scoped: "sqliteq:" + the store root, the
// folder (or the file under WithSingleFile). Queues of one root share a
// workflow store.
func (q *Queue) ScopeID() string {
	return q.f.scope
}

// finishDirect is finish in a transaction of its own, outside the group
// commit: found is false when no processing row had the ack ID, which is no
// error (a re-finish).
func (q *Queue) finishDirect(ackID string, state contract.JobState, reason string, result []byte) (found bool, err error) {
	id, ok := parseAckID(ackID)
	if !ok {
		return false, nil
	}
	q.f.mu.Lock()
	defer q.f.mu.Unlock()
	tx, err := q.f.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if found, err = q.finishTx(tx, id, state, reason, result, time.Now()); err != nil {
		return false, err
	}
	return found, tx.Commit()
}

// Enqueue adds an item to the queue
// Returns true if the operation was successful
func (q *Queue) Enqueue(item any) bool {
	return q.enqueue(item, 0, "")
}

// EnqueueOnce implements varmq.IDedupQueue: it adds item under id unless a
// row with that id exists, pending, processing, or completed with
// WithRemoveOnComplete(false). A repeat stores nothing and returns true. An
// empty id behaves like Enqueue.
func (q *Queue) EnqueueOnce(item any, id string) bool {
	return q.enqueue(item, 0, id)
}

// enqueue inserts one pending row in the file's next group commit round;
// priority is stored on priority queues only. A row with a non-empty id is
// skipped, still with true, when a row with that id exists.
func (q *Queue) enqueue(item any, priority int, id string) bool {
	if q.closed.Load() {
		return false
	}

	// Convert here, as database/sql would inside Exec, so the item's own code
	// (a driver.Valuer) runs in its caller and never in a round's leader
	data, err := driver.DefaultParameterConverter.ConvertValue(item)
	if err != nil {
		return false
	}

	// A wf: ID belongs to workflow items; a plain job must not hold one
	if strings.HasPrefix(id, "wf:") {
		var b []byte
		switch v := data.(type) {
		case []byte:
			b = v
		case string:
			b = []byte(v)
		}
		if tag, ok := workflowTag(b); !ok || tag.jobID() != id {
			return false
		}
	}

	// No id is NULL, which the job_id index leaves out; "" would be one id
	var jobID any
	if id != "" {
		jobID = id
	}

	now := time.Now().UTC()
	return q.f.b.do(func(tx *sql.Tx) (bool, error) {
		insert := q.insert
		if tx != nil {
			insert = tx.Stmt(insert)
		}
		// One statement, so a round of one, run without a transaction, is atomic
		var err error
		if q.priority {
			_, err = insert.Exec(data, now, now, jobID, priority)
		} else {
			_, err = insert.Exec(data, now, now, jobID)
		}
		// A repeat id changes no row and is no error: it succeeds
		return err == nil, err
	})
}

// errJobIDTaken: the job ID belongs to a row that is not the same workflow task.
var errJobIDTaken = errors.New("sqliteq: job id is held by another job")

// enqueueTx is enqueue in the caller's tx, which holds the file's write
// mutex: one pending row under id. A repeat is fine only when the stored row
// is the same workflow item (same wf, task and undo); else errJobIDTaken.
func (q *Queue) enqueueTx(tx *sql.Tx, item []byte, priority int, id string) error {
	now := time.Now().UTC()
	insert := tx.Stmt(q.insert)
	var res sql.Result
	var err error
	if q.priority {
		res, err = insert.Exec(item, now, now, id, priority)
	} else {
		res, err = insert.Exec(item, now, now, id)
	}
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n > 0 {
		return err
	}
	var data []byte
	if err := tx.QueryRow(fmt.Sprintf("SELECT data FROM %s WHERE job_id = ?", q.table), id).Scan(&data); err != nil {
		return err
	}
	want, ok := workflowTag(item)
	if have, hok := workflowTag(data); !ok || !hok || have != want {
		return errJobIDTaken
	}
	return nil
}

// pushWorkflow adds a workflow item under its job ID in a transaction of its
// own (see enqueueTx).
func (q *Queue) pushWorkflow(item []byte, priority int, id string) error {
	if q.closed.Load() {
		return errClosed
	}
	q.f.mu.Lock()
	defer q.f.mu.Unlock()
	tx, err := q.f.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := q.enqueueTx(tx, item, priority, id); err != nil {
		return err
	}
	return tx.Commit()
}

// claimN moves up to n pending rows to processing in one statement and
// returns them in queue order; refused reports a pause stopped it
func (q *Queue) claimN(n int) (out []claimed, capped, refused bool, err error) {
	g := q.pz.gen.Load()
	q.f.mu.Lock()
	out, capped, refused, err = q.claimGated(n)
	q.f.mu.Unlock()
	if refused {
		q.refused(g)
	}
	if err != nil || refused {
		return nil, false, refused, err
	}

	// RETURNING order is unspecified
	slices.SortFunc(out, func(a, b claimed) int {
		if c := cmp.Compare(a.priority, b.priority); c != 0 {
			return c
		}
		return cmp.Compare(a.id, b.id)
	})
	return out, capped, false, nil
}

// claimGated claims up to n rows; with a live cancel mark on the file it
// runs in one transaction that drops each marked row it claims (outcome
// cancelled) and claims again, at most maxDrops drops; capped reports that
// it stopped there. An empty claim reads the pause row: refused when it is
// set. Outside a tx a Resume from another process can land between the two,
// so an empty claim found unpaused claims once more. The caller holds the
// write mutex.
// ponytail: a second Pause and Resume, both in the window again, still make
// one empty pop look like a failure.
func (q *Queue) claimGated(n int) (out []claimed, capped, refused bool, err error) {
	now := time.Now()
	if !q.anyMark(now) {
		for try := range 2 {
			if out, err = q.claimRows(q.claim, n); err != nil || len(out) > 0 {
				return out, false, false, err
			}
			afterEmptyPop()
			if refused, err = readPaused(q.pausedAt); err != nil || refused || try == 1 {
				return nil, false, refused, err
			}
		}
	}

	tx, err := q.f.db.Begin()
	if err != nil {
		return nil, false, false, err
	}
	defer tx.Rollback()

	claim, isMarked := tx.Stmt(q.claim), tx.Stmt(q.isMarked)
	out = make([]claimed, 0, n)
	drops := 0
	for len(out) < n && drops < maxDrops {
		// No more than the drops left: a batch never passes the cap
		rows, err := q.claimRows(claim, min(n-len(out), maxDrops-drops))
		if err != nil {
			return nil, false, false, err
		}
		if len(rows) == 0 {
			break
		}
		for _, c := range rows {
			id := jobIDOf(c.once, c.data)
			drop, err := markedDrop(isMarked, id, c.data, now)
			if err != nil {
				return nil, false, false, err
			}
			if !drop {
				out = append(out, c)
				continue
			}
			if _, err := q.end(tx, c.id, id, contract.JobCancelled, "cancelled", nil, now); err != nil {
				return nil, false, false, err
			}
			drops++
		}
	}
	capped = len(out) < n && drops >= maxDrops
	if len(out) == 0 && !capped {
		// The write lock is held: the row read is the one the claim saw
		if refused, err = readPaused(tx.Stmt(q.pausedAt)); err != nil {
			return nil, false, false, err
		}
	}
	return out, capped, refused, tx.Commit()
}

// claimRows runs a claim statement and reads every row it returns; the
// caller holds the write mutex
func (q *Queue) claimRows(claim *sql.Stmt, n int) ([]claimed, error) {
	rows, err := claim.Query(time.Now().UTC(), n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]claimed, 0, n)
	for rows.Next() {
		var c claimed
		if q.priority {
			err = rows.Scan(&c.id, &c.priority, &c.data, &c.once)
		} else {
			err = rows.Scan(&c.id, &c.data, &c.once)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	// Reading to the end runs the statement to completion, which commits it
	return out, rows.Err()
}

// Dequeue removes and returns the next item from the queue
// Returns the item and a boolean indicating if the operation was successful
func (q *Queue) Dequeue() (any, bool) {
	if q.closed.Load() {
		return nil, false
	}

	g := q.pz.gen.Load()
	q.f.mu.Lock()
	data, ok, refused := q.dequeue()
	q.f.mu.Unlock()
	if refused {
		q.refused(g)
	}
	return data, ok
}

// dequeue is Dequeue under the write mutex; an empty pop reads the pause
// row, refused when it is set, and pops once more when it is not (see
// claimGated).
func (q *Queue) dequeue() (data []byte, ok, refused bool) {
	now := time.Now()
	if !q.anyMark(now) {
		for try := range 2 {
			data, _, err := popRow(q.pop)
			if !errors.Is(err, sql.ErrNoRows) {
				return data, err == nil, false
			}
			afterEmptyPop()
			if refused, err = readPaused(q.pausedAt); err != nil || refused || try == 1 {
				return nil, false, refused
			}
		}
	}
	for {
		data, capped, refused, err := q.popGated(now)
		if err != nil || data != nil || refused || !capped {
			return data, err == nil && data != nil, refused
		}
	}
}

// popGated pops in one transaction, dropping each marked item it pops
// (outcome cancelled), at most maxDrops; capped reports it stopped there
// with nothing, refused that the pause row stopped it. The caller holds
// the write mutex.
func (q *Queue) popGated(now time.Time) (data []byte, capped, refused bool, err error) {
	tx, err := q.f.db.Begin()
	if err != nil {
		return nil, false, false, err
	}
	defer tx.Rollback()
	pop, isMarked := tx.Stmt(q.pop), tx.Stmt(q.isMarked)
	for range maxDrops {
		data, once, err := popRow(pop)
		if errors.Is(err, sql.ErrNoRows) {
			if refused, err = readPaused(tx.Stmt(q.pausedAt)); err != nil {
				return nil, false, false, err
			}
			return nil, false, refused, tx.Commit() // keep the drops
		}
		if err != nil {
			return nil, false, false, err
		}
		id := jobIDOf(once, data)
		drop, err := markedDrop(isMarked, id, data, now)
		if err != nil {
			return nil, false, false, err
		}
		if !drop {
			return data, false, false, tx.Commit()
		}
		if err := q.record(tx, id, contract.JobCancelled, "cancelled", nil, now); err != nil {
			return nil, false, false, err
		}
	}
	return nil, true, false, tx.Commit()
}

// popRow runs a pop statement: the deleted row's data and job_id, or
// sql.ErrNoRows on an empty queue
func popRow(pop *sql.Stmt) (data []byte, once sql.NullString, err error) {
	err = pop.QueryRow().Scan(&data, &once)
	return data, once, err
}

// DequeueWithAckId moves the next item to processing and returns it with its
// acknowledgment ID, the row id in decimal
func (q *Queue) DequeueWithAckId() (any, bool, string) {
	if q.closed.Load() {
		return nil, false, ""
	}

	// A pop stopped at the drop cap has nothing yet: pop again, it can't
	// report items left
	for {
		c, capped, refused, err := q.claimN(1)
		if err != nil || refused || len(c) == 0 && !capped {
			return nil, false, ""
		}
		if len(c) > 0 {
			return c[0].data, true, strconv.FormatInt(c[0].id, 10)
		}
	}
}

// DequeueBatchWithAckIds implements varmq.IBatchAcknowledgeable: it moves up
// to min(len(items), len(ackIDs)) items to processing in one statement, in
// queue order, each under its row id as ack id. remaining is the pending
// count after the move, capped at remainingCap. n is 0 with a nil error when
// the queue is empty or closed.
func (q *Queue) DequeueBatchWithAckIds(items []any, ackIDs []string) (n, remaining int, err error) {
	want := min(len(items), len(ackIDs))
	if q.closed.Load() || want == 0 {
		return 0, 0, nil
	}

	c, capped, refused, err := q.claimN(want)
	if err != nil || refused {
		return 0, 0, err // paused: nothing left to pop until a Resume
	}
	// At the drop cap items may wait still: report one, so the caller pops again
	left := 0
	if capped {
		left = 1
	}
	if len(c) == 0 {
		return 0, left, nil
	}
	for i, row := range c {
		items[i], ackIDs[i] = row.data, strconv.FormatInt(row.id, 10)
	}

	// The rows are claimed already: an error here would make varmq drop them,
	// so a failed count reports items left and the next pop finds out
	if err := q.countCapped.QueryRow().Scan(&remaining); err != nil {
		return len(c), 1, nil
	}
	return len(c), max(remaining, left), nil
}

// Acknowledge marks a processing item as completed, deleting it unless
// WithRemoveOnComplete(false) is set, in the file's next group commit round
// Returns true only if exactly one processing row had this ack ID
func (q *Queue) Acknowledge(ackID string) bool {
	return q.finish(ackID, contract.JobCompleted, "", nil)
}

// Reject marks a processing item as failed for good: it is not requeued.
// The row is deleted, or with WithRemoveOnComplete(false) kept with status
// 'failed'. reason is the error of the job's outcome. Same group commit and
// result as Acknowledge.
func (q *Queue) Reject(ackID string, reason string) bool {
	return q.finish(ackID, contract.JobFailed, reason, nil)
}

// finish ends the processing row of ackID with state in the file's next
// group commit round. A round of one runs in its own transaction: the row
// and its outcome record commit together.
func (q *Queue) finish(ackID string, state contract.JobState, reason string, result []byte) bool {
	id, ok := parseAckID(ackID)
	if !ok {
		return false
	}

	now := time.Now()
	return q.f.b.do(func(tx *sql.Tx) (ok bool, err error) {
		if tx == nil {
			if tx, err = q.f.db.Begin(); err != nil {
				return false, err
			}
			defer func() {
				if err == nil {
					err = tx.Commit()
				} else {
					tx.Rollback()
				}
				ok = ok && err == nil
			}()
		}
		return q.finishTx(tx, id, state, reason, result, now)
	})
}

// finishTx is finish in the caller's tx, with the row id parsed.
func (q *Queue) finishTx(tx *sql.Tx, id int64, state contract.JobState, reason string, result []byte, now time.Time) (bool, error) {
	// No row is an outcome, not an error: it must not fail the round
	if ok, err := q.end(tx, id, "", state, reason, result, now); !ok || err != nil {
		return ok, err
	}
	// Totals: cancelled counts as failed (spec 6.14)
	total := "failed"
	if state == contract.JobCompleted {
		total = "completed"
	}
	_, err := tx.Stmt(q.tally).Exec(total)
	return err == nil, err
}

// end finishes processing row id with state, in tx: the row is deleted or
// kept with state as status, and a job with a Job ID gets its outcome
// record. jobID is the row's, when the caller knows it. Reports whether
// the row was processing.
func (q *Queue) end(tx *sql.Tx, id int64, jobID string, state contract.JobState, reason string, result []byte, now time.Time) (bool, error) {
	var row *sql.Row
	if q.removeOnComplete {
		row = tx.Stmt(q.fin).QueryRow(id)
	} else {
		row = tx.Stmt(q.fin).QueryRow(string(state), now.UTC(), id)
	}
	var (
		once sql.NullString
		data []byte
	)
	switch err := row.Scan(&once, &data); {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	}
	if jobID == "" {
		jobID = jobIDOf(once, data)
	}
	if jobID == "" {
		return true, nil
	}
	return true, q.record(tx, jobID, state, reason, result, now)
}

// Len returns the number of pending items in the queue
func (q *Queue) Len() int {
	var count int
	if err := q.count.QueryRow().Scan(&count); err != nil {
		return 0
	}
	return count
}

// Values returns all pending items in the queue, in queue order
func (q *Queue) Values() []any {
	rows, err := q.values.Query()
	if err != nil {
		return nil
	}
	defer rows.Close()

	var items []any
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			continue
		}

		// Now we just add the byte array directly as we're storing byte arrays
		// instead of JSON-serialized data
		items = append(items, data)
	}

	return items
}

// Purge removes all items from the queue, with its outcome records, cancel
// marks and totals; a pause stays (spec 6.6, 6.7, 6.14). A purged workflow
// task or undo ends cancelled (reason "queue purged"), so its workflow goes
// on: dependents follow their policy, and what that releases is pushed after
// the delete. The workflow store is the registered one, or one opened on
// demand when the file has workflow tables. Under WithSingleFile the delete
// and the ends are one transaction. Folder layout: an intent is stored in
// the workflow file first, and the recovery loop applies one a crash left.
// Workflow tables there but no store ready (check, open or init failed):
// nothing is deleted, the cause is logged; a later Purge can retry.
func (q *Queue) Purge() {
	ws, done, err := q.f.purgeStore()
	defer done()
	if err != nil {
		log.Printf("sqliteq: purge of %s: jobs kept, workflow store not ready: %v", q.name, err)
		return
	}
	if ws == nil {
		q.purge(nil)
		return
	}
	ctx := context.Background()
	pending := q.pendingTags()
	if ws.single {
		for _, tag := range pending {
			ws.preResolve(ctx, tag.wf)
		}
		for _, tag := range q.purge(ws) {
			ws.drain(ctx, `workflow_id = ?`, tag.wf)
		}
		return
	}
	intents := ws.recordPurge(ctx, q, pending)
	ws.endPurged(q, intents, q.purge(ws))
}

// pendingTags reads the workflow tags of the queued and running items.
func (q *Queue) pendingTags() []wfItemTag {
	tx, err := q.f.db.Begin()
	if err != nil {
		return nil
	}
	defer tx.Rollback()
	tags, _ := q.workflowTags(tx)
	return tags
}

// purge deletes the rows under the write mutex and returns the workflow tags
// it found in them (read in the same transaction); with a single-file store
// it also ends those tasks in the transaction, and returns them.
func (q *Queue) purge(ws *WorkflowStore) (tags []wfItemTag) {
	q.f.mu.Lock()
	defer q.f.mu.Unlock()

	tx, err := q.f.db.Begin()
	if err != nil {
		return nil
	}
	defer tx.Rollback()
	if ws != nil {
		if tags, err = q.workflowTags(tx); err != nil {
			return nil
		}
	}
	for _, t := range []string{q.table, q.jobs, q.cancelled} {
		if _, err := tx.Exec("DELETE FROM " + t); err != nil {
			return nil
		}
	}
	if _, err := tx.Exec("UPDATE " + q.meta + " SET value = 0 WHERE key <> 'paused_at'"); err != nil {
		return nil
	}
	if ws != nil && ws.single {
		ctx := context.Background()
		for _, tag := range tags {
			e := contract.TaskEnd{Workflow: tag.wf, Task: tag.task, Undo: tag.undo, Outcome: contract.OutcomeCancelled, Reason: purgeReason}
			if _, err := ws.endTx(ctx, tx, q.id, e); err != nil {
				log.Printf("sqliteq: purge of %s: ending workflow %s task %s: %v", q.name, tag.wf, tag.task, err)
				return nil
			}
		}
	}
	if tx.Commit() != nil {
		return nil
	}
	return tags
}

// workflowTags reads the tags of the queued and running workflow items, in
// queue order.
func (q *Queue) workflowTags(tx *sql.Tx) ([]wfItemTag, error) {
	rows, err := tx.Query(fmt.Sprintf(`SELECT data FROM %s WHERE status IN ('pending', 'processing')
		AND (instr(CAST(data AS TEXT), '"wf"') > 0 OR instr(CAST(data AS TEXT), char(92)) > 0) ORDER BY %s`, q.table, q.order))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tags []wfItemTag
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		if tag, ok := workflowTag(data); ok {
			tags = append(tags, tag)
		}
	}
	return tags, rows.Err()
}

// Close closes the queue and drops its outcome watches and OnCancel hooks
// without calling them. The database file stays open for the Queues.
func (q *Queue) Close() error {
	q.closed.Store(true)
	q.w.close()

	return nil
}
