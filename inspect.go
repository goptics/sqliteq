package sqliteq

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/goptics/varmq/contract"
)

// Queue inspection (varmq contract.QueueInspector; spec redis-storage.md 7)
// on SQLite: counts from the queue's table, totals from <name>__sqliteq_meta.
// Completed and failed jobs are rows kept by WithRemoveOnComplete(false);
// by default ended rows are deleted, so those counts and pages are empty.

var (
	_ contract.QueueInspector = (*Queue)(nil)
	_ contract.QueueInspector = (*PriorityQueue)(nil)
)

const metaSuffix = "__sqliteq_meta"

// initMeta creates the meta table (key -> integer; paused_at while
// paused) and the counters totals derive from, so no hot write touches it:
//   - submitted = the table's AUTOINCREMENT high mark - base; a repeat
//     EnqueueOnce bumps the mark without a row, so it bumps base too
//   - failed: Reject and cancel finishes
//   - other: rows gone without a finish (a Dequeue pop, a cancel drop),
//     counted by the pop or drop itself
//   - completed = submitted - pending - processing - failed - other
//
// An older table starts from the rows it holds; a meta of an earlier
// build (submitted and completed counted per write) is converted. The
// caller holds the write mutex.
func (q *Queue) initMeta() error {
	if _, err := q.f.db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (key TEXT PRIMARY KEY, value INTEGER NOT NULL)`, q.meta)); err != nil {
		return err
	}
	tx, err := q.f.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var base sql.NullInt64
	if err := tx.QueryRow(fmt.Sprintf(`SELECT value FROM %s WHERE key = 'base'`, q.meta)).Scan(&base); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if !base.Valid {
		var mark, rows, pending, processing, failed int64
		if err := tx.QueryRow(fmt.Sprintf(`SELECT %s,
			COUNT(*), COUNT(*) FILTER (WHERE status = 'pending'), COUNT(*) FILTER (WHERE status = 'processing'),
			COUNT(*) FILTER (WHERE status IN ('failed', 'cancelled')) FROM %s`, q.highMark(), q.table), q.name).Scan(
			&mark, &rows, &pending, &processing, &failed); err != nil {
			return err
		}
		var other int64
		var sub, comp, fail sql.NullInt64
		if err := tx.QueryRow(fmt.Sprintf(`SELECT (SELECT value FROM %[1]s WHERE key = 'submitted'),
			(SELECT value FROM %[1]s WHERE key = 'completed'), (SELECT value FROM %[1]s WHERE key = 'failed')`, q.meta)).Scan(
			&sub, &comp, &fail); err != nil {
			return err
		}
		if sub.Valid {
			// Earlier build: keep its totals as they read now
			rows, failed = sub.Int64, fail.Int64
			other = sub.Int64 - pending - processing - fail.Int64 - comp.Int64
		}
		for _, kv := range []struct {
			k string
			v int64
		}{{"base", mark - rows}, {"failed", failed}, {"other", other}} {
			if _, err := tx.Exec(fmt.Sprintf(`INSERT OR REPLACE INTO %s (key, value) VALUES (?, ?)`, q.meta), kv.k, kv.v); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key IN ('submitted', 'completed')`, q.meta)); err != nil {
			return err
		}
	}
	for _, s := range []string{
		// The earlier build's per-add counter, and a pop counter trigger of
		// an earlier build of this one: any trigger on the queue table
		// slows every write of it, so pops count in their own transaction
		`DROP TRIGGER IF EXISTS %[3]s`,
		`DROP TRIGGER IF EXISTS %[4]s`,
	} {
		if _, err := tx.Exec(fmt.Sprintf(s, q.table, q.meta, quoteIdent(q.name+"__sqliteq_submitted"), quoteIdent(q.name+"__sqliteq_popped"))); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// highMark is the SQL of the table's AUTOINCREMENT high mark, 0 before its
// first add; it takes the queue name as its one argument. sqlite_sequence
// keeps the name as the table was created; SQLite matches names
// case-blind.
func (q *Queue) highMark() string {
	return `COALESCE((SELECT seq FROM sqlite_sequence WHERE name = ? COLLATE NOCASE), 0)`
}

// queueID is sqliteq:<h>:fifo|prio:<name>: <h> the first 8 hex of the
// SHA-256 of the file's resolved path, so every process on the file agrees,
// two files never collide, and no path shows; the name in lower case, as
// SQLite matches tables. sqliteq has no in-memory mode: every queue is in
// a file.
func queueID(path, name string, priority bool) string {
	sum := sha256.Sum256([]byte(path))
	kind := "fifo"
	if priority {
		kind = "prio"
	}
	return "sqliteq:" + hex.EncodeToString(sum[:4]) + ":" + kind + ":" + foldName(name)
}

// QueueID implements contract.QueueInspector (see queueID).
func (q *Queue) QueueID() string {
	return q.id
}

// QueueStats implements contract.QueueInspector, in one statement (one
// snapshot). Counts: rows per status, cancelled under failed. Totals:
// adds stored and jobs finished (cancelled ones as failed; a waiting job
// dropped for a cancel or popped by Dequeue is neither) since the last
// Purge, derived from the table's AUTOINCREMENT high mark and counters of
// the rare paths; an older table starts from the rows it held. Paused is the file's, not the cache.
// ponytail: counts scan the table; index status if kept rows grow large.
func (q *Queue) QueueStats(ctx context.Context) (contract.QueueStats, error) {
	if q.closed.Load() {
		return contract.QueueStats{}, errClosed
	}
	var (
		s                        contract.QueueStats
		pausedAt                 sql.NullInt64
		submitted, failed, other int64
	)
	err := q.f.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT
		COUNT(*) FILTER (WHERE status = 'pending'),
		COUNT(*) FILTER (WHERE status = 'processing'),
		COUNT(*) FILTER (WHERE status = 'completed'),
		COUNT(*) FILTER (WHERE status IN ('failed', 'cancelled')),
		%[3]s - COALESCE((SELECT value FROM %[2]s WHERE key = 'base'), 0),
		COALESCE((SELECT value FROM %[2]s WHERE key = 'failed'), 0),
		COALESCE((SELECT value FROM %[2]s WHERE key = 'other'), 0),
		(SELECT value FROM %[2]s WHERE key = 'paused_at')
		FROM %[1]s`, q.table, q.meta, q.highMark()), q.name).Scan(
		&s.Counts.Pending, &s.Counts.Processing, &s.Counts.Completed, &s.Counts.Failed,
		&submitted, &failed, &other, &pausedAt)
	if err != nil {
		return contract.QueueStats{}, err
	}
	completed := submitted - int64(s.Counts.Pending) - int64(s.Counts.Processing) - failed - other
	s.Totals.Submitted, s.Totals.Completed, s.Totals.Failed = clampU(submitted), clampU(completed), clampU(failed)
	s.Paused, s.PausedAt = pausedAt.Valid, pausedAt.Int64
	return s, nil
}

// clampU is v as a total: 0 below 0, which only a file a build without
// these counters changed can reach.
func clampU(v int64) uint64 {
	return uint64(max(v, 0))
}

// Jobs implements contract.QueueInspector: pending in hand-out order,
// processing by ack id, completed, failed (with cancelled, state
// cancelled) and cancelled newest finish first. Not counted by sqliteq:
// attempts are 0 pending, else 1; an ended row's start is lost. errors: one entry, from
// the job's outcome record, when it has one.
// ponytail: ended pages sort by updated_at unindexed; fine until kept rows
// grow large.
func (q *Queue) Jobs(ctx context.Context, state contract.JobState, offset, limit int) ([]contract.JobRecord, error) {
	where, order := "", "id"
	switch state {
	case contract.JobPending:
		where, order = "status = 'pending'", q.order
	case contract.JobProcessing:
		where = "status = 'processing'"
	case contract.JobCompleted:
		where, order = "status = 'completed'", "updated_at DESC, id DESC"
	case contract.JobFailed:
		where, order = "status IN ('failed', 'cancelled')", "updated_at DESC, id DESC"
	case contract.JobCancelled:
		where, order = "status = 'cancelled'", "updated_at DESC, id DESC"
	default:
		return nil, fmt.Errorf("sqliteq: unknown job state %q", state)
	}
	if q.closed.Load() {
		return nil, errClosed
	}
	jobs := []contract.JobRecord{}
	if limit <= 0 {
		return jobs, nil
	}

	rows, err := q.f.db.QueryContext(ctx, fmt.Sprintf("SELECT id, data, job_id, status, created_at, updated_at FROM %s WHERE %s ORDER BY %s LIMIT ? OFFSET ?",
		q.table, where, order), limit, max(0, offset))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ended []int // failed and cancelled jobs with a Job ID
	for rows.Next() {
		var (
			id               int64
			data             []byte
			once             sql.NullString
			status           string
			created, updated any
		)
		if err := rows.Scan(&id, &data, &once, &status, &created, &updated); err != nil {
			return nil, err
		}
		j := contract.JobRecord{ID: jobIDOf(once, data), State: contract.JobState(status)}
		j.Payload, j.EnqueuedAt = unwrap(data)
		if j.EnqueuedAt == 0 {
			j.EnqueuedAt = unixMilli(created)
		}
		switch state {
		case contract.JobPending:
		case contract.JobProcessing:
			j.StartedAt, j.Attempts = unixMilli(updated), 1
		default:
			j.FinishedAt, j.Attempts = unixMilli(updated), 1
			if j.ID != "" && state != contract.JobCompleted {
				ended = append(ended, len(jobs))
			}
		}
		// Spec 3.1: a handed-out job without an id goes by its ack id
		if j.ID == "" && state != contract.JobPending {
			j.ID = strconv.FormatInt(id, 10)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ended) > 0 {
		if err := q.addErrors(ctx, jobs, ended); err != nil {
			return nil, err
		}
	}
	return jobs, nil
}

// addErrors gives each jobs[i] of ended its outcome record's error, when
// the record has its state; retention may have trimmed it.
func (q *Queue) addErrors(ctx context.Context, jobs []contract.JobRecord, ended []int) error {
	args := make([]any, len(ended))
	for i, at := range ended {
		args[i] = jobs[at].ID
	}
	rows, err := q.f.db.QueryContext(ctx, fmt.Sprintf("SELECT job_id, state, error FROM %s WHERE job_id IN (%s) AND error IS NOT NULL",
		q.jobs, strings.Repeat("?, ", len(args)-1)+"?"), args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	type outcome struct{ state, error string }
	byID := map[string]outcome{}
	for rows.Next() {
		var id string
		var o outcome
		if err := rows.Scan(&id, &o.state, &o.error); err != nil {
			return err
		}
		byID[id] = o
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, at := range ended {
		j := &jobs[at]
		if o, ok := byID[j.ID]; ok && o.state == string(j.State) {
			j.Errors = []contract.JobError{{Attempt: 1, At: j.FinishedAt, Error: o.error}}
		}
	}
	return nil
}

// unixMilli is a TIMESTAMP column read back as Unix ms; 0 when NULL or
// not a time.
func unixMilli(v any) int64 {
	if t, ok := v.(time.Time); ok {
		return t.UnixMilli()
	}
	return 0
}

// unwrap returns the payload and enqueue time of item: an envelope's data
// and enqueued_at, or for anything else the item itself if it is JSON, else
// the item as a JSON string. The bytes are kept as stored, never re-encoded.
func unwrap(item []byte) (json.RawMessage, int64) {
	var env struct {
		Status     json.RawMessage `json:"status"`
		Data       json.RawMessage `json:"data"`
		EnqueuedAt json.RawMessage `json:"enqueued_at"`
	}
	// An envelope has status and data (spec section 3). A field is nil only
	// when its key is absent: a JSON null still sets it to "null".
	if bytes.HasPrefix(bytes.TrimSpace(item), []byte("{")) && json.Unmarshal(item, &env) == nil && env.Status != nil && env.Data != nil {
		var at int64
		_ = json.Unmarshal(env.EnqueuedAt, &at)
		return env.Data, at
	}
	if json.Valid(item) {
		return item, 0
	}
	s, _ := json.Marshal(string(item))
	return s, 0
}
