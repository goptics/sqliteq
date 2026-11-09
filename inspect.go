package sqliteq

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
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

// initMeta creates the meta table (key -> integer: the totals submitted,
// completed, failed; paused_at while paused) and the trigger that counts
// stored adds; a repeat EnqueueOnce stores no row, so fires none. The
// caller holds the write mutex.
func (q *Queue) initMeta() error {
	for _, s := range []string{
		`CREATE TABLE IF NOT EXISTS %[2]s (key TEXT PRIMARY KEY, value INTEGER NOT NULL)`,
		// An older table starts from the rows it holds
		`INSERT OR IGNORE INTO %[2]s (key, value) VALUES
			('submitted', (SELECT COUNT(*) FROM %[1]s)),
			('completed', (SELECT COUNT(*) FROM %[1]s WHERE status = 'completed')),
			('failed', (SELECT COUNT(*) FROM %[1]s WHERE status IN ('failed', 'cancelled')))`,
		`CREATE TRIGGER IF NOT EXISTS %[3]s AFTER INSERT ON %[1]s BEGIN
			UPDATE %[2]s SET value = value + 1 WHERE key = 'submitted';
		END`,
	} {
		if _, err := q.f.db.Exec(fmt.Sprintf(s, q.table, q.meta, quoteIdent(q.name+"__sqliteq_submitted"))); err != nil {
			return err
		}
	}
	return nil
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
// dropped for a cancel is neither) since the last Purge; an older table
// starts from the rows it held. Paused is the file's, not the cache.
// ponytail: counts scan the table; index status if kept rows grow large.
func (q *Queue) QueueStats(ctx context.Context) (contract.QueueStats, error) {
	if q.closed.Load() {
		return contract.QueueStats{}, errClosed
	}
	var (
		s        contract.QueueStats
		pausedAt sql.NullInt64
	)
	err := q.f.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT
		COUNT(*) FILTER (WHERE status = 'pending'),
		COUNT(*) FILTER (WHERE status = 'processing'),
		COUNT(*) FILTER (WHERE status = 'completed'),
		COUNT(*) FILTER (WHERE status IN ('failed', 'cancelled')),
		COALESCE((SELECT value FROM %[2]s WHERE key = 'submitted'), 0),
		COALESCE((SELECT value FROM %[2]s WHERE key = 'completed'), 0),
		COALESCE((SELECT value FROM %[2]s WHERE key = 'failed'), 0),
		(SELECT value FROM %[2]s WHERE key = 'paused_at')
		FROM %[1]s`, q.table, q.meta)).Scan(
		&s.Counts.Pending, &s.Counts.Processing, &s.Counts.Completed, &s.Counts.Failed,
		&s.Totals.Submitted, &s.Totals.Completed, &s.Totals.Failed, &pausedAt)
	if err != nil {
		return contract.QueueStats{}, err
	}
	s.Paused, s.PausedAt = pausedAt.Valid, pausedAt.Int64
	return s, nil
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
