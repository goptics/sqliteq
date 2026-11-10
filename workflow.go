package sqliteq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goptics/varmq/contract"
)

// Workflows (varmq spec/redis-storage.md 6.9 to 6.12), on SQLite. The store
// keeps workflows, tasks and an outbox in three tables:
// __sqliteq_wf_workflows, __sqliteq_wf_tasks, __sqliteq_wf_outbox. In folder
// layout they live in their own file, <root>/__sqliteq_workflows.db; under
// WithSingleFile, in the store's file. Each end runs the state machine
// (wfmachine.go) over the workflow loaded in one write transaction.
//
// Separate files cannot share one atomic transaction, so folder layout pushes
// what an end releases through the outbox: the release is written in the
// workflow transaction, then pushed into the queue with EnqueueOnce, then
// deleted from the outbox. A recovery loop pushes leftovers. A duplicate
// push or a redelivered job is safe: the pre-run check (Runnable) skips a
// task that already ended. Under WithSingleFile the end, the pushes and the
// ack are one transaction.

var (
	_ contract.WorkflowStore     = (*WorkflowStore)(nil)
	_ contract.WorkflowInspector = (*WorkflowStore)(nil)
	_ contract.WorkflowKeyLister = (*WorkflowStore)(nil)
	_ contract.WorkflowCounter   = (*WorkflowStore)(nil)
)

const (
	defaultResultLimit = 64 << 10
	defaultRetention   = 7 * 24 * time.Hour

	// workflowsFile is the folder layout's workflow database.
	workflowsFile = "__sqliteq_workflows.db"

	wfWorkflows = `"__sqliteq_wf_workflows"`
	wfTasks     = `"__sqliteq_wf_tasks"`
	wfOutbox    = `"__sqliteq_wf_outbox"`
	wfPurged    = `"__sqliteq_wf_purged"`

	// sweepWorkflowsEvery is how often the loop drops expired workflows.
	sweepWorkflowsEvery = time.Minute
	// sweepOnSubmit is the least time between the sweeps Submit starts.
	sweepOnSubmit = 30 * time.Second
	// drainBatch is how many outbox rows one read takes.
	drainBatch = 256
	// drainCap is the most rows one drain pushes: a backlog goes on next tick.
	drainCap = 2048
	// purgeReason is the reason of a task its queue's Purge ended.
	purgeReason = "queue purged"
)

// taskTimeCols are the task time columns (contract v0.9.0), Unix ms, 0 when
// not reached. init adds them to a table made before them.
var taskTimeCols = [...]string{"enqueued_at", "started_at", "finished_at",
	"undo_enqueued_at", "undo_started_at", "undo_finished_at"}

// recoverEvery is how often the recovery loop pushes leftover outbox rows;
// those younger than it are left to the writer that is about to push them.
var recoverEvery = time.Second

// ErrResultTooLarge is contract.ErrResultTooLarge: a completed task whose
// result is over the store's limit fails with it.
var ErrResultTooLarge = contract.ErrResultTooLarge

type workflowConfig struct {
	resultLimit int
	retention   time.Duration
}

// WorkflowOption configures NewWorkflowStore.
type WorkflowOption func(*workflowConfig) error

// WithWorkflowResultLimit caps a workflow task's result at n bytes (default
// 64 KiB). A bigger one fails the task with ErrResultTooLarge.
func WithWorkflowResultLimit(n int) WorkflowOption {
	return func(c *workflowConfig) error {
		if n <= 0 {
			return fmt.Errorf("sqliteq: workflow result limit %d must be above 0", n)
		}
		c.resultLimit = n
		return nil
	}
}

// WithWorkflowRetention sets how long an ended workflow is kept (default 7
// days). A running one is kept until it ends.
func WithWorkflowRetention(d time.Duration) WorkflowOption {
	return func(c *workflowConfig) error {
		if d < time.Millisecond {
			return fmt.Errorf("sqliteq: workflow retention %v must be at least 1ms", d)
		}
		c.retention = d
		return nil
	}
}

// WorkflowStore is the workflow store of one Queues root. NewWorkflowStore
// registers it for its scope, so varmq finds it from the queues' ScopeID.
type WorkflowStore struct {
	qs     *queues
	f      *file
	single bool
	scope  string
	cfg    workflowConfig
	clock  atomic.Pointer[func() time.Time] // tests move it

	stop      chan struct{}
	done      chan struct{}
	warned    sync.Map     // keys logged once
	sweptAt   atomic.Int64 // Unix ms of the last sweep Submit started
	closeOnce sync.Once
	closeErr  error
}

// NewWorkflowStore opens the workflow store of qs and registers it with
// contract.RegisterWorkflowStore, so every queue of the same root can run
// workflow tasks; a process that only runs workers needs this one call. In
// folder layout it uses its own file, __sqliteq_workflows.db in the folder;
// under WithSingleFile, tables of the store's file. A second store for the
// same root is contract.ErrScopeRegistered. Close it before qs.
func NewWorkflowStore(qs Queues, opts ...WorkflowOption) (*WorkflowStore, error) {
	q, ok := qs.(*queues)
	if !ok {
		return nil, fmt.Errorf("sqliteq: workflow store needs the Queues made by New, got %T", qs)
	}
	cfg := workflowConfig{resultLimit: defaultResultLimit, retention: defaultRetention}
	for _, opt := range opts {
		if opt == nil {
			return nil, errNilOption
		}
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	path := q.root
	if !q.cfg.singleFile {
		var err error
		if path, err = resolve(filepath.Join(q.root, workflowsFile)); err != nil {
			return nil, err
		}
	}

	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return nil, errors.New("sqliteq: queues are closed")
	}
	f, err := acquire(path, q.root, q.cfg.checkpointPages)
	if err != nil {
		return nil, err
	}

	s := &WorkflowStore{qs: q, f: f, single: q.cfg.singleFile, scope: "sqliteq:" + q.root, cfg: cfg,
		stop: make(chan struct{}), done: make(chan struct{})}
	if err := s.init(); err != nil {
		return nil, errors.Join(err, f.release())
	}
	if err := contract.RegisterWorkflowStore(s); err != nil {
		return nil, errors.Join(err, f.release())
	}

	s.recoverOutbox(0) // leftovers of an earlier run
	go s.loop()
	return s, nil
}

// init creates the tables under the write mutex, and adds the task time
// columns to a tasks table made before them.
func (s *WorkflowStore) init() error {
	s.f.mu.Lock()
	defer s.f.mu.Unlock()

	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS ` + wfWorkflows + ` (
			id TEXT PRIMARY KEY,
			hash TEXT NOT NULL,
			state TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			finished_at INTEGER,
			fail_fast INTEGER NOT NULL,
			undo_on_cancel INTEGER NOT NULL,
			failed INTEGER NOT NULL DEFAULT 0,
			cancelled INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS "__sqliteq_wf_workflows_created" ON ` + wfWorkflows + ` (created_at DESC, id)`,
		`CREATE INDEX IF NOT EXISTS "__sqliteq_wf_workflows_finished" ON ` + wfWorkflows + ` (finished_at) WHERE finished_at IS NOT NULL`,
		// state filter of the listing in order, and the counts
		`CREATE INDEX IF NOT EXISTS "__sqliteq_wf_workflows_state" ON ` + wfWorkflows + ` (state, created_at DESC, id)`,
		`CREATE TABLE IF NOT EXISTS ` + wfTasks + ` (
			workflow_id TEXT NOT NULL,
			name TEXT NOT NULL,
			ord INTEGER NOT NULL,
			state TEXT NOT NULL,
			reason TEXT NOT NULL DEFAULT '',
			result BLOB,
			open INTEGER NOT NULL DEFAULT 0,
			deps TEXT NOT NULL,
			always INTEGER NOT NULL,
			queue_id TEXT NOT NULL,
			job_id TEXT NOT NULL,
			priority INTEGER NOT NULL DEFAULT 0,
			item BLOB,
			has_undo INTEGER NOT NULL DEFAULT 0,
			undo_queue_id TEXT NOT NULL DEFAULT '',
			undo_job_id TEXT NOT NULL DEFAULT '',
			undo_item BLOB,
			undo_state TEXT NOT NULL DEFAULT '',
			undo_open INTEGER NOT NULL DEFAULT 0,
			undo_reason TEXT NOT NULL DEFAULT '',
			enqueued_at INTEGER NOT NULL DEFAULT 0,
			started_at INTEGER NOT NULL DEFAULT 0,
			finished_at INTEGER NOT NULL DEFAULT 0,
			undo_enqueued_at INTEGER NOT NULL DEFAULT 0,
			undo_started_at INTEGER NOT NULL DEFAULT 0,
			undo_finished_at INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (workflow_id, name)
		) WITHOUT ROWID`,
		`CREATE TABLE IF NOT EXISTS ` + wfOutbox + ` (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			workflow_id TEXT NOT NULL,
			queue_id TEXT NOT NULL,
			job_id TEXT NOT NULL,
			item BLOB NOT NULL,
			priority INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS ` + wfPurged + ` (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			queue_id TEXT NOT NULL,
			workflow_id TEXT NOT NULL,
			task TEXT NOT NULL,
			undo INTEGER NOT NULL,
			created_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS "__sqliteq_wf_outbox_workflow" ON ` + wfOutbox + ` (workflow_id)`,
	} {
		if _, err := s.f.db.Exec(stmt); err != nil {
			return fmt.Errorf("sqliteq: failed to create workflow tables: %w", err)
		}
	}
	for _, col := range taskTimeCols {
		exists, err := hasColumn(s.f.db, strings.Trim(wfTasks, `"`), col)
		if err == nil && !exists {
			_, err = s.f.db.Exec(`ALTER TABLE ` + wfTasks + ` ADD COLUMN ` + col + ` INTEGER NOT NULL DEFAULT 0`)
		}
		if err != nil {
			return fmt.Errorf("sqliteq: failed to add workflow task column %s: %w", col, err)
		}
	}
	return nil
}

func (s *WorkflowStore) now() time.Time {
	if f := s.clock.Load(); f != nil {
		return (*f)()
	}
	return time.Now()
}

// ScopeID implements contract.Scoped.
func (s *WorkflowStore) ScopeID() string { return s.scope }

// Close unregisters the store, stops its loop and releases its file. Queues
// stay open. Calling it again does nothing.
func (s *WorkflowStore) Close() error {
	s.closeOnce.Do(func() {
		close(s.stop)
		<-s.done
		contract.UnregisterWorkflowStore(s)
		s.closeErr = s.f.release()
	})
	return s.closeErr
}

// loop pushes leftover outbox rows and sweeps expired workflows until Close.
func (s *WorkflowStore) loop() {
	defer close(s.done)

	tick := time.NewTicker(recoverEvery)
	defer tick.Stop()
	lastSweep := s.now()
	for {
		select {
		case <-s.stop:
			return
		case <-tick.C:
		}
		s.recoverOutbox(recoverEvery)
		if now := s.now(); now.Sub(lastSweep) >= sweepWorkflowsEvery {
			lastSweep = now
			if err := s.sweep(context.Background()); err != nil {
				log.Printf("sqliteq: sweeping workflows: %v", err)
			}
		}
	}
}

func (s *WorkflowStore) notFound(id string) error {
	return fmt.Errorf("%w: %s", contract.ErrWorkflowNotFound, id)
}

// validWFName: 1 to n characters of [A-Za-z0-9_.-] (spec 1.2).
func validWFName(s string, n int) bool {
	if len(s) == 0 || len(s) > n {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '-') {
			return false
		}
	}
	return true
}

func validWorkflowID(id string) bool { return validWFName(id, 128) && id != "index" }
func validTaskName(n string) bool    { return validWFName(n, 64) }

// wfItemTag is the workflow fields of an item.
type wfItemTag struct {
	wf, task string
	undo     bool
}

// workflowTag reports the workflow fields of item data when they could come
// from a Submit (spec 3, wftag): wf and task valid names, undo absent, null
// or a boolean, and id "wf:<wf>:<task>" (":undo" appended for undo true). A
// key that differs from id, wf, task or undo only in case makes it a plain
// job, as encoding/json would read it as that field.
func workflowTag(data []byte) (wfItemTag, bool) {
	var t wfItemTag
	if len(data) == 0 || data[0] != '{' {
		return t, false
	}
	var env map[string]json.RawMessage
	if json.Unmarshal(data, &env) != nil {
		return t, false
	}
	// encoding/json would read such a key as that field
	for k := range env {
		for _, name := range [...]string{"id", "wf", "task", "undo"} {
			if k != name && strings.EqualFold(k, name) {
				return t, false
			}
		}
	}
	var id string
	if json.Unmarshal(env["wf"], &t.wf) != nil || json.Unmarshal(env["task"], &t.task) != nil ||
		json.Unmarshal(env["id"], &id) != nil {
		return t, false
	}
	if raw, ok := env["undo"]; ok && string(raw) != "null" {
		if json.Unmarshal(raw, &t.undo) != nil {
			return t, false
		}
	}
	if !validWorkflowID(t.wf) || !validTaskName(t.task) {
		return t, false
	}
	want := "wf:" + t.wf + ":" + t.task
	if t.undo {
		want += ":undo"
	}
	return t, id == want
}

// jobID is the job ID a Submit gives the task or undo.
func (t wfItemTag) jobID() string {
	id := "wf:" + t.wf + ":" + t.task
	if t.undo {
		id += ":undo"
	}
	return id
}

// querier is what a *sql.DB and a *sql.Tx share.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// write runs fn in one write transaction under the file's write mutex.
func (s *WorkflowStore) write(ctx context.Context, fn func(tx *sql.Tx) error) error {
	s.f.mu.Lock()
	defer s.f.mu.Unlock()

	tx, err := s.f.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// planQueue is the queue handle of a plan's task or undo, in this scope.
func (s *WorkflowStore) planQueue(name string, q, item any) (*handle, []byte, error) {
	var h *handle
	switch v := q.(type) {
	case *Queue:
		if v != nil {
			h = &handle{q: v}
		}
	case *PriorityQueue:
		if v != nil {
			h = &handle{pq: v}
		}
	}
	if h == nil || h.queue().ScopeID() != s.scope {
		return nil, nil, fmt.Errorf("%w: task %q queue is not in sqliteq scope %s", contract.ErrScopeMismatch, name, s.scope)
	}
	b, ok := item.([]byte)
	if !ok || !json.Valid(b) || len(b) == 0 || b[0] != '{' {
		return nil, nil, fmt.Errorf("varmq: task %q: item is not a JSON envelope", name)
	}
	return h, b, nil
}

// learn records handles a plan names, so a worker that only finishes tasks
// still finds queues made by another Queues of the same root.
func (qs *queues) learn(h *handle) {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	if !qs.closed {
		qs.setHandle(h, false)
	}
}

// lookup finds queue id for any client of the scope in this process: this
// client's table first, then the file registry; a hit is learned. Call it
// outside the file's write mutex: the registry lock comes before it.
func (s *WorkflowStore) lookup(id string) (*handle, bool) {
	if h, ok := s.qs.handleOf(id); ok {
		return h, true
	}
	var found *handle
	registry.mu.Lock()
	for _, f := range registry.files {
		if f.scope != s.scope {
			continue
		}
		for _, h := range f.handles {
			if h.queue().id == id {
				found = h
			}
		}
	}
	registry.mu.Unlock()
	if found == nil {
		return nil, false
	}
	s.qs.learn(found)
	return found, true
}

// openIDs learns every queue of the scope open in this process and returns
// their IDs.
func (s *WorkflowStore) openIDs() []string {
	var hs []*handle
	registry.mu.Lock()
	for _, f := range registry.files {
		if f.scope == s.scope {
			for _, h := range f.handles {
				hs = append(hs, h)
			}
		}
	}
	registry.mu.Unlock()
	for _, h := range hs {
		s.qs.learn(h)
	}
	m := *s.qs.byID.Load()
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	return ids
}

// preResolve looks up the queues of workflow id before a write, so the
// transaction (which holds the file mutex) only reads the lock-free table.
func (s *WorkflowStore) preResolve(ctx context.Context, id string) {
	rows, err := s.f.db.QueryContext(ctx, `SELECT queue_id FROM `+wfTasks+` WHERE workflow_id = ?
		UNION SELECT undo_queue_id FROM `+wfTasks+` WHERE workflow_id = ? AND has_undo = 1`, id, id)
	if err != nil {
		return
	}
	var ids []string
	for rows.Next() {
		var q string
		if rows.Scan(&q) == nil {
			ids = append(ids, q)
		}
	}
	rows.Close()
	for _, q := range ids {
		s.lookup(q)
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Submit stores plan p and pushes its roots. A stored ID is success when its
// plan hash is equal, else ErrWorkflowExists.
func (s *WorkflowStore) Submit(ctx context.Context, p contract.WorkflowPlan) error {
	if len(p.Tasks) == 0 {
		return fmt.Errorf("varmq: workflow %s has no tasks", p.ID)
	}
	if !validWorkflowID(p.ID) {
		return fmt.Errorf("varmq: invalid workflow id %q", p.ID)
	}

	type planned struct {
		tp    contract.TaskPlan
		item  []byte
		uitem []byte
		deps  []byte
		uq    *handle
		qh    *handle
	}
	plan := make([]planned, len(p.Tasks))
	names := make(map[string]bool, len(p.Tasks))
	for _, tp := range p.Tasks {
		names[tp.Name] = true
	}
	for i, tp := range p.Tasks {
		for _, d := range tp.Deps {
			if !names[d] {
				return fmt.Errorf("varmq: task %q depends on %q, which is not in the workflow", tp.Name, d)
			}
		}
		if !validTaskName(tp.Name) {
			return fmt.Errorf("varmq: invalid task name %q", tp.Name)
		}
		h, item, err := s.planQueue(tp.Name, tp.Queue, tp.Item)
		if err != nil {
			return err
		}
		pl := planned{tp: tp, item: item, qh: h}
		if u := tp.Undo; u != nil {
			if pl.uq, pl.uitem, err = s.planQueue(tp.Name+" undo", u.Queue, u.Item); err != nil {
				return err
			}
		}
		deps := tp.Deps
		if deps == nil {
			deps = []string{}
		}
		pl.deps, _ = json.Marshal(deps) // strings: Marshal can't fail
		plan[i] = pl
	}
	for _, pl := range plan {
		s.qs.learn(pl.qh)
		if pl.uq != nil {
			s.qs.learn(pl.uq)
		}
	}

	var minSeq int64
	var exists bool
	err := s.write(ctx, func(tx *sql.Tx) error {
		if err := s.dropExpired(ctx, tx, p.ID); err != nil {
			return err
		}
		var hash string
		switch err := tx.QueryRowContext(ctx, `SELECT hash FROM `+wfWorkflows+` WHERE id = ?`, p.ID).Scan(&hash); {
		case err == nil:
			if hash != p.Hash {
				return fmt.Errorf("%w: %s", contract.ErrWorkflowExists, p.ID)
			}
			exists = true
			return nil
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}

		now := s.now().UnixMilli()
		if _, err := tx.ExecContext(ctx, `INSERT INTO `+wfWorkflows+
			` (id, hash, state, created_at, fail_fast, undo_on_cancel) VALUES (?, ?, 'running', ?, ?, ?)`,
			p.ID, p.Hash, now, btoi(p.FailFast), btoi(p.UndoOnCancel)); err != nil {
			return err
		}

		w := &wfFlow{id: p.ID, state: contract.WorkflowRunning, created: now, failFast: p.FailFast,
			undoOnCancel: p.UndoOnCancel, tasks: make(map[string]*wfTask, len(plan))}
		var roots []wfEffect
		for i, pl := range plan {
			tp := pl.tp
			if _, dup := w.tasks[tp.Name]; dup {
				return fmt.Errorf("varmq: duplicate task name %q", tp.Name)
			}
			t := &wfTask{name: tp.Name, deps: tp.Deps, always: tp.Always, queueID: pl.qh.queue().id, jobID: tp.JobID,
				priority: tp.Priority, state: contract.JobPending, open: len(tp.Deps)}
			if t.open > 0 {
				t.state = contract.JobWaiting
			} else {
				roots = append(roots, wfEffect{push: tp.Name})
			}
			var uq, uj string
			var uitem []byte
			if tp.Undo != nil {
				t.undo = &wfUndo{queueID: pl.uq.queue().id, jobID: tp.Undo.JobID}
				uq, uj, uitem = t.undo.queueID, t.undo.jobID, pl.uitem
			}
			w.tasks[tp.Name] = t
			w.order = append(w.order, tp.Name)
			if _, err := tx.ExecContext(ctx, `INSERT INTO `+wfTasks+
				` (workflow_id, name, ord, state, open, deps, always, queue_id, job_id, priority, item,
				   has_undo, undo_queue_id, undo_job_id, undo_item)
				  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				p.ID, tp.Name, i, string(t.state), t.open, string(pl.deps), btoi(tp.Always), t.queueID, t.jobID,
				tp.Priority, pl.item, btoi(tp.Undo != nil), uq, uj, uitem); err != nil {
				return err
			}
		}
		w.index()

		var err error
		minSeq, _, err = s.release(ctx, tx, w, roots)
		return err
	})
	if err != nil {
		return err
	}
	if !exists {
		if minSeq != 0 {
			s.drain(ctx, `workflow_id = ? AND seq >= ?`, p.ID, minSeq)
		}
		if last := s.sweptAt.Load(); s.now().UnixMilli()-last >= sweepOnSubmit.Milliseconds() && s.sweptAt.CompareAndSwap(last, s.now().UnixMilli()) {
			if err := s.sweep(ctx); err != nil {
				log.Printf("sqliteq: sweeping workflows: %v", err)
			}
		}
	}
	return nil
}

// outRow is one push: an item bound for a queue under its job ID.
type outRow struct {
	queueID, jobID string
	item           []byte
	priority       int
}

// stamp sets the finish time when fx ends the workflow; call it before save.
func (s *WorkflowStore) stamp(w *wfFlow, fx []wfEffect) {
	for _, e := range fx {
		if e.end != "" {
			w.finished = max(s.now().UnixMilli(), w.created) // never before the start
		}
	}
}

// release turns the pushes of fx into rows (reading and clearing each task's
// stored item, stamping its enqueue time) and delivers them in tx.
// Folder layout: outbox rows, minSeq the first one's seq (0 when none).
// Single file: straight into the queues (ids), except those this client
// cannot reach, which get outbox rows.
func (s *WorkflowStore) release(ctx context.Context, tx *sql.Tx, w *wfFlow, fx []wfEffect) (minSeq int64, ids []string, err error) {
	var rows []outRow
	now := s.now().UnixMilli()
	for _, e := range fx {
		if e.push == "" {
			continue
		}
		t := w.tasks[e.push]
		col, set, row := "item", "item = NULL, enqueued_at = ?", outRow{queueID: t.queueID, jobID: t.jobID, priority: t.priority}
		if e.undo {
			col, set, row = "undo_item", "undo_enqueued_at = ?", outRow{queueID: t.undo.queueID, jobID: t.undo.jobID}
		}
		where := ` WHERE workflow_id = ? AND name = ?`
		if err = tx.QueryRowContext(ctx, `SELECT `+col+` FROM `+wfTasks+where, w.id, e.push).Scan(&row.item); err != nil {
			return 0, nil, err
		}
		if row.item == nil {
			continue
		}
		if _, err = tx.ExecContext(ctx, `UPDATE `+wfTasks+` SET `+set+where, now, w.id, e.push); err != nil {
			return 0, nil, err
		}
		rows = append(rows, row)
	}

	for _, r := range rows {
		// A queue this client cannot reach (opened later, or by another
		// client) gets an outbox row, in either layout: the push waits
		if h, ok := s.qs.handleOf(r.queueID); ok && s.single {
			switch err = h.queue().enqueueTx(tx, r.item, r.priority, r.jobID); {
			case err == nil:
				if !slices.Contains(ids, r.queueID) {
					ids = append(ids, r.queueID)
				}
				continue
			case errors.Is(err, errJobIDTaken):
				s.warnOnce(r.jobID, "job id is held by another job; the push stays in the outbox")
			default:
				return 0, nil, err
			}
		}
		res, err := tx.ExecContext(ctx, `INSERT INTO `+wfOutbox+
			` (workflow_id, queue_id, job_id, item, priority, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
			w.id, r.queueID, r.jobID, r.item, r.priority, now)
		if err != nil {
			return 0, nil, err
		}
		if minSeq == 0 {
			if minSeq, err = res.LastInsertId(); err != nil {
				return 0, nil, err
			}
		}
	}
	return minSeq, ids, nil
}

// drain pushes the outbox rows matching where into their queues and deletes
// each pushed one. Only rows of queues open in this process are read; a
// closed queue keeps its rows for the next recovery. It returns the queue IDs pushed into.
func (s *WorkflowStore) drain(ctx context.Context, where string, args ...any) []string {
	open := s.openIDs()
	if len(open) == 0 {
		return nil
	}
	in := strings.TrimSuffix(strings.Repeat("?,", len(open)), ",")
	var ids []string
	after, total := int64(0), 0
	for {
		qargs := []any{after}
		qargs = append(qargs, args...)
		for _, id := range open {
			qargs = append(qargs, id)
		}
		rows, err := s.f.db.QueryContext(ctx, `SELECT seq, queue_id, job_id, item, priority FROM `+wfOutbox+
			` WHERE seq > ? AND (`+where+`) AND queue_id IN (`+in+`) ORDER BY seq LIMIT `+fmt.Sprint(drainBatch), qargs...)
		if err != nil {
			log.Printf("sqliteq: reading workflow outbox: %v", err)
			return ids
		}
		type row struct {
			seq int64
			outRow
		}
		var batch []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.seq, &r.queueID, &r.jobID, &r.item, &r.priority); err != nil {
				rows.Close()
				return ids
			}
			batch = append(batch, r)
		}
		err = errors.Join(rows.Err(), rows.Close())
		if err != nil || len(batch) == 0 {
			return ids
		}

		total += len(batch)
		for _, r := range batch {
			after = r.seq
			h, ok := s.lookup(r.queueID)
			if !ok {
				continue
			}
			if err := pushRow(h, r.outRow); err != nil {
				if errors.Is(err, errJobIDTaken) {
					s.warnOnce(r.jobID, "job id is held by another job; the row stays")
				}
				continue
			}
			s.f.mu.Lock()
			_, err := s.f.db.ExecContext(ctx, `DELETE FROM `+wfOutbox+` WHERE seq = ?`, r.seq)
			s.f.mu.Unlock()
			if err != nil {
				log.Printf("sqliteq: deleting workflow outbox row: %v", err)
				continue
			}
			if !slices.Contains(ids, r.queueID) {
				ids = append(ids, r.queueID)
			}
		}
		if len(batch) < drainBatch || total >= drainCap {
			return ids
		}
	}
}

// pushRow adds the row's item to its queue once per job ID.
func pushRow(h *handle, r outRow) error {
	return h.queue().pushWorkflow(r.item, r.priority, r.jobID)
}

// warnOnce logs msg for key once per store.
func (s *WorkflowStore) warnOnce(key, msg string) {
	if _, seen := s.warned.LoadOrStore(key, struct{}{}); !seen {
		log.Printf("sqliteq: workflow outbox %s: %s", key, msg)
	}
}

// recoverOutbox pushes outbox rows at least minAge old.
func (s *WorkflowStore) recoverOutbox(minAge time.Duration) {
	s.logUnreachable()
	s.applyIntents()
	s.drain(context.Background(), `created_at <= ?`, s.now().Add(-minAge).UnixMilli())
}

// dropExpired deletes workflow id when it ended more than the retention ago
// and no sweep has run yet, so a Submit of the ID starts a new workflow.
func (s *WorkflowStore) dropExpired(ctx context.Context, tx *sql.Tx, id string) error {
	cut := s.now().Add(-s.cfg.retention).UnixMilli()
	for _, t := range []string{wfTasks, wfOutbox} {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+t+` WHERE workflow_id = ? AND EXISTS
			(SELECT 1 FROM `+wfWorkflows+` WHERE id = ? AND finished_at IS NOT NULL AND finished_at <= ?)`, id, id, cut); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM `+wfWorkflows+` WHERE id = ? AND finished_at IS NOT NULL AND finished_at <= ?`, id, cut)
	return err
}

// logUnreachable names, once each, the queues of outbox rows that no client in
// this process has open.
func (s *WorkflowStore) logUnreachable() {
	open := map[string]bool{}
	for _, id := range s.openIDs() {
		open[id] = true
	}
	rows, err := s.f.db.Query(`SELECT DISTINCT queue_id FROM ` + wfOutbox)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil && !open[id] {
			s.warnOnce(id, "queue is not open in this process; its rows wait")
		}
	}
}

// sweep drops workflows ended more than the retention ago, with their tasks
// and outbox rows.
func (s *WorkflowStore) sweep(ctx context.Context) error {
	cut := s.now().Add(-s.cfg.retention).UnixMilli()
	return s.write(ctx, func(tx *sql.Tx) error {
		gone := `workflow_id IN (SELECT id FROM ` + wfWorkflows + ` WHERE finished_at IS NOT NULL AND finished_at <= ?)`
		for _, t := range []string{wfTasks, wfOutbox} {
			if _, err := tx.ExecContext(ctx, `DELETE FROM `+t+` WHERE `+gone, cut); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM `+wfWorkflows+` WHERE finished_at IS NOT NULL AND finished_at <= ?`, cut)
		return err
	})
}

// load reads workflow id with its tasks (no items, no results).
func (s *WorkflowStore) load(ctx context.Context, q querier, id string) (*wfFlow, error) {
	w := &wfFlow{id: id}
	var (
		finished       sql.NullInt64
		ff, uoc, f, cn int
	)
	switch err := q.QueryRowContext(ctx, `SELECT state, created_at, finished_at, fail_fast, undo_on_cancel, failed, cancelled FROM `+
		wfWorkflows+` WHERE id = ?`, id).Scan(&w.state, &w.created, &finished, &ff, &uoc, &f, &cn); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, s.notFound(id)
	case err != nil:
		return nil, err
	}
	w.finished = finished.Int64
	w.failFast, w.undoOnCancel, w.failed, w.cancelled = ff == 1, uoc == 1, f == 1, cn == 1

	rows, err := q.QueryContext(ctx, `SELECT name, state, reason, open, deps, always, queue_id, job_id, priority, COALESCE(length(result), 0),
		has_undo, undo_queue_id, undo_job_id, undo_state, undo_open, undo_reason,
		enqueued_at, started_at, finished_at, undo_enqueued_at, undo_started_at, undo_finished_at
		FROM `+wfTasks+` WHERE workflow_id = ? ORDER BY ord`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	w.tasks = map[string]*wfTask{}
	for rows.Next() {
		var (
			t       wfTask
			deps    string
			always  int
			hasUndo int
			u       wfUndo
		)
		if err := rows.Scan(&t.name, &t.state, &t.reason, &t.open, &deps, &always, &t.queueID, &t.jobID, &t.priority, &t.resultSize,
			&hasUndo, &u.queueID, &u.jobID, &t.undoState, &t.undoOpen, &t.undoReason,
			&t.enqueued, &t.started, &t.finished, &t.undoEnqueued, &t.undoStarted, &t.undoFinished); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(deps), &t.deps); err != nil {
			return nil, fmt.Errorf("sqliteq: workflow %s task %s: bad deps: %w", id, t.name, err)
		}
		if len(t.deps) == 0 {
			t.deps = nil
		}
		t.always = always == 1
		if hasUndo == 1 {
			t.undo = &u
		}
		w.tasks[t.name] = &t
		w.order = append(w.order, t.name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	w.index()
	return w, nil
}

// mut is the part of a task (and of the workflow) the machine changes.
type mut struct {
	state, undoState              contract.JobState
	open, undoOpen                int
	reason, undoReason            string
	wfState                       contract.WorkflowState
	failed, cancelled, undoCancel bool
}

func (w *wfFlow) snapshot() map[string]mut {
	m := make(map[string]mut, len(w.tasks)+1)
	for name, t := range w.tasks {
		m[name] = mut{state: t.state, undoState: t.undoState, open: t.open, undoOpen: t.undoOpen, reason: t.reason, undoReason: t.undoReason}
	}
	m[""] = mut{wfState: w.state, failed: w.failed, cancelled: w.cancelled, undoCancel: w.undoOnCancel}
	return m
}

// dirty lists the tasks the machine changed since snapshot orig, and whether
// the workflow row changed.
func (w *wfFlow) dirty(orig map[string]mut) (tasks []string, flow bool) {
	for _, name := range w.order {
		t := w.tasks[name]
		if orig[name] != (mut{state: t.state, undoState: t.undoState, open: t.open, undoOpen: t.undoOpen, reason: t.reason, undoReason: t.undoReason}) {
			tasks = append(tasks, name)
		}
	}
	return tasks, orig[""] != mut{wfState: w.state, failed: w.failed, cancelled: w.cancelled, undoCancel: w.undoOnCancel}
}

// save writes what changed since orig, with the finish times the machine
// stamped (an end always changes the state); result is the finishing task's
// result, written when that task is task.
func (s *WorkflowStore) save(ctx context.Context, tx *sql.Tx, w *wfFlow, orig map[string]mut, task string, result []byte) error {
	tasks, flow := w.dirty(orig)
	if flow {
		var fin any
		if w.finished > 0 {
			fin = w.finished
		}
		if _, err := tx.ExecContext(ctx, `UPDATE `+wfWorkflows+` SET state = ?, finished_at = ?, undo_on_cancel = ?, failed = ?, cancelled = ? WHERE id = ?`,
			string(w.state), fin, btoi(w.undoOnCancel), btoi(w.failed), btoi(w.cancelled), w.id); err != nil {
			return err
		}
	}
	for _, name := range tasks {
		t := w.tasks[name]
		if _, err := tx.ExecContext(ctx, `UPDATE `+wfTasks+` SET state = ?, open = ?, reason = ?, finished_at = ?,
			undo_state = ?, undo_open = ?, undo_reason = ?, undo_finished_at = ? WHERE workflow_id = ? AND name = ?`,
			string(t.state), t.open, t.reason, t.finished, string(t.undoState), t.undoOpen, t.undoReason, t.undoFinished, w.id, name); err != nil {
			return err
		}
		if name == task && len(result) > 0 && t.state == contract.JobCompleted {
			if _, err := tx.ExecContext(ctx, `UPDATE `+wfTasks+` SET result = ? WHERE workflow_id = ? AND name = ?`, result, w.id, name); err != nil {
				return err
			}
		}
	}
	return nil
}

// targets are the queue IDs a workflow can push into: every task's and undo's,
// once each, in task order.
func (w *wfFlow) targets() []string {
	var ids []string
	add := func(id string) {
		if !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	for _, name := range w.order {
		t := w.tasks[name]
		add(t.queueID)
		if t.undo != nil {
			add(t.undo.queueID)
		}
	}
	return ids
}

// Cancel cancels workflow id: waiting tasks now, queued ones at their
// pre-run check. undo nil keeps the workflow's undo_on_cancel. Cancelling an
// ended or cancelled workflow does nothing.
func (s *WorkflowStore) Cancel(ctx context.Context, id string, undo *bool) error {
	if !validWorkflowID(id) {
		return s.notFound(id)
	}
	s.preResolve(ctx, id)
	var minSeq int64
	err := s.write(ctx, func(tx *sql.Tx) error {
		w, err := s.load(ctx, tx, id)
		if err != nil {
			return err
		}
		on := w.undoOnCancel
		if undo != nil {
			on = *undo
		}
		orig := w.snapshot()
		w.at = s.now().UnixMilli()
		fx := w.cancel(on)
		s.stamp(w, fx)
		if err := s.save(ctx, tx, w, orig, "", nil); err != nil {
			return err
		}
		minSeq, _, err = s.release(ctx, tx, w, fx)
		return err
	})
	if err != nil {
		return err
	}
	if minSeq != 0 {
		s.drain(ctx, `workflow_id = ? AND seq >= ?`, id, minSeq)
	}
	return nil
}

// Runnable is the pre-run check (spec 6.12). A yes stamps the start time of
// the task (undo: of its undo) in the same transaction: the job runs next.
func (s *WorkflowStore) Runnable(ctx context.Context, workflowID, task string, undo bool) (bool, error) {
	if !validWorkflowID(workflowID) {
		return false, s.notFound(workflowID)
	}
	var ok bool
	err := s.write(ctx, func(tx *sql.Tx) error {
		var (
			state                      contract.WorkflowState
			ff, failed, cancelled      int
			tstate, ustate             contract.JobState
			always, hasUndo, taskFound int
		)
		err := tx.QueryRowContext(ctx, `SELECT w.state, w.fail_fast, w.failed, w.cancelled,
			COALESCE(t.state, ''), COALESCE(t.undo_state, ''), COALESCE(t.always, 0), COALESCE(t.has_undo, 0), t.name IS NOT NULL
			FROM `+wfWorkflows+` w LEFT JOIN `+wfTasks+` t ON t.workflow_id = w.id AND t.name = ? WHERE w.id = ?`, task, workflowID).
			Scan(&state, &ff, &failed, &cancelled, &tstate, &ustate, &always, &hasUndo, &taskFound)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return s.notFound(workflowID)
		case err != nil:
			return err
		case taskFound == 0:
			return nil
		}
		col := "started_at"
		if undo {
			col = "undo_started_at"
			ok = state == contract.WorkflowCompensating && hasUndo == 1 && ustate == contract.JobPending
		} else {
			stopped := ff == 1 && failed == 1 && always == 0
			ok = state == contract.WorkflowRunning && cancelled == 0 && !stopped && tstate == contract.JobPending
		}
		if !ok {
			return nil
		}
		_, err = tx.ExecContext(ctx, `UPDATE `+wfTasks+` SET `+col+` = ? WHERE workflow_id = ? AND name = ?`,
			s.now().UnixMilli(), workflowID, task)
		return err
	})
	return ok && err == nil, err
}

// DepResult returns task's stored result: ErrDepFailed when it failed or was
// cancelled, ErrNoResult when it left none, has not ended or is not a task.
func (s *WorkflowStore) DepResult(ctx context.Context, workflowID, task string) ([]byte, error) {
	if !validWorkflowID(workflowID) {
		return nil, s.notFound(workflowID)
	}
	var (
		state  sql.NullString
		result []byte
	)
	err := s.f.db.QueryRowContext(ctx, `SELECT t.state, t.result FROM `+wfWorkflows+` w
		LEFT JOIN `+wfTasks+` t ON t.workflow_id = w.id AND t.name = ? WHERE w.id = ?`, task, workflowID).Scan(&state, &result)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, s.notFound(workflowID)
	case err != nil:
		return nil, err
	}
	switch contract.JobState(state.String) {
	case contract.JobFailed, contract.JobCancelled:
		return nil, contract.ErrDepFailed
	case contract.JobCompleted:
		if len(result) > 0 {
			return result, nil
		}
	}
	return nil, contract.ErrNoResult
}

// Record returns workflow id as stored.
func (s *WorkflowStore) Record(ctx context.Context, id string) (contract.WorkflowRecord, error) {
	if !validWorkflowID(id) {
		return contract.WorkflowRecord{}, s.notFound(id)
	}
	w, err := s.load(ctx, s.f.db, id)
	if err != nil {
		return contract.WorkflowRecord{}, err
	}
	return w.record(), nil
}

// record builds the WorkflowRecord: tasks in Add order. An undo is waiting
// until compensation reaches it.
func (w *wfFlow) record() contract.WorkflowRecord {
	r := contract.WorkflowRecord{ID: w.id, State: w.state, CreatedAt: w.created, FinishedAt: w.finished,
		UndoOnCancel: w.undoOnCancel, Tasks: make([]contract.TaskRecord, 0, len(w.order))}
	for _, name := range w.order {
		t := w.tasks[name]
		tr := contract.TaskRecord{Name: name, State: t.state, Deps: slices.Clone(t.deps), QueueID: t.queueID,
			JobID: t.jobID, Always: t.always, Reason: t.reason, ResultSize: t.resultSize,
			EnqueuedAt: t.enqueued, StartedAt: t.started, FinishedAt: t.finished}
		if t.undo != nil {
			st := t.undoState
			if st == "" {
				st = contract.JobWaiting
			}
			tr.Undo = &contract.TaskRecord{Name: name, State: st, QueueID: t.undo.queueID, JobID: t.undo.jobID, Reason: t.undoReason,
				EnqueuedAt: t.undoEnqueued, StartedAt: t.undoStarted, FinishedAt: t.undoFinished}
		}
		r.Tasks = append(r.Tasks, tr)
	}
	return r
}

// Workflows lists up to limit workflows in state ("" any), newest first then
// by ID, skipping offset.
func (s *WorkflowStore) Workflows(ctx context.Context, state contract.WorkflowState, offset, limit int) ([]contract.WorkflowRecord, error) {
	keys, err := s.WorkflowKeys(ctx, state, offset, limit)
	if err != nil {
		return nil, err
	}
	out := make([]contract.WorkflowRecord, 0, len(keys))
	for _, k := range keys {
		r, err := s.Record(ctx, k.ID)
		if errors.Is(err, contract.ErrWorkflowNotFound) {
			continue // swept since the listing
		}
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// WorkflowKeys is Workflows without the records.
func (s *WorkflowStore) WorkflowKeys(ctx context.Context, state contract.WorkflowState, offset, limit int) ([]contract.WorkflowKey, error) {
	offset, limit = max(offset, 0), min(max(limit, 0), 1<<31-1)
	if limit == 0 {
		return []contract.WorkflowKey{}, nil
	}
	where, args := "", []any{}
	if state != "" {
		where, args = ` WHERE state = ?`, append(args, string(state))
	}
	rows, err := s.f.db.QueryContext(ctx, `SELECT id, created_at FROM `+wfWorkflows+where+
		` ORDER BY created_at DESC, id LIMIT ? OFFSET ?`, append(args, limit, offset)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []contract.WorkflowKey{}
	for rows.Next() {
		var k contract.WorkflowKey
		if err := rows.Scan(&k.ID, &k.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// WorkflowCounts counts the stored workflows per state: the rows Workflows
// lists. A state with none is absent.
// ponytail: scans the state index; a counter row per state if stores get huge.
func (s *WorkflowStore) WorkflowCounts(ctx context.Context) (map[contract.WorkflowState]int64, error) {
	rows, err := s.f.db.QueryContext(ctx, `SELECT state, COUNT(*) FROM `+wfWorkflows+` GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[contract.WorkflowState]int64{}
	for rows.Next() {
		var (
			state contract.WorkflowState
			n     int64
		)
		if err := rows.Scan(&state, &n); err != nil {
			return nil, err
		}
		out[state] = n
	}
	return out, rows.Err()
}

// setClock moves the store's clock; tests only.
func (s *WorkflowStore) setClock(f func() time.Time) { s.clock.Store(&f) }
