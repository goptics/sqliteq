package sqliteq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/goptics/varmq/contract"
)

// endResult is what one task end did to its workflow.
type endResult struct {
	gone     bool     // no such workflow (expired): the item settles alone
	skip     bool     // the end does not name this queue's task: the item settles alone
	refinish bool     // the task had ended already: nothing changed
	targets  []string // refinish: every push-target queue of the workflow
	minSeq   int64    // folder layout: first outbox row written
	pushed   []string // single file: queues pushed into
}

// FinishTask settles the job (queueID, ackID) and applies the task end to
// its workflow. Folder layout: one transaction in the workflow file records
// the end and writes what it releases to the outbox; then each row is pushed
// into its queue and deleted; then the job is acknowledged. A crash between
// the steps leaves rows the recovery loop pushes, or a job that is
// redelivered and skipped by the pre-run check (Runnable). Single file: all
// three steps are one transaction. It returns the queue IDs it pushed into,
// or, for a task that had ended already, every push-target queue of the
// workflow. A gone workflow, or an end that does not name this queue's task,
// settles the job and returns ok with no IDs. A completed result over the
// limit fails the task (ErrResultTooLarge); one that is not JSON is dropped.
func (s *WorkflowStore) FinishTask(queueID, ackID string, e contract.TaskEnd) ([]string, bool) {
	h, ok := s.lookup(queueID)
	if !ok {
		log.Printf("sqliteq: finish task %s/%s: queue %s is not open in this client", e.Workflow, e.Task, queueID)
		return nil, false
	}
	q := h.queue()

	state, reason, result := contract.JobCompleted, e.Reason, e.Result
	switch e.Outcome {
	case contract.OutcomeCompleted:
		reason = ""
		switch {
		case e.Undo:
			result = nil
		case len(result) > s.cfg.resultLimit:
			e.Outcome, state, reason, result = contract.OutcomeFailed, contract.JobFailed, contract.ErrResultTooLarge.Error(), nil
		case !json.Valid(result):
			result = nil
		}
	case contract.OutcomeFailed:
		state, result = contract.JobFailed, nil
	case contract.OutcomeCancelled:
		state, result = contract.JobCancelled, nil
	default:
		log.Printf("sqliteq: task %s/%s: unknown outcome %q", e.Workflow, e.Task, e.Outcome)
		return nil, false
	}
	e.Reason, e.Result = reason, result

	if !validWorkflowID(e.Workflow) || !validTaskName(e.Task) {
		_, err := q.finishDirect(ackID, state, reason, result)
		return nil, err == nil
	}

	ctx := context.Background()
	s.preResolve(ctx, e.Workflow)
	var r endResult
	var acked bool
	err := s.write(ctx, func(tx *sql.Tx) (err error) {
		if r, err = s.endTx(ctx, tx, queueID, e); err != nil {
			return err
		}
		if !s.single {
			return nil
		}
		// One file: the end, its pushes and the ack commit together
		id, ok := parseAckID(ackID)
		if !ok {
			return nil
		}
		acked = true
		_, err = q.finishTx(tx, id, state, reason, result, time.Now())
		return err
	})
	if err != nil {
		log.Printf("sqliteq: finishing workflow %s task %s: %v", e.Workflow, e.Task, err)
		return nil, false
	}

	pushed := r.pushed
	if r.minSeq != 0 {
		for _, id := range s.drain(ctx, `workflow_id = ? AND seq >= ?`, e.Workflow, r.minSeq) {
			if !slices.Contains(pushed, id) {
				pushed = append(pushed, id)
			}
		}
	}
	if !acked {
		if _, err := q.finishDirect(ackID, state, reason, result); err != nil {
			log.Printf("sqliteq: finishing workflow %s task %s: ack: %v", e.Workflow, e.Task, err)
			return nil, false
		}
	}
	if r.refinish {
		pushed = r.targets
	}
	return pushed, true
}

// endTx applies end e of the task in queue queueID to its workflow, in tx:
// the state machine, the changed rows, and the release of what it makes ready.
func (s *WorkflowStore) endTx(ctx context.Context, tx *sql.Tx, queueID string, e contract.TaskEnd) (endResult, error) {
	var r endResult
	w, err := s.load(ctx, tx, e.Workflow)
	if errors.Is(err, contract.ErrWorkflowNotFound) {
		r.gone = true
		return r, nil
	}
	if err != nil {
		return r, err
	}

	t := w.tasks[e.Task]
	if t == nil || (e.Undo && (t.undo == nil || t.undo.queueID != queueID)) || (!e.Undo && t.queueID != queueID) {
		r.skip = true
		return r, nil
	}

	orig := w.snapshot()
	w.at = s.now().UnixMilli()
	fx, err := w.finish(e)
	if err != nil {
		return r, err
	}
	if tasks, flow := w.dirty(orig); len(tasks) == 0 && !flow && len(fx) == 0 {
		r.refinish, r.targets = true, w.targets()
		return r, nil
	}
	s.stamp(w, fx)
	if err := s.save(ctx, tx, w, orig, e.Task, e.Result); err != nil {
		return r, err
	}
	r.minSeq, r.pushed, err = s.release(ctx, tx, w, fx)
	return r, err
}

// purgeIntent is a purged workflow item whose end is still to be applied.
type purgeIntent struct {
	seq      int64 // 0: not stored
	queueID  string
	tag      wfItemTag
	storedAt int64
}

// recordPurge stores an intent per tag in the workflow file before the
// Purge deletes the rows, so a crash after the delete loses no end: the
// recovery loop applies leftovers.
func (s *WorkflowStore) recordPurge(ctx context.Context, q *Queue, tags []wfItemTag) []purgeIntent {
	var out []purgeIntent
	for _, tag := range tags {
		s.f.mu.Lock()
		res, err := s.f.db.ExecContext(ctx, `INSERT INTO `+wfPurged+` (queue_id, workflow_id, task, undo, created_at) VALUES (?, ?, ?, ?, ?)`,
			q.id, tag.wf, tag.task, btoi(tag.undo), s.now().UnixMilli())
		s.f.mu.Unlock()
		if err != nil {
			log.Printf("sqliteq: purge of %s: recording workflow %s task %s: %v", q.name, tag.wf, tag.task, err)
			continue
		}
		seq, _ := res.LastInsertId()
		out = append(out, purgeIntent{seq: seq, queueID: q.id, tag: tag})
	}
	return out
}

// endPurged applies the ends of items a Purge of queue q removed: each
// intent, then the tags the delete's own read found that no intent covers.
func (s *WorkflowStore) endPurged(q *Queue, intents []purgeIntent, tags []wfItemTag) {
	seen := map[wfItemTag]bool{}
	for _, in := range intents {
		seen[in.tag] = true
	}
	for _, tag := range tags {
		if !seen[tag] {
			intents = append(intents, purgeIntent{queueID: q.id, tag: tag})
		}
	}
	for _, in := range intents {
		s.applyIntent(context.Background(), in)
	}
}

// applyIntent ends the purged task as cancelled (reason "queue purged") and
// deletes the intent, in one transaction. A task not in the queue, or one
// that had ended, changes nothing.
func (s *WorkflowStore) applyIntent(ctx context.Context, in purgeIntent) {
	s.preResolve(ctx, in.tag.wf)
	e := contract.TaskEnd{Workflow: in.tag.wf, Task: in.tag.task, Undo: in.tag.undo, Outcome: contract.OutcomeCancelled, Reason: purgeReason}
	var r endResult
	err := s.write(ctx, func(tx *sql.Tx) (err error) {
		if r, err = s.endTx(ctx, tx, in.queueID, e); err != nil || in.seq == 0 {
			return err
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM `+wfPurged+` WHERE seq = ?`, in.seq)
		return err
	})
	if err != nil {
		log.Printf("sqliteq: purge: ending workflow %s task %s: %v", in.tag.wf, in.tag.task, err)
		return
	}
	if r.minSeq != 0 {
		s.drain(ctx, `workflow_id = ? AND seq >= ?`, in.tag.wf, r.minSeq)
	}
}

// applyIntents applies the stored intents whose job row is gone from its
// queue: the purge's delete has committed. While the row is still there the
// delete has not committed yet, or the purge aborted and the job lives on;
// the intent waits. Once that job ends normally, its task has ended, and
// applying the intent changes nothing but deleting it.
func (s *WorkflowStore) applyIntents() {
	rows, err := s.f.db.Query(`SELECT seq, queue_id, workflow_id, task, undo FROM `+wfPurged+` ORDER BY seq LIMIT ?`, drainBatch)
	if err != nil {
		return
	}
	var list []purgeIntent
	for rows.Next() {
		var in purgeIntent
		var undo int
		if rows.Scan(&in.seq, &in.queueID, &in.tag.wf, &in.tag.task, &undo) == nil {
			in.tag.undo = undo == 1
			list = append(list, in)
		}
	}
	rows.Close()
	for _, in := range list {
		h, ok := s.lookup(in.queueID)
		if !ok {
			continue
		}
		q := h.queue()
		var n int
		err := q.f.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE job_id = ? AND status IN ('pending', 'processing')", q.table),
			in.tag.jobID()).Scan(&n)
		if err != nil || n > 0 {
			continue
		}
		s.applyIntent(context.Background(), in)
	}
}

// purgeStore returns the store whose workflows a Purge of the file must end:
// the registered one, else one opened on demand when the file's workflow
// tables exist (nil when they do not). done releases an on-demand store.
// err: tables may exist but no store is ready (check, open or init failed);
// the Purge must keep the jobs, or their workflows stay running for ever.
func (f *file) purgeStore() (ws *WorkflowStore, done func(), err error) {
	none := func() {}
	if st, ok := contract.LookupWorkflowStore(f.scope); ok {
		if ws, ok := st.(*WorkflowStore); ok {
			return ws, none, nil
		}
	}
	root := strings.TrimPrefix(f.scope, "sqliteq:")
	single := root == f.key
	wf, release := f, none
	if single {
		var n int
		if err := f.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name = '__sqliteq_wf_workflows'`).Scan(&n); err != nil {
			return nil, none, fmt.Errorf("checking for workflow tables: %w", err)
		}
		if n == 0 {
			return nil, none, nil
		}
	} else {
		path, err := resolve(filepath.Join(root, workflowsFile))
		if err == nil {
			_, err = os.Stat(path)
		}
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return nil, none, nil
		case err != nil:
			return nil, none, fmt.Errorf("checking for the workflow file: %w", err)
		}
		if wf, err = acquire(path, root, f.pages); err != nil {
			return nil, none, fmt.Errorf("opening the workflow file: %w", err)
		}
		release = func() { wf.release() }
	}
	qs := &queues{cfg: storeConfig{singleFile: single, checkpointPages: f.pages}, root: root, files: map[*file]struct{}{}}
	qs.byID.Store(&map[string]*handle{})
	ws = &WorkflowStore{qs: qs, f: wf, single: single, scope: f.scope, cfg: workflowConfig{resultLimit: defaultResultLimit, retention: defaultRetention}}
	// tables of an older version get the columns this one reads
	if err := ws.init(); err != nil {
		release()
		return nil, none, err
	}
	return ws, release, nil
}
