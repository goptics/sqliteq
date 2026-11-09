package sqliteq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
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
	h, ok := s.qs.handleOf(queueID)
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
		return nil, q.finish(ackID, state, reason, result)
	}

	ctx := context.Background()
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
	if !s.single {
		pushed = nil
		if r.minSeq != 0 {
			pushed = s.drain(ctx, `workflow_id = ? AND seq >= ?`, e.Workflow, r.minSeq)
		}
	}
	if !acked {
		q.finish(ackID, state, reason, result)
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

// endPurged ends the workflow tasks of items a Purge of queue q removed as
// cancelled (reason "queue purged"), so their workflows go on. There is no
// job to settle: Purge deleted it. A task not in q, or one that had ended,
// changes nothing.
func (s *WorkflowStore) endPurged(q *Queue, tags []wfItemTag) {
	ctx := context.Background()
	for _, tag := range tags {
		e := contract.TaskEnd{Workflow: tag.wf, Task: tag.task, Undo: tag.undo, Outcome: contract.OutcomeCancelled, Reason: purgeReason}
		var r endResult
		err := s.write(ctx, func(tx *sql.Tx) (err error) {
			r, err = s.endTx(ctx, tx, q.id, e)
			return err
		})
		if err != nil {
			log.Printf("sqliteq: purge of %s: ending workflow %s task %s: %v", q.name, tag.wf, tag.task, err)
			continue
		}
		if r.minSeq != 0 {
			s.drain(ctx, `workflow_id = ? AND seq >= ?`, tag.wf, r.minSeq)
		}
	}
}
