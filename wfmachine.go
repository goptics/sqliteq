package sqliteq

import (
	"fmt"
	"slices"

	"github.com/goptics/varmq/contract"
)

// Workflow state machine: varmq's wfmachine.go over a workflow loaded from
// the tables. Pure: no I/O. The caller holds the write tx. Every walk goes
// in order, never map order, so pushes come out in task order.

type wfTask struct {
	name       string
	deps       []string
	always     bool
	queueID    string
	jobID      string
	priority   int
	undo       *wfUndo
	state      contract.JobState
	open       int // dependencies not ended yet
	reason     string
	result     []byte // set by a completed finish only; not loaded
	resultSize int    // length of the stored result
	undoState  contract.JobState
	undoOpen   int // undos of completed descendants not ended yet
	undoReason string
}

// wfUndo is a task's compensation job.
type wfUndo struct{ queueID, jobID string }

type wfFlow struct {
	id           string
	state        contract.WorkflowState
	created      int64
	finished     int64
	failFast     bool
	undoOnCancel bool
	failed       bool // an unabsorbed failure happened before any cancel
	cancelled    bool // cancel was called
	order        []string
	tasks        map[string]*wfTask
	next         map[string][]string // dependents per task, in order
}

// wfEffect is one thing storage must do: push a task (or its undo), and/or
// end the workflow.
type wfEffect struct {
	push string
	undo bool
	end  contract.WorkflowState
}

func (s *wfFlow) index() {
	s.next = make(map[string][]string, len(s.order))
	for _, name := range s.order {
		for _, d := range s.tasks[name].deps {
			s.next[d] = append(s.next[d], name)
		}
	}
}

// finish applies a task's (or undo's) outcome. A finish for a task that is
// not pending or processing, or for an ended workflow, does nothing.
func (s *wfFlow) finish(e contract.TaskEnd) ([]wfEffect, error) {
	t, ok := s.tasks[e.Task]
	if !ok {
		return nil, fmt.Errorf("varmq: workflow %q has no task %q", e.Workflow, e.Task)
	}
	if e.Undo {
		return s.finishUndo(t, e), nil
	}
	if s.state != contract.WorkflowRunning || !active(t.state) {
		return nil, nil
	}

	t.reason, t.result = e.Reason, e.Result
	push := map[string]bool{}
	if e.Outcome == contract.OutcomeCompleted {
		t.state = contract.JobCompleted
		for _, d := range s.next[e.Task] {
			s.release(d, push)
		}
		return s.settle(push), nil
	}

	t.state = contract.JobFailed
	if e.Outcome == contract.OutcomeCancelled {
		t.state = contract.JobCancelled
	}
	// After cancel, ends (pre-run cancels included) are not failures
	if !s.cancelled && !s.absorbed(e.Task) {
		if s.failFast && !s.failed { // first unabsorbed failure only
			s.failFast0(e.Task, push)
		}
		s.failed = true
	}
	s.drop(e.Task, push)
	return s.settle(push), nil
}

// cancel cancels waiting tasks now; pending ones stay for the pre-run check.
func (s *wfFlow) cancel(undo bool) []wfEffect {
	if s.state != contract.WorkflowRunning || s.cancelled {
		return nil
	}
	s.cancelled, s.undoOnCancel = true, undo
	for _, name := range s.order {
		if t := s.tasks[name]; t.state == contract.JobWaiting {
			t.state = contract.JobCancelled
		}
	}
	return s.settle(nil)
}

// runnable is the pre-run check.
func (s *wfFlow) runnable(task string, undo bool) bool {
	t, ok := s.tasks[task]
	if !ok {
		return false
	}
	if undo {
		return s.state == contract.WorkflowCompensating && t.undo != nil && t.undoState == contract.JobPending
	}
	stopped := s.failFast && s.failed && !t.always
	return s.state == contract.WorkflowRunning && !s.cancelled && !stopped && t.state == contract.JobPending
}

func (s *wfFlow) finishUndo(t *wfTask, e contract.TaskEnd) []wfEffect {
	if s.state != contract.WorkflowCompensating || !active(t.undoState) {
		return nil
	}
	t.undoState, t.undoReason = contract.JobFailed, e.Reason
	if e.Outcome == contract.OutcomeCompleted {
		t.undoState, t.undoReason = contract.JobCompleted, ""
	}
	push := map[string]bool{}
	up := s.reach(e.Task, true)
	for _, name := range s.order {
		x := s.tasks[name]
		if !up[name] || x.undoState != contract.JobWaiting {
			continue
		}
		if x.undoOpen--; x.undoOpen == 0 {
			x.undoState = contract.JobPending
			push[name] = true
		}
	}
	fx := s.emit(push, true)

	failed := false
	for _, name := range s.order {
		switch s.tasks[name].undoState {
		case contract.JobWaiting, contract.JobPending, contract.JobProcessing:
			return fx
		case contract.JobFailed:
			failed = true
		}
	}
	end := contract.WorkflowCompensated
	if failed {
		end = contract.WorkflowCompensationFailed
	}
	return s.endWith(fx, end)
}

// release counts one ended dependency of a waiting task; pushes it at zero.
func (s *wfFlow) release(name string, push map[string]bool) {
	t := s.tasks[name]
	if t.state != contract.JobWaiting {
		return
	}
	if t.open--; t.open == 0 {
		t.state = contract.JobPending
		push[name] = true
	}
}

// drop applies a failed or cancelled task to its dependents: always ones
// count it as ended, others are cancelled, recursively.
func (s *wfFlow) drop(name string, push map[string]bool) {
	for _, d := range s.next[name] {
		t := s.tasks[d]
		if t.state != contract.JobWaiting {
			continue
		}
		if t.always {
			s.release(d, push)
			continue
		}
		t.state = contract.JobCancelled
		s.drop(d, push)
	}
}

// absorbed: the task has dependents and every one is always.
func (s *wfFlow) absorbed(name string) bool {
	next := s.next[name]
	return len(next) > 0 && !slices.ContainsFunc(next, func(d string) bool { return !s.tasks[d].always })
}

// failFast0 cancels every waiting task on an unabsorbed failure, except the
// failed task's always dependents; each cancelled task is dropped.
func (s *wfFlow) failFast0(failed string, push map[string]bool) {
	keep := map[string]bool{}
	for _, d := range s.next[failed] {
		keep[d] = s.tasks[d].always
	}
	var cancelled []string
	for _, name := range s.order {
		if t := s.tasks[name]; t.state == contract.JobWaiting && !keep[name] {
			t.state = contract.JobCancelled
			cancelled = append(cancelled, name)
		}
	}
	for _, name := range cancelled {
		s.drop(name, push)
	}
}

// reach returns every task reachable from name: ancestors when up, else
// descendants.
func (s *wfFlow) reach(name string, up bool) map[string]bool {
	seen := map[string]bool{}
	stack := []string{name}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		edges := s.next[n]
		if up {
			edges = s.tasks[n].deps
		}
		for _, m := range edges {
			if !seen[m] {
				seen[m] = true
				stack = append(stack, m)
			}
		}
	}
	return seen
}

// settle emits the pushes and, once the forward run is over, ends the
// workflow or starts compensation.
func (s *wfFlow) settle(push map[string]bool) []wfEffect {
	fx := s.emit(push, false)
	for _, name := range s.order {
		switch s.tasks[name].state {
		case contract.JobWaiting, contract.JobPending, contract.JobProcessing:
			return fx
		}
	}
	if s.failed || s.cancelled && s.undoOnCancel {
		if undos := s.compensate(); len(undos) > 0 {
			s.state = contract.WorkflowCompensating
			return append(fx, undos...)
		}
	}
	switch {
	case s.failed:
		return s.endWith(fx, contract.WorkflowFailed)
	case s.cancelled:
		return s.endWith(fx, contract.WorkflowCancelled)
	}
	return s.endWith(fx, contract.WorkflowCompleted)
}

// compensate marks the undo of every completed task waiting on the undos of
// all its completed descendants that have one. Returns the undos ready now.
func (s *wfFlow) compensate() []wfEffect {
	push := map[string]bool{}
	for _, name := range s.order {
		t := s.tasks[name]
		if t.state != contract.JobCompleted || t.undo == nil {
			continue
		}
		t.undoState, t.undoOpen = contract.JobWaiting, 0
		for d := range s.reach(name, false) {
			if dt := s.tasks[d]; dt.state == contract.JobCompleted && dt.undo != nil {
				t.undoOpen++
			}
		}
		if t.undoOpen == 0 {
			t.undoState = contract.JobPending
			push[name] = true
		}
	}
	return s.emit(push, true)
}

func (s *wfFlow) endWith(fx []wfEffect, state contract.WorkflowState) []wfEffect {
	s.state = state
	return append(fx, wfEffect{end: state})
}

// emit turns a push set into effects in task order.
func (s *wfFlow) emit(push map[string]bool, undo bool) []wfEffect {
	if len(push) == 0 {
		return nil
	}
	fx := make([]wfEffect, 0, len(push))
	for _, name := range s.order {
		if push[name] {
			fx = append(fx, wfEffect{push: name, undo: undo})
		}
	}
	return fx
}

func active(st contract.JobState) bool {
	return st == contract.JobPending || st == contract.JobProcessing
}
