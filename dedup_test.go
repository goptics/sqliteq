package sqliteq

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// TestImplementsDedup checks at compile time that both kinds of queue
// implement varmq's IDedupQueue and IDedupPriorityQueue.
func TestImplementsDedup(t *testing.T) {
	var _ interface{ EnqueueOnce(any, string) bool } = (*Queue)(nil)
	var _ interface{ EnqueueOnce(any, int, string) bool } = (*PriorityQueue)(nil)
}

// once adds item under id; on a priority queue it does so at priority 1
type once func(item any, id string) bool

// openOnce opens queue "q", or priority queue "q", in a fresh folder store,
// and returns its Queue with its EnqueueOnce. countRows reads table "q".
func openOnce(t *testing.T, priority bool, opts ...Option) (*Queue, once, Queues) {
	t.Helper()
	qs := mustNew(t, t.TempDir())
	if priority {
		pq, err := qs.NewPriorityQueue("q", opts...)
		if err != nil {
			qs.Close()
			t.Fatalf("Failed to create priority queue: %v", err)
		}
		return pq.Queue, func(item any, id string) bool { return pq.EnqueueOnce(item, 1, id) }, qs
	}
	q, err := qs.NewQueue("q", opts...)
	if err != nil {
		qs.Close()
		t.Fatalf("Failed to create queue: %v", err)
	}
	return q, q.EnqueueOnce, qs
}

// dequeueAll pops every pending item and returns them as strings, in order
func dequeueAll(t *testing.T, q *Queue) []string {
	t.Helper()
	var out []string
	for {
		item, ok := q.Dequeue()
		if !ok {
			return out
		}
		out = append(out, string(item.([]byte)))
	}
}

func TestEnqueueOnceRepeat(t *testing.T) { testOnceRepeat(t, false) }

func testOnceRepeat(t *testing.T, priority bool) {
	q, add, qs := openOnce(t, priority)
	defer qs.Close()

	if !add([]byte("a"), "j1") {
		t.Fatal("First EnqueueOnce failed")
	}
	if !add([]byte("b"), "j1") {
		t.Error("A repeat returned false, want true")
	}
	if n := q.Len(); n != 1 {
		t.Errorf("Expected 1 pending item, got %d", n)
	}
	if n := countRows(t, q.f.db, "1"); n != 1 {
		t.Errorf("Expected 1 row, got %d", n)
	}
	if got := dequeueAll(t, q); fmt.Sprint(got) != "[a]" {
		t.Errorf("Dequeued %v, want [a]", got)
	}

	// Dequeue deleted the row, and the id with it
	if !add([]byte("c"), "j1") {
		t.Error("EnqueueOnce after Dequeue returned false, want true")
	}
	if got := dequeueAll(t, q); fmt.Sprint(got) != "[c]" {
		t.Errorf("Dequeued %v after the re-add, want [c]", got)
	}
}

func TestEnqueueOnceWhileProcessing(t *testing.T) { testOnceWhileProcessing(t, false) }

func testOnceWhileProcessing(t *testing.T, priority bool) {
	q, add, qs := openOnce(t, priority)
	defer qs.Close()

	if !add([]byte("a"), "j1") {
		t.Fatal("EnqueueOnce failed")
	}
	if _, ok, _ := q.DequeueWithAckId(); !ok {
		t.Fatal("DequeueWithAckId failed")
	}

	if !add([]byte("b"), "j1") {
		t.Error("A repeat of a processing job returned false, want true")
	}
	if n := q.Len(); n != 0 {
		t.Errorf("Expected no pending item, got %d", n)
	}
	if n := countRows(t, q.f.db, "1"); n != 1 {
		t.Errorf("Expected 1 row (the processing one), got %d", n)
	}
}

func TestEnqueueOnceAfterAck(t *testing.T) { testOnceAfterAck(t, false) }

func testOnceAfterAck(t *testing.T, priority bool) {
	for _, remove := range []bool{true, false} {
		t.Run(fmt.Sprintf("remove=%v", remove), func(t *testing.T) {
			q, add, qs := openOnce(t, priority, WithRemoveOnComplete(remove))
			defer qs.Close()

			if !add([]byte("a"), "j1") {
				t.Fatal("EnqueueOnce failed")
			}
			_, ok, ackID := q.DequeueWithAckId()
			if !ok {
				t.Fatal("DequeueWithAckId failed")
			}
			if !q.Acknowledge(ackID) {
				t.Fatal("Acknowledge failed")
			}

			if !add([]byte("c"), "j1") {
				t.Error("EnqueueOnce after the ack returned false, want true")
			}
			if remove {
				// The row is gone, and the id with it
				if got := dequeueAll(t, q); fmt.Sprint(got) != "[c]" {
					t.Errorf("Dequeued %v, want [c]", got)
				}
				return
			}
			// The completed row keeps the id
			if n := q.Len(); n != 0 {
				t.Errorf("Expected no pending item, got %d", n)
			}
			if n := countRows(t, q.f.db, "1"); n != 1 {
				t.Errorf("Expected 1 row (the completed one), got %d", n)
			}
		})
	}
}

func TestEnqueueOncePriority(t *testing.T) {
	t.Run("Repeat", func(t *testing.T) { testOnceRepeat(t, true) })
	t.Run("WhileProcessing", func(t *testing.T) { testOnceWhileProcessing(t, true) })
	t.Run("AfterAck", func(t *testing.T) { testOnceAfterAck(t, true) })

	t.Run("Order", func(t *testing.T) {
		qs := mustNew(t, t.TempDir())
		defer qs.Close()
		pq, err := qs.NewPriorityQueue("q")
		if err != nil {
			t.Fatalf("Failed to create priority queue: %v", err)
		}

		for _, c := range []struct {
			item     string
			priority int
			id       string
		}{
			{"low", 5, "a"},
			{"high", 1, "b"},
			{"mid", 3, "c"},
			{"high-again", 0, "b"}, // a repeat: neither stored nor moved up
			{"low-again", 9, "a"},
			{"no-id", 2, ""},
		} {
			if !pq.EnqueueOnce([]byte(c.item), c.priority, c.id) {
				t.Fatalf("EnqueueOnce(%s, %d, %q) failed", c.item, c.priority, c.id)
			}
		}

		if got := dequeueAll(t, pq.Queue); fmt.Sprint(got) != "[high no-id mid low]" {
			t.Errorf("Dequeued %v, want [high no-id mid low]", got)
		}
	})
}

// An empty id stores the item like Enqueue does, every time
func TestEnqueueOnceEmptyID(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()

	for i := 0; i < 3; i++ {
		if !q.EnqueueOnce([]byte("x"), "") {
			t.Fatalf("EnqueueOnce %d with no id failed", i)
		}
		if !q.Enqueue([]byte("y")) {
			t.Fatalf("Enqueue %d failed", i)
		}
	}
	if n := q.Len(); n != 6 {
		t.Errorf("Expected 6 pending items, got %d", n)
	}
	if n := countRows(t, q.f.db, "job_id IS NULL"); n != 6 {
		t.Errorf("Expected 6 rows with a NULL job_id, got %d", n)
	}
}

// The conflict clause covers the job_id index only: a nil item still fails,
// and a failed add leaves its id free
func TestEnqueueOnceNil(t *testing.T) {
	for _, priority := range []bool{false, true} {
		t.Run(fmt.Sprintf("priority=%v", priority), func(t *testing.T) {
			q, add, qs := openOnce(t, priority)
			defer qs.Close()

			if add(nil, "j1") {
				t.Error("EnqueueOnce(nil) succeeded")
			}
			if q.Enqueue(nil) {
				t.Error("Enqueue(nil) succeeded")
			}
			if n := countRows(t, q.f.db, "1"); n != 0 {
				t.Errorf("Expected no rows, got %d", n)
			}
			if !add([]byte("a"), "j1") {
				t.Error("EnqueueOnce after a failed one failed")
			}
			if n := q.Len(); n != 1 {
				t.Errorf("Expected 1 pending item, got %d", n)
			}
		})
	}
}

// Two adds of one id in one grouped round: the second conflicts inside the
// round's transaction, both return true, and one row is stored. With a nil
// item in the round, the round falls back to one transaction per call and
// the same holds.
func TestEnqueueOnceOneRound(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(fmt.Sprintf("bad=%v", bad), func(t *testing.T) {
			q, qs := openQueue(t)
			defer qs.Close()

			first := func() bool { return q.Enqueue([]byte("x")) }
			rest := []func() bool{
				func() bool { return q.EnqueueOnce([]byte("a"), "j1") },
				func() bool { return q.EnqueueOnce([]byte("b"), "j1") },
				func() bool { return q.EnqueueOnce([]byte("c"), "j2") },
			}
			if bad {
				rest = append(rest,
					func() bool { return q.Enqueue(nil) },
					func() bool { return q.EnqueueOnce(nil, "j3") },
				)
			}

			got := oneRound(t, q.f, first, rest)
			for i := 0; i < 4; i++ {
				if !got[i] {
					t.Errorf("Call %d failed", i)
				}
			}
			for i := 4; i < len(got); i++ {
				if got[i] {
					t.Errorf("Call %d (a nil item) succeeded", i)
				}
			}
			if n := countRows(t, q.f.db, "job_id = 'j1'"); n != 1 {
				t.Errorf("Expected 1 row for j1, got %d", n)
			}
			if n := countRows(t, q.f.db, "1"); n != 3 {
				t.Errorf("Expected 3 rows (x, j1, j2), got %d", n)
			}
		})
	}
}

// 32 concurrent adds of one id: every call returns true and one row is stored
func TestEnqueueOnceConcurrent(t *testing.T) {
	for _, priority := range []bool{false, true} {
		t.Run(fmt.Sprintf("priority=%v", priority), func(t *testing.T) {
			q, add, qs := openOnce(t, priority)
			defer qs.Close()

			const n = 32
			var (
				wg    sync.WaitGroup
				start = make(chan struct{})
				oks   [n]bool
			)
			for g := 0; g < n; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					oks[g] = add([]byte(fmt.Sprint(g)), "j1")
				}()
			}
			close(start)
			wg.Wait()

			for g, ok := range oks {
				if !ok {
					t.Errorf("EnqueueOnce in goroutine %d returned false", g)
				}
			}
			if l := q.Len(); l != 1 {
				t.Errorf("Expected 1 pending item, got %d", l)
			}
			if c := countRows(t, q.f.db, "1"); c != 1 {
				t.Errorf("Expected 1 row, got %d", c)
			}
		})
	}
}

// A v0.2.3 file gains job_id and its unique partial index on open, keeps
// them on reopen, and its old rows still dequeue in order.
func TestEnqueueOnceOldFile(t *testing.T) {
	for _, priority := range []bool{false, true} {
		name := "jobs"
		if priority {
			name = "pjobs"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "v023.db")
			writeV023File(t, path, name, priority)

			for round := 1; round <= 2; round++ {
				qs := mustNew(t, path, WithSingleFile())
				var (
					q   *Queue
					add once
				)
				if priority {
					pq, err := qs.NewPriorityQueue(name)
					if err != nil {
						t.Fatalf("Round %d: failed to open v0.2.3 priority queue: %v", round, err)
					}
					q, add = pq.Queue, func(item any, id string) bool { return pq.EnqueueOnce(item, 0, id) }
				} else {
					var err error
					if q, err = qs.NewQueue(name); err != nil {
						t.Fatalf("Round %d: failed to open v0.2.3 queue: %v", round, err)
					}
					add = q.EnqueueOnce
				}

				if ok, err := hasColumn(q.f.db, name, "job_id"); err != nil || !ok {
					t.Fatalf("Round %d: job_id column present = %v, %v", round, ok, err)
				}
				var unique, partial int
				err := q.f.db.QueryRow(`SELECT "unique", partial FROM pragma_index_list(?) WHERE name = ?`,
					name, name+jobIDIndexSuffix).Scan(&unique, &partial)
				if err != nil || unique != 1 || partial != 1 {
					t.Errorf("Round %d: job_id index unique=%d partial=%d err=%v, want a unique partial index", round, unique, partial, err)
				}
				if cols, err := indexColumns(q.f.db, name+jobIDIndexSuffix); err != nil || fmt.Sprint(cols) != "[job_id]" {
					t.Errorf("Round %d: job_id index columns = %v, %v; want [job_id]", round, cols, err)
				}

				if round == 1 {
					if !add([]byte("new"), "j1") || !add([]byte("dup"), "j1") {
						t.Error("EnqueueOnce on the old table failed")
					}
					if n := q.Len(); n != 6 {
						t.Errorf("Expected 6 pending jobs, got %d", n)
					}
				} else {
					want := "[job-1 job-2 job-3 job-4 job-5 new]"
					if got := dequeueAll(t, q); fmt.Sprint(got) != want {
						t.Errorf("Dequeued %v, want %s", got, want)
					}
				}

				if err := qs.Close(); err != nil {
					t.Fatalf("Round %d: failed to close: %v", round, err)
				}
			}
		})
	}
}
