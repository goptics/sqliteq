package sqliteq

import (
	"fmt"
	"slices"
	"testing"

	"github.com/goptics/varmq/contract"
)

// Totals derive from the high mark and rare counters: a repeat id is no
// add, a Dequeue pop and a pop's cancel drop count as neither end, every
// object on the file reads the same, Purge restarts them.
func TestTotalsDerived(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(fmt.Sprintf("keep=%t", keep), func(t *testing.T) {
			q, qs := openQueue(t, WithRemoveOnComplete(!keep))
			defer qs.Close()
			other := otherProcess(t, q)

			for _, id := range []string{"a", "b", "c", "d", "e"} {
				if !q.EnqueueOnce(job(id), id) {
					t.Fatalf("EnqueueOnce %s failed", id)
				}
			}
			if !other.EnqueueOnce(job("a"), "a") || !q.EnqueueOnce(job("b"), "b") {
				t.Fatal("Expected a repeat EnqueueOnce to return true")
			}
			if _, ok := q.Dequeue(); !ok { // a: popped, no finish
				t.Fatal("Dequeue found nothing")
			}
			q.CancelJob("b")
			if _, ok := other.Dequeue(); !ok { // drops b, pops c
				t.Fatal("Dequeue found nothing")
			}
			_, d := pop(t, q)
			_, e := pop(t, other)
			if !other.Acknowledge(d) || !q.Reject(e, "x") {
				t.Fatal("finish failed")
			}

			want := contract.JobTotals{Submitted: 5, Completed: 1, Failed: 1}
			for i, o := range []*Queue{q, other} {
				if got := stats(t, o).Totals; got != want {
					t.Errorf("object %d: got %+v, want %+v", i, got, want)
				}
			}

			q.Purge()
			q.EnqueueOnce(job("f"), "f")
			q.EnqueueOnce(job("f"), "f")
			_, f := pop(t, q)
			q.Acknowledge(f)
			if got, want := stats(t, other).Totals, (contract.JobTotals{Submitted: 1, Completed: 1}); got != want {
				t.Errorf("after Purge: got %+v, want %+v", got, want)
			}
		})
	}
}

// A file whose meta an earlier build kept (submitted and completed
// counted per write, a per-add trigger) keeps its totals, and the trigger
// goes.
func TestTotalsFromEarlierMeta(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	enqueueN(t, q, 5)
	_, a := pop(t, q)
	pop(t, q) // stays processing
	q.Acknowledge(a)

	// The earlier build's counters: totals off from the rows on purpose
	for _, s := range []string{
		"DELETE FROM " + q.meta + " WHERE key IN ('base', 'other')",
		"INSERT OR REPLACE INTO " + q.meta + " (key, value) VALUES ('submitted', 9), ('completed', 4), ('failed', 2)",
		"CREATE TRIGGER " + quoteIdent(q.name+"__sqliteq_submitted") + " AFTER INSERT ON " + q.table +
			" BEGIN UPDATE " + q.meta + " SET value = value + 1 WHERE key = 'submitted'; END",
	} {
		if _, err := q.f.db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}

	o := otherProcess(t, q)
	if got, want := stats(t, o).Totals, (contract.JobTotals{Submitted: 9, Completed: 4, Failed: 2}); got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
	o.Enqueue([]byte("x"))
	if got, want := stats(t, o).Totals, (contract.JobTotals{Submitted: 10, Completed: 4, Failed: 2}); got != want {
		t.Errorf("after an add: got %+v, want %+v", got, want)
	}
	var n int
	q.f.db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name = ?", q.name+"__sqliteq_submitted").Scan(&n)
	if n != 0 {
		t.Error("Expected the earlier per-add trigger to be dropped")
	}
}

// A priority queue's repeat id is no add either; its priority stays.
func TestTotalsPriorityRepeat(t *testing.T) {
	qs := mustNew(t, t.TempDir())
	defer qs.Close()
	pq, err := qs.NewPriorityQueue("pq")
	if err != nil {
		t.Fatal(err)
	}
	if !pq.EnqueueOnce(job("a"), 5, "a") || !pq.EnqueueOnce(job("a"), 1, "a") || !pq.Enqueue(job("b"), 3) {
		t.Fatal("enqueue failed")
	}
	if got := ids(jobs(t, pq.Queue, contract.JobPending, 0, 10)); !slices.Equal(got, []string{"b", "a"}) {
		t.Errorf("pending: got %q", got)
	}
	_, ack := pop(t, pq.Queue)
	pq.Acknowledge(ack)
	if got, want := stats(t, pq.Queue).Totals, (contract.JobTotals{Submitted: 2, Completed: 1}); got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

// No trigger on the queue table: one compiles into every add (v0.3.0 had
// none); an add that stores an id clears its record and mark itself.
func TestQueueTableTriggers(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	rows, err := q.f.db.Query("SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?", q.name)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		rows.Scan(&n)
		names = append(names, n)
	}
	if len(names) != 0 {
		t.Errorf("got triggers %v", names)
	}
}

// The readd trigger an earlier build left is dropped on open; a re-add
// still clears the id's record.
func TestEarlierReaddTriggerDropped(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	if _, err := q.f.db.Exec(fmt.Sprintf(`CREATE TRIGGER %s AFTER INSERT ON %s WHEN NEW.job_id IS NOT NULL BEGIN
		DELETE FROM %s WHERE job_id = NEW.job_id; END`, quoteIdent(q.name+readdSuffix), q.table, q.jobs)); err != nil {
		t.Fatal(err)
	}
	o := otherProcess(t, q)
	var n int
	if err := o.f.db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?", q.name).Scan(&n); err != nil || n != 0 {
		t.Fatalf("Expected no trigger after open, got %d %v", n, err)
	}
	o.EnqueueOnce(job("a"), "a")
	_, ack := pop(t, o)
	if !o.Acknowledge(ack) {
		t.Fatal("ack failed")
	}
	o.EnqueueOnce(job("a"), "a")
	if err := o.f.db.QueryRow("SELECT COUNT(*) FROM " + o.jobs + " WHERE job_id = 'a'").Scan(&n); err != nil || n != 0 {
		t.Errorf("Expected the re-add to drop the record, got %d %v", n, err)
	}
}
