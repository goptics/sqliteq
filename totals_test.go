package sqliteq

import (
	"fmt"
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
