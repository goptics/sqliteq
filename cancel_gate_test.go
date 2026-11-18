package sqliteq

import (
	"testing"
)

// hasMarkedRow reports whether meta row 'marked' exists.
func hasMarkedRow(t *testing.T, q *Queue) bool {
	t.Helper()
	var n int
	if err := q.f.db.QueryRow("SELECT COUNT(*) FROM " + q.meta + " WHERE key = 'marked'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

// A cancel from another object gates this object's fast pops through
// meta row 'marked': the marked job is dropped by claim and by Dequeue;
// once no mark is live a gated pop drops the row and fast pops resume.
func TestCancelGateRow(t *testing.T) {
	for _, viaDequeue := range []bool{false, true} {
		q, qs := openQueue(t)
		other := otherProcess(t, q)
		for _, id := range []string{"a", "b", "c"} {
			q.EnqueueOnce(job(id), id)
		}
		if err := other.CancelJob("a"); err != nil {
			t.Fatal(err)
		}
		if !hasMarkedRow(t, q) {
			t.Fatal("Expected CancelJob to store 'marked'")
		}
		var got string
		if viaDequeue {
			v, ok := q.Dequeue()
			if !ok {
				t.Fatal("Dequeue found nothing")
			}
			got = string(v.([]byte))
		} else {
			got, _ = pop(t, q)
		}
		if got != string(job("b")) {
			t.Errorf("dequeue=%v: got %s, want b (a dropped)", viaDequeue, got)
		}
		if hasMarkedRow(t, q) {
			t.Errorf("dequeue=%v: Expected 'marked' gone with no live mark", viaDequeue)
		}
		if got, _ := pop(t, q); got != string(job("c")) {
			t.Errorf("dequeue=%v: got %s, want c", viaDequeue, got)
		}
		qs.Close()
	}
}

// A live mark an earlier build left (no 'marked' row) gates fast pops
// after open.
func TestCancelGateRowFromEarlierBuild(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	q.EnqueueOnce(job("a"), "a")
	q.EnqueueOnce(job("b"), "b")
	if err := q.CancelJob("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.f.db.Exec("DELETE FROM " + q.meta + " WHERE key = 'marked'"); err != nil {
		t.Fatal(err)
	}
	o := otherProcess(t, q)
	if got, _ := pop(t, o); got != string(job("b")) {
		t.Errorf("got %s, want b (a dropped)", got)
	}
}
