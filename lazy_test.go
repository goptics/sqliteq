package sqliteq

import "testing"

// The plain path prepares no cold statement: each one held on a
// connection pins lookaside slots there.
func TestPlainPathLeavesColdStatementsUnprepared(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()

	q.Enqueue([]byte("a"))
	if _, ok, ackID := q.DequeueWithAckId(); !ok || !q.Acknowledge(ackID) {
		t.Fatal("Expected the plain add, pop and ack to work")
	}
	cold := map[string]*lazyStmt{"insert": &q.insert, "claim": &q.claim, "pop": &q.pop, "fin": &q.fin, "values": &q.values,
		"putJob": &q.putJob, "isMarked": &q.isMarked, "tally": &q.tally, "popFast": &q.popFast, "byIDs": &q.byIDs}
	for name, l := range cold {
		if l.s.Load() != nil {
			t.Errorf("Expected %s unprepared on the plain path", name)
		}
	}
	if q.Values(); q.values.s.Load() == nil {
		t.Error("Expected Values to prepare its statement")
	}
}
