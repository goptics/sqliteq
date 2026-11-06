package sqliteq

import "testing"

// Reject removes a processing row for good: it is not requeued, a second
// Reject or an Acknowledge of the same ack ID fails, and a bad ack ID fails.
func TestReject(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()

	q.Enqueue("bad job")
	q.Enqueue("next")
	_, ok, ackID := q.DequeueWithAckId()
	if !ok {
		t.Fatal("DequeueWithAckId failed")
	}

	if !q.Reject(ackID, "boom") {
		t.Fatal("Reject failed")
	}
	if n := countRows(t, q.f.db, "1 = 1"); n != 1 {
		t.Errorf("Expected 1 row left (the pending one), got %d", n)
	}
	if err := q.requeueNoAck(); err != nil { // what a restart runs
		t.Fatal(err)
	}
	if n := q.Len(); n != 1 {
		t.Errorf("Expected Len 1 after a requeue sweep (rejected job not back), got %d", n)
	}
	if q.Reject(ackID, "again") || q.Acknowledge(ackID) {
		t.Error("Expected a second finish of the same ack ID to fail")
	}
	if q.Reject("not-an-ack-id", "x") {
		t.Error("Expected Reject of a malformed ack ID to fail")
	}
}

// With WithRemoveOnComplete(false) a rejected row is kept as 'failed',
// apart from acknowledged rows kept as 'completed'.
func TestRejectKeepsFailedRow(t *testing.T) {
	q, qs := openQueue(t, WithRemoveOnComplete(false))
	defer qs.Close()

	q.Enqueue("ok")
	q.Enqueue("bad")
	_, _, okID := q.DequeueWithAckId()
	_, _, badID := q.DequeueWithAckId()
	if !q.Acknowledge(okID) || !q.Reject(badID, "boom") {
		t.Fatal("finish failed")
	}

	if n := countRows(t, q.f.db, "status = 'completed' AND ack = 1"); n != 1 {
		t.Errorf("Expected 1 completed row, got %d", n)
	}
	if n := countRows(t, q.f.db, "status = 'failed' AND ack = 1"); n != 1 {
		t.Errorf("Expected 1 failed row, got %d", n)
	}
	if err := q.requeueNoAck(); err != nil {
		t.Fatal(err)
	}
	if n := q.Len(); n != 0 {
		t.Errorf("Expected Len 0 (finished rows never requeue), got %d", n)
	}
}

// A priority queue rejects through the embedded Queue.
func TestPriorityReject(t *testing.T) {
	qs := mustNew(t, t.TempDir())
	defer qs.Close()
	pq, err := qs.NewPriorityQueue("p")
	if err != nil {
		t.Fatal(err)
	}
	pq.Enqueue("x", 1)
	_, ok, ackID := pq.DequeueWithAckId()
	if !ok || !pq.Reject(ackID, "boom") {
		t.Fatal("priority Reject failed")
	}
	if pq.Len() != 0 {
		t.Errorf("Expected empty priority queue, got %d", pq.Len())
	}
}
