package sqliteq

import (
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/goptics/varmq/contract"
)

// The no-ID fast path of jobIDOf gives what a full parse gives.
func TestJobIDOfMatchesParse(t *testing.T) {
	parse := func(data []byte) string {
		var env struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(data, &env) != nil {
			return ""
		}
		return env.ID
	}
	for _, s := range []string{
		`{"id":"","status":"queued","data":1,"enqueued_at":5}`,
		`{"id":"","status":"queued","data":{"id":"inner"}}`,
		`{"id":"","ID":"x"}`,
		`{"id":"","Id":"x"}`,
		`{"id":"","id":"x"}`,
		`{"id":"","ıd":"x"}`,
		`{"id":"","İD":"x"}`,
		`{"id":"","id":"x"}`,
		`{"id":"","data":"invoice"}`,
		`{"id":"" broken`,
		`{"id":"abc","data":1}`,
		`{"data":1,"id":"z"}`,
	} {
		if got, want := jobIDOf(sql.NullString{}, []byte(s)), parse([]byte(s)); got != want {
			t.Errorf("%s: got %q, want %q", s, got, want)
		}
	}
}

// A watch of an ended job reads its record by id, whatever bytes the id
// holds: JSON-safe ones by the prepared read, others by an IN list.
func TestWatchEndedOddIDs(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	odd := []string{"plain", "quote\"back\\slash", "nul\x00byte", "bad\xffutf8", "ünï"}
	for _, id := range odd {
		if !q.EnqueueOnce([]byte("x"), id) {
			t.Fatalf("EnqueueOnce %q failed", id)
		}
		_, ack := pop(t, q)
		if !q.Reject(ack, id) {
			t.Fatalf("Reject %q failed", id)
		}
	}
	for _, id := range odd {
		if o := waitOutcome(t, watch(q, id)); o.ID != id || o.Error != id || o.State != contract.JobFailed {
			t.Errorf("%q: got %+v", id, o)
		}
	}
}
