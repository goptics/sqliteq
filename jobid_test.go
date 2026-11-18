package sqliteq

import (
	"database/sql"
	"encoding/json"
	"testing"
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
