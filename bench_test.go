package sqliteq

import (
	"fmt"
	"testing"
)

// benchQueue opens a queue with n pending rows, filled in one statement.
func benchQueue(b *testing.B, n int) *Queue {
	b.Helper()
	qs, err := New(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { qs.Close() })
	q, err := qs.NewQueue("bench")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := q.f.db.Exec(fmt.Sprintf(`INSERT INTO %s (data, status, ack, created_at, updated_at)
		WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < ?)
		SELECT x'6a6f62', 'pending', 0, 0, 0 FROM c`, q.table), n); err != nil {
		b.Fatal(err)
	}
	return q
}

// backlog is the pending rows left behind the ones a benchmark takes
const backlog = 100_000

func BenchmarkClaim(b *testing.B) {
	q := benchQueue(b, b.N+backlog)
	b.ResetTimer()
	for range b.N {
		if _, ok, _ := q.DequeueWithAckId(); !ok {
			b.Fatal("empty")
		}
	}
}

func BenchmarkAck(b *testing.B) {
	q := benchQueue(b, b.N+backlog)
	items, acks := make([]any, b.N), make([]string, b.N)
	if n, _, err := q.DequeueBatchWithAckIds(items, acks); err != nil || n != b.N {
		b.Fatal(n, err)
	}
	b.ResetTimer()
	for _, id := range acks {
		if !q.Acknowledge(id) {
			b.Fatal("not acknowledged")
		}
	}
}

// BenchmarkBatchPop pops in batches of 64; one op is one item.
func BenchmarkBatchPop(b *testing.B) {
	q := benchQueue(b, b.N+backlog)
	items, acks := make([]any, 64), make([]string, 64)
	b.ResetTimer()
	for left := b.N; left > 0; {
		n, _, err := q.DequeueBatchWithAckIds(items[:min(64, left)], acks)
		if err != nil || n == 0 {
			b.Fatal(n, err)
		}
		left -= n
	}
}
