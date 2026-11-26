package sqliteq

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goptics/varmq"
	"github.com/goptics/varmq/contract"
)

func TestSQLiteQueue(t *testing.T) {
	// Create a temporary store folder
	dbPath := t.TempDir()

	queues := mustNew(t, dbPath)
	// Create a new queue with default settings (removeOnComplete = true)
	q, err := queues.NewQueue("test_queue")
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer queues.Close()

	// Test enqueue
	t.Run("Enqueue", func(t *testing.T) {
		success := q.Enqueue([]byte("test item 1"))
		if !success {
			t.Error("Enqueue failed")
		}

		success = q.Enqueue([]byte("42"))
		if !success {
			t.Error("Enqueue failed")
		}

		success = q.Enqueue([]byte("some complex data"))
		if !success {
			t.Error("Enqueue failed")
		}

		if q.Len() != 3 {
			t.Errorf("Expected queue length 3, got %d", q.Len())
		}
	})

	// Test values
	t.Run("Values", func(t *testing.T) {
		values := q.Values()
		if len(values) != 3 {
			t.Errorf("Expected 3 values, got %d", len(values))
		}
	})

	// Test dequeue
	t.Run("Dequeue", func(t *testing.T) {
		data, success := q.Dequeue()
		if !success {
			t.Error("Dequeue failed")
		}

		// The first item should be "test item 1" as bytes
		byteData, ok := data.([]byte)
		if !ok {
			t.Errorf("Expected []byte, got %T", data)
		}

		if string(byteData) != "test item 1" {
			t.Errorf("Expected 'test item 1', got '%s'", string(byteData))
		}

		if q.Len() != 2 {
			t.Errorf("Expected queue length 2, got %d", q.Len())
		}

		// Verify the item is completely removed from the database
		var count int
		row := q.f.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = 'completed' OR status = 'processing'", q.table))
		error := row.Scan(&count)
		if error != nil {
			t.Errorf("Error checking items in database: %v", error)
		}
		if count != 0 {
			t.Errorf("Expected 0 items with completed/processing status, got %d", count)
		}
	})

	// Test dequeue with ack ID
	t.Run("DequeueWithAckId", func(t *testing.T) {
		item, success, ackID := q.DequeueWithAckId()
		if !success {
			t.Error("DequeueWithAckId failed")
		}

		if ackID == "" {
			t.Error("Expected non-empty ack ID")
		}

		// The next item should be the string "42" as bytes
		byteData, ok := item.([]byte)
		if !ok {
			t.Errorf("Expected []byte, got %T", item)
		}

		if string(byteData) != "42" {
			t.Errorf("Expected '42', got '%s'", string(byteData))
		}

		// Test acknowledge
		ackSuccess := q.Acknowledge(ackID)
		if !ackSuccess {
			t.Error("Acknowledge failed")
		}

		// Test invalid ack ID
		ackSuccess = q.Acknowledge("invalid-ack-id")
		if ackSuccess {
			t.Error("Acknowledge with invalid ID should fail")
		}
	})

	// Test purge
	t.Run("Purge", func(t *testing.T) {
		q.Purge()
		if q.Len() != 0 {
			t.Errorf("Expected queue length 0 after purge, got %d", q.Len())
		}
	})

	// Test empty queue
	t.Run("EmptyQueue", func(t *testing.T) {
		item, success := q.Dequeue()
		if success {
			t.Errorf("Dequeue on empty queue should fail, got %v", item)
		}

		item, success, ackID := q.DequeueWithAckId()
		if success {
			t.Errorf("DequeueWithAckId on empty queue should fail, got %v, %s", item, ackID)
		}
	})
}

// Test removeOnComplete option behavior
func TestRemoveOnCompleteOption(t *testing.T) {
	// Test with removeOnComplete = false
	t.Run("KeepCompletedItems", func(t *testing.T) {
		// Create a temporary store folder
		dbPath := t.TempDir()

		// Create a queue with removeOnComplete = false
		queues := mustNew(t, dbPath)
		q, err := queues.NewQueue("test_queue", WithRemoveOnComplete(false))
		if err != nil {
			t.Fatalf("Failed to create queue: %v", err)
		}
		defer queues.Close()

		// Enqueue an item
		q.Enqueue("test item")

		// Dequeue with ack ID
		_, success, ackID := q.DequeueWithAckId()
		if !success {
			t.Error("DequeueWithAckId failed")
		}

		// Acknowledge the item
		if !q.Acknowledge(ackID) {
			t.Error("Acknowledge failed")
		}

		// Since removeOnComplete is false, the item should still be in the database
		// but marked as completed, so the queue length should be 0
		if q.Len() != 0 {
			t.Errorf("Expected queue length 0, got %d", q.Len())
		}

		// Verify the item is still in the database by checking directly
		var count int
		row := q.f.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = 'completed'", q.table))
		error := row.Scan(&count)
		if error != nil {
			t.Errorf("Error checking completed items: %v", error)
		}
		if count != 1 {
			t.Errorf("Expected 1 completed item in database, got %d", count)
		}
	})

	// Test with removeOnComplete = true (default)
	t.Run("RemoveCompletedItems", func(t *testing.T) {
		// Create a temporary store folder
		dbPath := t.TempDir()

		queues := mustNew(t, dbPath)
		// Create a queue with default removeOnComplete = true
		q, err := queues.NewQueue("test_queue")
		if err != nil {
			t.Fatalf("Failed to create queue: %v", err)
		}
		defer queues.Close()

		// Enqueue an item
		q.Enqueue("test item")

		// Test direct dequeue (no ack ID)
		_, success := q.Dequeue()
		if !success {
			t.Error("Dequeue failed")
		}

		// Since dequeueInternal with withAckId=false now deletes directly,
		// the item should be removed from the database immediately
		var count int
		row := q.f.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", q.table))
		error := row.Scan(&count)
		if error != nil {
			t.Errorf("Error checking items in database: %v", error)
		}
		if count != 0 {
			t.Errorf("Expected 0 items in database after Dequeue, got %d", count)
		}

		// Try with DequeueWithAckId and Acknowledge process
		q.Enqueue([]byte("test item 2"))

		_, success, ackID := q.DequeueWithAckId()
		if !success {
			t.Error("DequeueWithAckId failed")
		}

		// Check that the item is still in the database with processing status
		row = q.f.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = 'processing'", q.table))
		error = row.Scan(&count)
		if error != nil {
			t.Errorf("Error checking processing items: %v", error)
		}
		if count != 1 {
			t.Errorf("Expected 1 processing item in database, got %d", count)
		}

		// Acknowledge the item
		if !q.Acknowledge(ackID) {
			t.Error("Acknowledge failed")
		}

		// Since removeOnComplete is true, the item should be removed from the database
		row = q.f.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", q.table))
		error = row.Scan(&count)
		if error != nil {
			t.Errorf("Error checking items in database: %v", error)
		}
		if count != 0 {
			t.Errorf("Expected 0 items in database after Acknowledge, got %d", count)
		}
	})
}

// Test concurrent operations
func TestConcurrentOperations(t *testing.T) {
	// Create a temporary store folder
	dbPath := t.TempDir()

	// Create a new queue
	queues := mustNew(t, dbPath)
	q, err := queues.NewQueue("test_queue")
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer queues.Close()

	// Enqueue items concurrently
	numItems := 100
	done := make(chan bool)

	// Producer goroutine
	go func() {
		for i := 0; i < numItems; i++ {
			itemData := []byte(fmt.Sprintf("item-%d", i))
			if !q.Enqueue(itemData) {
				t.Errorf("Failed to enqueue item %d", i)
			}
		}
		done <- true
	}()

	// Consumer goroutine
	processed := 0
	ackIDs := make([]string, 0, numItems)

	go func() {
		for processed < numItems {
			_, success, ackID := q.DequeueWithAckId()
			if success {
				ackIDs = append(ackIDs, ackID)
				processed++
			}
		}
		done <- true
	}()

	// Wait for producer and consumer to finish
	<-done
	<-done

	// Verify all items were processed
	if processed != numItems {
		t.Errorf("Expected %d processed items, got %d", numItems, processed)
	}

	// Acknowledge all items
	for _, ackID := range ackIDs {
		if !q.Acknowledge(ackID) {
			t.Errorf("Failed to acknowledge item with ID %s", ackID)
		}
	}

	// Verify queue is empty
	if q.Len() != 0 {
		t.Errorf("Expected empty queue, got length %d", q.Len())
	}
}

func TestQuoteIdent(t *testing.T) {
	tt := []struct{ input, want string }{
		{"foo", `"foo"`},
		{"spaces are here", `"spaces are here"`},
		{`"quoted"`, `"""quoted"""`},
		{``, `""`}, // belive it or not this is a valid table name
	}

	for _, tc := range tt {
		t.Run(tc.input, func(t *testing.T) {
			got := quoteIdent(tc.input)
			if got != tc.want {
				t.Errorf("Unexpected quotes tabled (want %q, got %q)", tc.want, got)
			}
		})
	}
}

func TestTableInitErrorKeepsPool(t *testing.T) {
	qs := mustNew(t, filepath.Join(t.TempDir(), "pool.db"), WithSingleFile())
	defer qs.Close()

	ok, err := qs.NewQueue("ok")
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	// A view named like the queue makes CREATE INDEX fail during table init
	if _, err := ok.f.db.Exec(`CREATE VIEW "bad" AS SELECT 1 AS x`); err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}
	if _, err := qs.NewQueue("bad"); err == nil {
		t.Fatal("Expected table init to fail for the second queue")
	}

	if !ok.Enqueue([]byte("still works")) {
		t.Error("Enqueue on the first queue failed after the second queue's init error")
	}
}

func TestRecoveryError(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "recovery.db"))
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	q := &Queue{f: &file{db: db}, name: "recovery", table: quoteIdent("recovery")}
	db.Close()

	if err := q.requeueNoAck(); err == nil {
		t.Error("Expected an error from requeueNoAck on a closed database")
	}
}

// openQueue opens queue "q" in a fresh folder store.
func openQueue(t *testing.T, opts ...Option) (*Queue, Queues) {
	t.Helper()
	qs := mustNew(t, t.TempDir())
	q, err := qs.NewQueue("q", opts...)
	if err != nil {
		qs.Close()
		t.Fatalf("Failed to create queue: %v", err)
	}
	return q, qs
}

// countRows counts the rows of table "q" matching where.
func countRows(t *testing.T, db *sql.DB, where string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "q" WHERE ` + where).Scan(&n); err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	return n
}

func TestAckIDIsRowID(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()

	q.Enqueue([]byte("a"))
	q.Enqueue([]byte("b"))
	q.Dequeue()
	_, ok, ackID := q.DequeueWithAckId()
	if !ok {
		t.Fatal("DequeueWithAckId failed")
	}

	got, err := strconv.ParseInt(ackID, 10, 64)
	if err != nil {
		t.Fatalf("Ack id %q is not a decimal integer: %v", ackID, err)
	}
	var id int64
	if err := q.f.db.QueryRow(`SELECT id FROM "q" WHERE status = 'processing'`).Scan(&id); err != nil {
		t.Fatalf("Failed to read the claimed row: %v", err)
	}
	if got != id || id != 2 {
		t.Errorf("Ack id %d, claimed row id %d, want both 2", got, id)
	}
}

func TestAckGarbage(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()

	q.Enqueue([]byte("claimed"))
	q.Enqueue([]byte("pending"))
	_, ok, ackID := q.DequeueWithAckId()
	if !ok {
		t.Fatal("DequeueWithAckId failed")
	}

	// Garbage, a v0.2 cuid, non-canonical spellings of the claimed id, and
	// the id of a row that is still pending
	garbage := []string{"", "abc", "-1", "0", "99999999999999999999", "cjld2cjxh0000qzrmn831i7rn",
		" " + ackID, "+" + ackID, "0" + ackID, ackID + ".0", "2"}
	if ackID == "2" {
		t.Fatalf("Expected the claimed row to be id 1, got ack id %q", ackID)
	}
	for _, id := range garbage {
		if q.Acknowledge(id) {
			t.Errorf("Acknowledge(%q) = true, want false", id)
		}
	}

	if n := countRows(t, q.f.db, "1"); n != 2 {
		t.Errorf("Expected 2 rows untouched, got %d", n)
	}
	if n := countRows(t, q.f.db, "status = 'processing'"); n != 1 {
		t.Errorf("Expected the claimed row still processing, got %d processing rows", n)
	}
	if !q.Acknowledge(ackID) {
		t.Error("Acknowledge of the real id failed after the garbage")
	}
}

func TestAckTwice(t *testing.T) {
	for _, remove := range []bool{true, false} {
		t.Run(fmt.Sprintf("remove=%v", remove), func(t *testing.T) {
			q, qs := openQueue(t, WithRemoveOnComplete(remove))
			defer qs.Close()

			q.Enqueue([]byte("a"))
			_, ok, ackID := q.DequeueWithAckId()
			if !ok {
				t.Fatal("DequeueWithAckId failed")
			}
			if !q.Acknowledge(ackID) {
				t.Fatal("First Acknowledge failed")
			}
			if q.Acknowledge(ackID) {
				t.Error("Second Acknowledge of the same id returned true")
			}
		})
	}
}

func TestKeepCompleted(t *testing.T) {
	q, qs := openQueue(t, WithRemoveOnComplete(false))
	defer qs.Close()

	q.Enqueue([]byte("done"))
	q.Enqueue([]byte("waiting"))
	_, ok, ackID := q.DequeueWithAckId()
	if !ok {
		t.Fatal("DequeueWithAckId failed")
	}
	if !q.Acknowledge(ackID) {
		t.Fatal("Acknowledge failed")
	}

	if n := countRows(t, q.f.db, "status = 'completed' AND ack = 1"); n != 1 {
		t.Errorf("Expected 1 completed row with ack = 1, got %d", n)
	}
	if n := q.Len(); n != 1 {
		t.Errorf("Expected Len 1 (the completed row excluded), got %d", n)
	}
}

func TestOrderByID(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()

	const n = 1000
	for i := 0; i < n; i++ {
		if !q.Enqueue([]byte(strconv.Itoa(i))) {
			t.Fatalf("Enqueue %d failed", i)
		}
	}
	// Enqueues inside one millisecond, or a clock stepping back, leave
	// created_at out of insert order; here it runs backwards outright
	if _, err := q.f.db.Exec(`UPDATE "q" SET created_at = datetime('2025-01-01', '-' || id || ' seconds')`); err != nil {
		t.Fatalf("Failed to rewrite created_at: %v", err)
	}

	values := q.Values()
	if len(values) != n {
		t.Fatalf("Expected %d values, got %d", n, len(values))
	}
	for i, v := range values {
		if got := string(v.([]byte)); got != strconv.Itoa(i) {
			t.Fatalf("Values()[%d] = %q, want %q", i, got, strconv.Itoa(i))
		}
	}
	for i := 0; i < n; i++ {
		item, ok := q.Dequeue()
		if !ok {
			t.Fatalf("Dequeue %d failed", i)
		}
		if got := string(item.([]byte)); got != strconv.Itoa(i) {
			t.Fatalf("Dequeue %d = %q, want %q", i, got, strconv.Itoa(i))
		}
	}
}

func TestConcurrentClaimsNoDuplicates(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()

	const producers, perProducer, consumers = 8, 100, 8
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				if !q.Enqueue([]byte(fmt.Sprintf("%d-%d", p, i))) {
					t.Errorf("Enqueue %d-%d failed", p, i)
				}
			}
		}(p)
	}

	var (
		mu   sync.Mutex
		seen = map[string]int{}
		done atomic.Int64
	)
	var cw sync.WaitGroup
	for c := 0; c < consumers; c++ {
		cw.Add(1)
		go func() {
			defer cw.Done()
			for done.Load() < producers*perProducer {
				item, ok, ackID := q.DequeueWithAckId()
				if !ok {
					continue
				}
				mu.Lock()
				seen[string(item.([]byte))]++
				mu.Unlock()
				if !q.Acknowledge(ackID) {
					t.Errorf("Acknowledge(%s) failed", ackID)
				}
				done.Add(1)
			}
		}()
	}
	wg.Wait()
	cw.Wait()

	if len(seen) != producers*perProducer {
		t.Errorf("Expected %d distinct jobs, got %d", producers*perProducer, len(seen))
	}
	for job, n := range seen {
		if n != 1 {
			t.Errorf("Job %s delivered %d times", job, n)
		}
	}
	if n := countRows(t, q.f.db, "1"); n != 0 {
		t.Errorf("Expected no rows left, got %d", n)
	}
}

func TestImplementsBatch(t *testing.T) {
	type batcher interface {
		DequeueBatchWithAckIds([]any, []string) (int, int, error)
	}
	var _ batcher = (*Queue)(nil)
	var _ batcher = (*PriorityQueue)(nil)
}

// enqueueN adds jobs "0".."n-1" to q.
func enqueueN(t *testing.T, q *Queue, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if !q.Enqueue([]byte(strconv.Itoa(i))) {
			t.Fatalf("Enqueue %d failed", i)
		}
	}
}

func TestBatchPop(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	enqueueN(t, q, 10)

	items, ackIDs := make([]any, 4), make([]string, 4)
	n, remaining, err := q.DequeueBatchWithAckIds(items, ackIDs)
	if err != nil || n != 4 || remaining != 6 {
		t.Fatalf("Batch of 4: n=%d remaining=%d err=%v, want 4, 6, nil", n, remaining, err)
	}
	seen := map[string]bool{}
	for i := 0; i < n; i++ {
		if got := string(items[i].([]byte)); got != strconv.Itoa(i) {
			t.Errorf("items[%d] = %q, want %q", i, got, strconv.Itoa(i))
		}
		if _, ok := parseAckID(ackIDs[i]); !ok {
			t.Errorf("ackIDs[%d] = %q is not a row id", i, ackIDs[i])
		}
		if seen[ackIDs[i]] {
			t.Errorf("ackIDs[%d] = %q repeats", i, ackIDs[i])
		}
		seen[ackIDs[i]] = true
	}

	items, ackIDs = make([]any, 10), make([]string, 10)
	n, remaining, err = q.DequeueBatchWithAckIds(items, ackIDs)
	if err != nil || n != 6 || remaining != 0 {
		t.Fatalf("Batch of 10: n=%d remaining=%d err=%v, want 6, 0, nil", n, remaining, err)
	}
	for i := 0; i < n; i++ {
		if got := string(items[i].([]byte)); got != strconv.Itoa(i+4) {
			t.Errorf("items[%d] = %q, want %q", i, got, strconv.Itoa(i+4))
		}
	}

	n, remaining, err = q.DequeueBatchWithAckIds(items, ackIDs)
	if err != nil || n != 0 || remaining != 0 {
		t.Fatalf("Empty batch: n=%d remaining=%d err=%v, want 0, 0, nil", n, remaining, err)
	}
	if got := countRows(t, q.f.db, "status = 'processing'"); got != 10 {
		t.Errorf("Expected 10 processing rows, got %d", got)
	}
}

func TestBatchMinLen(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	enqueueN(t, q, 10)

	n, remaining, err := q.DequeueBatchWithAckIds(make([]any, 5), make([]string, 2))
	if err != nil || n > 2 || remaining != 10-n {
		t.Fatalf("n=%d remaining=%d err=%v, want n <= 2, 10-n, nil", n, remaining, err)
	}
	if got := countRows(t, q.f.db, "status = 'processing'"); got != n {
		t.Errorf("Expected %d processing rows, got %d", n, got)
	}
}

func TestBatchRemainingCap(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	enqueueN(t, q, 300)

	n, remaining, err := q.DequeueBatchWithAckIds(make([]any, 1), make([]string, 1))
	if err != nil || n != 1 || remaining != remainingCap {
		t.Fatalf("n=%d remaining=%d err=%v, want 1, %d, nil", n, remaining, err, remainingCap)
	}
	if got := q.Len(); got != 299 {
		t.Errorf("Len() = %d, want 299", got)
	}
}

func TestBatchClosed(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	enqueueN(t, q, 3)
	q.Close()

	n, _, err := q.DequeueBatchWithAckIds(make([]any, 3), make([]string, 3))
	if n != 0 || err != nil {
		t.Fatalf("n=%d err=%v after Close, want 0, nil", n, err)
	}
	if got := countRows(t, q.f.db, "status = 'pending'"); got != 3 {
		t.Errorf("Expected 3 pending rows, got %d", got)
	}
}

func TestCrashAfterBatch(t *testing.T) {
	dir := t.TempDir()
	qs := mustNew(t, dir)
	q, err := qs.NewQueue("q")
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	enqueueN(t, q, 5)
	if n, _, err := q.DequeueBatchWithAckIds(make([]any, 3), make([]string, 3)); n != 3 || err != nil {
		t.Fatalf("Batch of 3: n=%d err=%v", n, err)
	}
	// The process "dies": the file closes with 3 rows claimed and no ack
	if err := qs.Close(); err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	qs = mustNew(t, dir)
	defer qs.Close()
	q, err = qs.NewQueue("q")
	if err != nil {
		t.Fatalf("Failed to reopen queue: %v", err)
	}
	if got := q.Len(); got != 5 {
		t.Fatalf("Len() = %d after reopen, want 5", got)
	}
	for i := 0; i < 5; i++ {
		item, ok := q.Dequeue()
		if !ok {
			t.Fatalf("Dequeue %d failed", i)
		}
		if got := string(item.([]byte)); got != strconv.Itoa(i) {
			t.Fatalf("Dequeue %d = %q, want %q", i, got, strconv.Itoa(i))
		}
	}
}

func TestConcurrentWritersOneFile(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()

	concurrentWriters(t, q)
}

// concurrentWriters runs 8 producers and 16 ackers against one batch popper
// and checks every job is delivered once and acked.
func concurrentWriters(t *testing.T, q *Queue) {
	t.Helper()
	const producers, perProducer, ackers, batch = 8, 500, 16, 16
	const total = producers * perProducer

	var pw sync.WaitGroup
	for p := 0; p < producers; p++ {
		pw.Add(1)
		go func(p int) {
			defer pw.Done()
			for i := 0; i < perProducer; i++ {
				if !q.Enqueue([]byte(fmt.Sprintf("%d-%d", p, i))) {
					t.Errorf("Enqueue %d-%d failed", p, i)
				}
			}
		}(p)
	}

	type job struct{ data, ackID string }
	jobs := make(chan job, total)
	var (
		mu   sync.Mutex
		seen = map[string]int{}
	)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		defer close(jobs)
		items, ackIDs := make([]any, batch), make([]string, batch)
		// A failed add leaves fewer than total jobs, so the drain is bounded
		deadline := time.Now().Add(time.Minute)
		for got := 0; got < total; {
			if time.Now().After(deadline) {
				t.Errorf("Drained %d of %d jobs before the deadline", got, total)
				return
			}
			n, _, err := q.DequeueBatchWithAckIds(items, ackIDs)
			if err != nil {
				t.Errorf("Batch pop failed: %v", err)
				return
			}
			for i := 0; i < n; i++ {
				jobs <- job{string(items[i].([]byte)), ackIDs[i]}
			}
			got += n
		}
	}()

	var aw sync.WaitGroup
	for a := 0; a < ackers; a++ {
		aw.Add(1)
		go func() {
			defer aw.Done()
			for j := range jobs {
				mu.Lock()
				seen[j.data]++
				mu.Unlock()
				if !q.Acknowledge(j.ackID) {
					t.Errorf("Acknowledge(%s) of %s failed", j.ackID, j.data)
				}
			}
		}()
	}

	pw.Wait()
	<-drained
	aw.Wait()

	if len(seen) != total {
		t.Errorf("Expected %d distinct jobs, got %d", total, len(seen))
	}
	for data, n := range seen {
		if n != 1 {
			t.Errorf("Job %s delivered %d times", data, n)
		}
	}
	if n := countRows(t, q.f.db, "1"); n != 0 {
		t.Errorf("Expected no rows left, got %d", n)
	}
}

// TestWritesTakeFileMutex holds the file's write mutex and checks that every
// write waits for it and no read does.
func TestWritesTakeFileMutex(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	enqueueN(t, q, 10)
	_, _, ack1 := q.DequeueWithAckId()

	writes := []struct {
		name string
		op   func()
	}{
		{"Enqueue", func() { q.Enqueue([]byte("x")) }},
		{"Dequeue", func() { q.Dequeue() }},
		{"DequeueWithAckId", func() { q.DequeueWithAckId() }},
		{"DequeueBatchWithAckIds", func() { q.DequeueBatchWithAckIds(make([]any, 2), make([]string, 2)) }},
		{"Acknowledge", func() { q.Acknowledge(ack1) }},
		{"CancelJob", func() { q.CancelJob("x") }},
		{"gated DequeueWithAckId", func() { q.DequeueWithAckId() }},
		{"gated Dequeue", func() { q.Dequeue() }},
		{"requeueNoAck", func() { q.requeueNoAck() }},
		{"Purge", func() { q.Purge() }},
	}
	for _, w := range writes {
		t.Run(w.name, func(t *testing.T) {
			q.f.mu.Lock()
			done := make(chan struct{})
			go func() {
				w.op()
				close(done)
			}()
			select {
			case <-done:
				q.f.mu.Unlock()
				t.Fatalf("%s ran while the write mutex was held", w.name)
			case <-time.After(100 * time.Millisecond):
			}
			q.f.mu.Unlock()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("%s did not finish after the mutex was released", w.name)
			}
		})
	}

	reads := []struct {
		name string
		op   func()
	}{
		{"Len", func() { q.Len() }},
		{"Values", func() { q.Values() }},
	}
	for _, r := range reads {
		t.Run(r.name, func(t *testing.T) {
			q.f.mu.Lock()
			defer q.f.mu.Unlock()
			done := make(chan struct{})
			go func() {
				r.op()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("%s waited for the write mutex", r.name)
			}
		})
	}
}

// otherProcess opens q's table again on a pool of its own, as a second
// process on the same database file would.
func otherProcess(t *testing.T, q *Queue) *Queue {
	t.Helper()
	f, err := newFile(q.f.key, strings.TrimPrefix(q.f.scope, "sqliteq:"), q.f.pages, q.f.hb)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.release() })
	h, err := f.open(q.name, q.priority, []Option{WithRemoveOnComplete(q.removeOnComplete)})
	if err != nil {
		t.Fatal(err)
	}
	return h.queue()
}

// job is a varmq-style envelope with Job ID id.
func job(id string) []byte { return []byte(`{"id":"` + id + `","data":1}`) }

// watch starts a watch of id on q; the channel gets its one outcome.
func watch(q *Queue, id string) <-chan contract.JobOutcome {
	ch := make(chan contract.JobOutcome, 2)
	q.WatchOutcome(id, func(o contract.JobOutcome) { ch <- o })
	return ch
}

func waitOutcome(t *testing.T, ch <-chan contract.JobOutcome) contract.JobOutcome {
	t.Helper()
	select {
	case o := <-ch:
		return o
	case <-time.After(5 * time.Second):
		t.Fatal("no outcome")
		return contract.JobOutcome{}
	}
}

func noOutcome(t *testing.T, ch <-chan contract.JobOutcome) {
	t.Helper()
	select {
	case o := <-ch:
		t.Fatalf("unexpected outcome %+v", o)
	case <-time.After(4 * pollEvery):
	}
}

// pop hands out the next item and its ack id, failing on none.
func pop(t *testing.T, q *Queue) (string, string) {
	t.Helper()
	v, ok, ack := q.DequeueWithAckId()
	if !ok {
		t.Fatal("DequeueWithAckId found nothing")
	}
	return string(v.([]byte)), ack
}

// A watch reports each way a job ends: Acknowledge, Reject with its reason,
// AcknowledgeResult with its JSON; a job without a Job ID writes nothing.
func TestWatchOutcome(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()

	cases := []struct {
		id     string
		finish func(ack string) bool
		want   contract.JobOutcome
	}{
		{"ack", q.Acknowledge, contract.JobOutcome{ID: "ack", State: contract.JobCompleted}},
		{"rej", func(a string) bool { return q.Reject(a, "boom") }, contract.JobOutcome{ID: "rej", State: contract.JobFailed, Error: "boom"}},
		{"res", func(a string) bool { return q.AcknowledgeResult(a, []byte(`{"n":42}`)) },
			contract.JobOutcome{ID: "res", State: contract.JobCompleted, Result: []byte(`{"n":42}`)}},
	}
	for _, c := range cases {
		ch := watch(q, c.id)
		q.Enqueue(job(c.id))
		_, ack := pop(t, q)
		noOutcome(t, ch)
		if !c.finish(ack) {
			t.Fatalf("%s: finish failed", c.id)
		}
		got := waitOutcome(t, ch)
		if got.ID != c.want.ID || got.State != c.want.State || got.Error != c.want.Error || string(got.Result) != string(c.want.Result) {
			t.Errorf("%s: got %+v, want %+v", c.id, got, c.want)
		}
		noOutcome(t, ch) // once
	}

	q.Enqueue([]byte("plain"))
	_, ack := pop(t, q)
	q.Acknowledge(ack)
	if n := countJobs(t, q); n != 3 {
		t.Errorf("Expected 3 outcome records, got %d", n)
	}
}

func countJobs(t *testing.T, q *Queue) int {
	t.Helper()
	var n int
	if err := q.f.db.QueryRow("SELECT COUNT(*) FROM " + q.jobs).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// AcknowledgeResult with invalid JSON fails and changes nothing.
func TestAcknowledgeResultInvalid(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	q.Enqueue(job("a"))
	_, ack := pop(t, q)
	if q.AcknowledgeResult(ack, []byte(`{nope`)) {
		t.Fatal("Expected invalid JSON to fail")
	}
	if n := countRows(t, q.f.db, "status = 'processing'"); n != 1 {
		t.Errorf("Expected the row still processing, got %d", n)
	}
	if !q.Acknowledge(ack) {
		t.Error("Expected Acknowledge to finish it")
	}
}

// A job that ended before the watch is reported from its record; cancel
// stops a watch; a closed queue watches nothing.
func TestWatchEndedBeforeAndCancel(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	q.Enqueue(job("early"))
	_, ack := pop(t, q)
	q.Reject(ack, "late watcher")
	if got := waitOutcome(t, watch(q, "early")); got.State != contract.JobFailed || got.Error != "late watcher" {
		t.Errorf("got %+v", got)
	}

	ch := make(chan contract.JobOutcome, 1)
	stop := q.WatchOutcome("never", func(o contract.JobOutcome) { ch <- o })
	stop()
	stop()
	q.Enqueue(job("never"))
	_, ack = pop(t, q)
	q.Acknowledge(ack)
	noOutcome(t, ch)

	q.Close()
	q.WatchOutcome("early", func(o contract.JobOutcome) { ch <- o })
	noOutcome(t, ch)
	if err := q.CancelJob("x"); !errors.Is(err, errClosed) {
		t.Errorf("CancelJob on a closed queue: got %v, want errClosed", err)
	}
}

// Two queue objects on one file, as two processes: each learns of jobs the
// other finishes, and a cancel reaches the other's pops and OnCancel.
func TestOutcomeAcrossProcesses(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	q2 := otherProcess(t, q)

	ch := watch(q, "x")
	q.Enqueue(job("x"))
	_, ack := pop(t, q2)
	q2.Acknowledge(ack)
	if got := waitOutcome(t, ch); got.ID != "x" || got.State != contract.JobCompleted {
		t.Errorf("got %+v", got)
	}

	ids := make(chan string, 4)
	defer q2.OnCancel(func(id string) { ids <- id })()
	q2.Enqueue(job("run"))
	_, ack = pop(t, q2)
	ch = watch(q2, "run")
	if err := q.CancelJob("run"); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-ids:
		if id != "run" {
			t.Errorf("OnCancel got %q", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnCancel did not fire")
	}
	if !q2.AcknowledgeCancelled(ack, "stopped") {
		t.Fatal("AcknowledgeCancelled failed")
	}
	if got := waitOutcome(t, ch); got.State != contract.JobCancelled || got.Error != "stopped" {
		t.Errorf("got %+v", got)
	}
	var marks int
	q.f.db.QueryRow("SELECT COUNT(*) FROM " + q.cancelled).Scan(&marks)
	if marks != 0 {
		t.Errorf("Expected the finish to clear the mark, %d left", marks)
	}
	if err := q.CancelJob("run"); !errors.Is(err, contract.ErrJobEnded) {
		t.Errorf("CancelJob of an ended job: got %v, want ErrJobEnded", err)
	}
}

// A marked waiting job is never handed out, by any pop: it is dropped with
// outcome cancelled and the pop goes on with the next item.
func TestCancelWaiting(t *testing.T) {
	q, qs := openQueue(t, WithRemoveOnComplete(false))
	defer qs.Close()

	for _, id := range []string{"a", "b", "c", "d", "e"} {
		q.Enqueue(job(id))
	}
	q.EnqueueOnce([]byte("raw"), "f") // a Job ID from EnqueueOnce, not JSON
	q.Enqueue([]byte("g"))
	chA, chF := watch(q, "a"), watch(q, "f")
	for _, id := range []string{"a", "c", "d", "f"} {
		if err := q.CancelJob(id); err != nil {
			t.Fatal(err)
		}
	}

	if v, _ := pop(t, q); v != string(job("b")) {
		t.Errorf("DequeueWithAckId: got %s, want b", v)
	}
	items, acks := make([]any, 2), make([]string, 2)
	n, _, err := q.DequeueBatchWithAckIds(items, acks)
	if err != nil || n != 2 || string(items[0].([]byte)) != string(job("e")) || string(items[1].([]byte)) != "g" {
		t.Errorf("DequeueBatchWithAckIds: got %d %v %v", n, err, items[:n])
	}
	if got := waitOutcome(t, chA); got.State != contract.JobCancelled || got.Error != "cancelled" {
		t.Errorf("got %+v", got)
	}
	if got := waitOutcome(t, chF); got.State != contract.JobCancelled {
		t.Errorf("got %+v", got)
	}
	if n := countRows(t, q.f.db, "status = 'cancelled'"); n != 4 {
		t.Errorf("Expected 4 kept cancelled rows, got %d", n)
	}
	if _, ok, _ := q.DequeueWithAckId(); ok {
		t.Error("Expected nothing left")
	}

	// Dequeue without ack drops too
	q.Enqueue(job("h"))
	q.Enqueue(job("i"))
	q.CancelJob("h")
	if v, ok := q.Dequeue(); !ok || string(v.([]byte)) != string(job("i")) {
		t.Errorf("Dequeue: got %v %v, want i", v, ok)
	}
	if got := waitOutcome(t, watch(q, "h")); got.State != contract.JobCancelled {
		t.Errorf("got %+v", got)
	}
}

// EnqueueOnce that stores an id drops its old outcome and its cancel mark
// (spec 6.13, 6.14 Re-add); a dedupe hit changes neither.
func TestReaddClearsOutcomeAndMark(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()

	q.EnqueueOnce(job("r"), "r")
	_, ack := pop(t, q)
	q.Acknowledge(ack)
	if countJobs(t, q) != 1 {
		t.Fatal("Expected a record")
	}
	q.EnqueueOnce(job("r"), "r")
	if countJobs(t, q) != 0 {
		t.Error("Expected the re-add to drop the old record")
	}
	ch := watch(q, "r")
	noOutcome(t, ch) // the earlier run is not reported

	if err := q.CancelJob("r"); err != nil {
		t.Fatal(err)
	}
	q.EnqueueOnce(job("r"), "r") // dedupe hit: the row is still there
	if _, ok, _ := q.DequeueWithAckId(); ok {
		t.Fatal("Expected the marked job dropped")
	}
	if got := waitOutcome(t, ch); got.State != contract.JobCancelled {
		t.Errorf("got %+v", got)
	}

	q.CancelJob("s") // unknown id: marked
	q.EnqueueOnce(job("s"), "s")
	if v, _ := pop(t, q); v != string(job("s")) {
		t.Errorf("Expected the re-added job handed out, got %s", v)
	}
}

// Purge drops outcome records and cancel marks with the items.
func TestPurgeClearsOutcomesAndMarks(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	q.Enqueue(job("done"))
	_, ack := pop(t, q)
	q.Acknowledge(ack)
	q.CancelJob("later")
	q.Purge()
	var marks int
	q.f.db.QueryRow("SELECT COUNT(*) FROM " + q.cancelled).Scan(&marks)
	if countJobs(t, q) != 0 || marks != 0 {
		t.Errorf("Expected Purge to clear records and marks, got %d and %d", countJobs(t, q), marks)
	}
	q.Enqueue(job("later"))
	if v, _ := pop(t, q); v != string(job("later")) {
		t.Errorf("Expected the purged mark gone, got %s", v)
	}
}

// OnCancel reports marks made after it, not before; stop ends it; an
// expired mark neither reaches hooks nor drops items.
func TestOnCancel(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	q.CancelJob("old")
	ids := make(chan string, 4)
	stop := q.OnCancel(func(id string) { ids <- id })
	q.CancelJob("new")
	select {
	case id := <-ids:
		if id != "new" {
			t.Errorf("got %q, want new", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnCancel did not fire")
	}
	stop()
	q.CancelJob("after")
	select {
	case id := <-ids:
		t.Errorf("got %q after stop", id)
	case <-time.After(4 * pollEvery):
	}

	q.f.db.Exec("UPDATE "+q.cancelled+" SET expires_at = ? WHERE job_id = 'old'", time.Now().Add(-time.Second).UnixMilli())
	q.Enqueue(job("old"))
	if v, _ := pop(t, q); v != string(job("old")) {
		t.Errorf("Expected an expired mark to drop nothing, got %s", v)
	}
	if err := q.CancelJob(""); err == nil {
		t.Error("Expected an empty id to fail")
	}
}

// Retention trims each state group by age and count; the count limit
// spares records younger than trimFloor; a queue object's first record
// trims.
func TestOutcomeRetention(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	now := time.Now()
	insert := func(tx *sql.Tx, prefix string, n int, at time.Time) {
		for i := range n {
			if _, err := tx.Exec("INSERT INTO "+q.jobs+" (job_id, state, finished_at) VALUES (?, 'completed', ?)", prefix+strconv.Itoa(i), at.UnixMilli()+int64(i)); err != nil {
				t.Fatal(err)
			}
		}
	}

	tx, _ := q.f.db.Begin()
	insert(tx, "fresh", keepCompleted+5, now)
	if err := q.trim(tx, now); err != nil {
		t.Fatal(err)
	}
	tx.Commit()
	if n := countJobs(t, q); n != keepCompleted+5 {
		t.Fatalf("Expected records younger than trimFloor kept, got %d", n)
	}
	q.f.db.Exec("DELETE FROM " + q.jobs)

	tx, _ = q.f.db.Begin()
	insert(tx, "c", keepCompleted+5, now.Add(-2*trimFloor))
	tx.Exec("INSERT INTO "+q.jobs+" (job_id, state, finished_at) VALUES ('old', 'failed', ?)", now.Add(-keepFailedAge).UnixMilli())
	tx.Exec("INSERT INTO "+q.jobs+" (job_id, state, finished_at) VALUES ('new', 'cancelled', ?)", now.UnixMilli())
	if err := q.trim(tx, now); err != nil {
		t.Fatal(err)
	}
	tx.Commit()
	if n := countJobs(t, q); n != keepCompleted+1 {
		t.Errorf("Expected %d records, got %d", keepCompleted+1, n)
	}
	var oldest string
	q.f.db.QueryRow("SELECT job_id FROM " + q.jobs + " WHERE state = 'completed' ORDER BY finished_at LIMIT 1").Scan(&oldest)
	if oldest != "c5" {
		t.Errorf("Expected the 5 oldest completed trimmed, oldest left %s", oldest)
	}

	q2 := otherProcess(t, q) // records counter at 0
	q2.f.db.Exec("UPDATE "+q.jobs+" SET finished_at = ? WHERE job_id = 'c5'", now.Add(-keepCompletedAge).UnixMilli())
	q2.Enqueue(job("one"))
	_, ack := pop(t, q2)
	q2.Acknowledge(ack)
	var n int
	q.f.db.QueryRow("SELECT COUNT(*) FROM " + q.jobs + " WHERE job_id = 'c5'").Scan(&n)
	if n != 0 {
		t.Error("Expected a queue object's first record to trim")
	}
}

// varmq end to end: a result job added on one queue object runs on a
// worker bound to another (another process), and its handle gets the
// result; Cancel stops a running job and drops a waiting one.
func TestVarmqRemoteResultAndCancel(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	q2 := otherProcess(t, q)

	started := make(chan struct{})
	ran := make(chan int, 8)
	w := varmq.NewResultWorker(func(j varmq.Job[int]) (int, error) {
		ran <- j.Data()
		if j.Data() < 0 {
			close(started)
			<-j.Context().Done()
			return 0, j.Context().Err()
		}
		return j.Data() * 2, nil
	})
	defer w.Stop()

	p := varmq.NewResultQueue[int, int](q)
	waiting, ok := p.Add(7) // no worker yet
	if !ok {
		t.Fatal("Add failed")
	}
	if err := waiting.Cancel(); err != nil {
		t.Fatal(err)
	}
	w.WithQueue(q2)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	j, _ := p.Add(21)
	if err := j.WaitContext(ctx); err != nil {
		t.Fatal(err)
	}
	if r, err := j.Result(); err != nil || r != 42 {
		t.Errorf("Result: got %d %v, want 42", r, err)
	}
	if err := waiting.WaitContext(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := waiting.Result(); !errors.Is(err, varmq.ErrJobCancelled) {
		t.Errorf("waiting job: got %v, want ErrJobCancelled", err)
	}

	running, _ := p.Add(-1)
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("job did not start")
	}
	if err := running.Cancel(); err != nil {
		t.Fatal(err)
	}
	if err := running.WaitContext(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := running.Result(); !errors.Is(err, varmq.ErrJobCancelled) {
		t.Errorf("running job: got %v, want ErrJobCancelled", err)
	}
	close(ran)
	for v := range ran {
		if v == 7 {
			t.Error("The cancelled waiting job ran")
		}
	}
}

// A pop drops at most maxDrops marked items; at the cap it reports items
// left, and the next pop goes on.
func TestCancelDropCap(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	for i := range maxDrops {
		id := "m" + strconv.Itoa(i)
		q.Enqueue(job(id))
		q.CancelJob(id)
	}
	q.Enqueue([]byte("plain"))

	items, acks := make([]any, 1), make([]string, 1)
	if n, left, err := q.DequeueBatchWithAckIds(items, acks); err != nil || n != 0 || left != 1 {
		t.Fatalf("at the cap: got n=%d left=%d err=%v, want 0, 1", n, left, err)
	}
	if v, _ := pop(t, q); v != "plain" {
		t.Errorf("got %s, want plain", v)
	}
	if n := countJobs(t, q); n != maxDrops {
		t.Errorf("Expected %d cancelled records, got %d", maxDrops, n)
	}

	// Single-item pops can't report items left: past the cap they pop again
	for _, p := range []struct {
		name string
		pop  func() (any, bool)
	}{
		{"DequeueWithAckId", func() (any, bool) { v, ok, _ := q.DequeueWithAckId(); return v, ok }},
		{"Dequeue", q.Dequeue},
	} {
		for i := range maxDrops + 1 {
			id := p.name + strconv.Itoa(i)
			q.Enqueue(job(id))
			q.CancelJob(id)
		}
		q.Enqueue([]byte("plain"))
		if v, ok := p.pop(); !ok || string(v.([]byte)) != "plain" {
			t.Errorf("%s past the cap: got %v %v, want plain", p.name, v, ok)
		}
	}
}

// sweep trims records past their age and drops expired marks.
func TestSweep(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	old := time.Now().Add(-keepFailedAge).UnixMilli()
	q.f.db.Exec("INSERT INTO "+q.jobs+" (job_id, state, finished_at) VALUES ('old', 'failed', ?)", old)
	q.f.db.Exec("INSERT INTO "+q.jobs+" (job_id, state, finished_at) VALUES ('new', 'failed', ?)", time.Now().UnixMilli())
	q.f.db.Exec("INSERT INTO "+q.cancelled+" (job_id, expires_at) VALUES ('gone', ?)", old)
	q.CancelJob("live")
	q.sweep()
	var marks int
	q.f.db.QueryRow("SELECT COUNT(*) FROM " + q.cancelled).Scan(&marks)
	if countJobs(t, q) != 1 || marks != 1 {
		t.Errorf("Expected 1 record and 1 mark left, got %d and %d", countJobs(t, q), marks)
	}
}

// Queues.Close stops polls and drops live watches and hooks without
// calling them.
func TestCloseDropsWatchesAndHooks(t *testing.T) {
	q, qs := openQueue(t)
	q2 := otherProcess(t, q)
	ch := watch(q, "w")
	ids := make(chan string, 1)
	q.OnCancel(func(id string) { ids <- id })
	if err := qs.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-q.w.stop:
	default:
		t.Fatal("Expected Close to stop the poll")
	}
	q2.Enqueue(job("w"))
	_, ack := pop(t, q2)
	q2.Acknowledge(ack)
	q2.CancelJob("c")
	noOutcome(t, ch)
	select {
	case id := <-ids:
		t.Errorf("hook called after Close with %q", id)
	default:
	}
	if stop := q.OnCancel(func(string) { t.Error("hook on a closed queue") }); stop == nil {
		t.Error("Expected a stop func")
	}
}

// A hook added while a poll reads gets the marks made after its call; a
// stopped hook gets none; a mark whose job finished (stale) reaches nobody.
func TestOnCancelHooksDuringPoll(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	a, b := make(chan string, 4), make(chan string, 4)
	stopA := q.OnCancel(func(id string) { a <- id })
	q.w.mu.Lock()
	seq := q.w.lastSeq // a poll's snapshot, taken before B
	q.w.mu.Unlock()
	q.CancelJob("before-b")
	stopB := q.OnCancel(func(id string) { b <- id })
	q.CancelJob("x")
	q.pollCancels(seq)

	// The queue's own poll may report a mark too: compare distinct ids
	got := func(ch chan string) []string {
		var ids []string
		for {
			select {
			case id := <-ch:
				ids = append(ids, id)
			case <-time.After(4 * pollEvery):
				slices.Sort(ids)
				return slices.Compact(ids)
			}
		}
	}
	if ids := got(a); !slices.Equal(ids, []string{"before-b", "x"}) {
		t.Errorf("hook A: got %v", ids)
	}
	if ids := got(b); !slices.Equal(ids, []string{"x"}) {
		t.Errorf("hook B: got %v, want only the mark after its call", ids)
	}

	var xSeq int64
	q.f.db.QueryRow("SELECT seq FROM " + q.cancelled + " WHERE job_id = 'x'").Scan(&xSeq)
	stopA()
	q.Enqueue(job("y"))
	_, ack := pop(t, q)
	q.CancelJob("y")
	q.Acknowledge(ack) // the finish drops the mark: stale
	q.CancelJob("z")
	q.pollCancels(xSeq)
	if ids := got(a); len(ids) != 0 {
		t.Errorf("stopped hook got %v", ids)
	}
	if ids := got(b); !slices.Equal(ids, []string{"z"}) {
		t.Errorf("hook B: got %v, want z only (y is stale)", ids)
	}
	stopB()
}

// A repeat CancelJob gets a new seq and expiry, so hooks hear it again.
func TestCancelRepeatRenotifies(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	ids := make(chan string, 4)
	defer q.OnCancel(func(id string) { ids <- id })()
	read := func() (seq, exp int64) {
		q.f.db.QueryRow("SELECT seq, expires_at FROM "+q.cancelled+" WHERE job_id = 'x'").Scan(&seq, &exp)
		return
	}
	for i := range 2 {
		q.CancelJob("x")
		select {
		case <-ids:
		case <-time.After(5 * time.Second):
			t.Fatalf("cancel %d not reported", i+1)
		}
		if i == 0 {
			s1, e1 := read()
			time.Sleep(2 * time.Millisecond)
			defer func() {
				if s2, e2 := read(); s2 <= s1 || e2 <= e1 {
					t.Errorf("Expected a new seq and expiry: %d,%d -> %d,%d", s1, e1, s2, e2)
				}
			}()
		}
	}
}

// A gated Dequeue that drops the last item returns empty and keeps the
// drop; the mark is gone after it.
func TestDequeueGatedEmpty(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	q.Enqueue(job("x"))
	q.CancelJob("x")
	ch := watch(q, "x")
	if v, ok := q.Dequeue(); ok {
		t.Fatalf("got %v", v)
	}
	if got := waitOutcome(t, ch); got.State != contract.JobCancelled {
		t.Errorf("got %+v", got)
	}
	var marks int
	q.f.db.QueryRow("SELECT COUNT(*) FROM " + q.cancelled).Scan(&marks)
	if q.Len() != 0 || marks != 0 {
		t.Errorf("Expected no item and no mark, got %d and %d", q.Len(), marks)
	}
}

// With WithRemoveOnComplete(false) each end keeps its row with its status,
// and the record keeps the result.
func TestKeptRowEnds(t *testing.T) {
	q, qs := openQueue(t, WithRemoveOnComplete(false))
	defer qs.Close()
	q.Enqueue(job("r"))
	q.Enqueue(job("c"))
	_, ackR := pop(t, q)
	_, ackC := pop(t, q)
	if !q.AcknowledgeResult(ackR, []byte(`"ok"`)) || !q.AcknowledgeCancelled(ackC, "stop") {
		t.Fatal("finish failed")
	}
	if countRows(t, q.f.db, "status = 'completed'") != 1 || countRows(t, q.f.db, "status = 'cancelled'") != 1 {
		t.Error("Expected one completed and one cancelled row")
	}
	if got := waitOutcome(t, watch(q, "r")); string(got.Result) != `"ok"` {
		t.Errorf("got %+v", got)
	}
	if got := waitOutcome(t, watch(q, "c")); got.State != contract.JobCancelled || got.Error != "stop" {
		t.Errorf("got %+v", got)
	}
}

// A read reaches only watches started before it: an earlier run's record,
// read before a re-add, never ends a watch made after.
func TestDeliverSkipsNewerWatches(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	ch := watch(q, "x")
	q.w.mu.Lock()
	gen := q.w.gen
	q.w.mu.Unlock()
	q.deliver(contract.JobOutcome{ID: "x", State: contract.JobCompleted}, gen-1)
	noOutcome(t, ch)
	q.deliver(contract.JobOutcome{ID: "x", State: contract.JobCompleted}, gen)
	waitOutcome(t, ch)
}

// Every watch fires when far more jobs end between two polls than
// retention keeps, and more ids are watched than one read used to take.
func TestWatchBurst(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	q2 := otherProcess(t, q)
	const n = 3 * keepCompleted
	var fired atomic.Int64
	done := make(chan struct{})
	for i := range n {
		id := "b" + strconv.Itoa(i)
		q.WatchOutcome(id, func(contract.JobOutcome) {
			if fired.Add(1) == n {
				close(done)
			}
		})
		q.Enqueue(job(id))
	}
	items, acks := make([]any, 500), make([]string, 500)
	for {
		k, _, err := q2.DequeueBatchWithAckIds(items, acks)
		if err != nil || k == 0 {
			break
		}
		for _, a := range acks[:k] {
			q2.Acknowledge(a)
		}
	}
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatalf("%d of %d watches fired", fired.Load(), n)
	}
}

// Queue names may not hold __sqliteq_, which names a queue's own tables.
func TestReservedQueueName(t *testing.T) {
	qs := mustNew(t, filepath.Join(t.TempDir(), "one.db"), WithSingleFile())
	defer qs.Close()
	if _, err := qs.NewQueue("orders__sqliteq_jobs"); err == nil {
		t.Error("Expected a reserved name to fail")
	}
}

// An item whose envelope does not decode to a string id is a plain job:
// with a live mark (gated pops) it is handed out, finishes as usual, and
// writes no record.
func TestUndecodableEnvelopeIsPlainJob(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	q.CancelJob("other") // keeps the pops gated
	bad := []string{`{bad`, `{"id":5}`, `{"id":null}`, `{"id":""}`, `[1]`, "plain", `{"id":"x"`}
	for _, b := range bad {
		q.Enqueue([]byte(b))
	}
	for i, b := range bad {
		v, ack := pop(t, q)
		if v != b {
			t.Fatalf("pop %d: got %q, want %q", i, v, b)
		}
		if i%2 == 0 {
			q.Acknowledge(ack)
		} else {
			q.Reject(ack, "r")
		}
	}
	if n := countJobs(t, q); n != 0 {
		t.Errorf("Expected no records, got %d", n)
	}
}

// QueueID is sqliteq:<8 hex of the file's path>:fifo|prio:<name in lower
// case>: one file agrees in every process, two files never collide.
func TestQueueID(t *testing.T) {
	dir := t.TempDir()
	qs := mustNew(t, dir)
	defer qs.Close()
	q, _ := qs.NewQueue("Jobs")
	pq, _ := qs.NewPriorityQueue("Prio")
	sum := sha256.Sum256([]byte(q.f.key))
	if want := "sqliteq:" + hex.EncodeToString(sum[:4]) + ":fifo:jobs"; q.QueueID() != want {
		t.Errorf("got %q, want %q", q.QueueID(), want)
	}
	if id := pq.QueueID(); !strings.HasPrefix(id, "sqliteq:") || !strings.HasSuffix(id, ":prio:prio") {
		t.Errorf("priority queue id %q", id)
	}
	if otherProcess(t, q).QueueID() != q.QueueID() {
		t.Error("Expected one id for one file in every process")
	}

	// Same table name, other file
	single := mustNew(t, filepath.Join(dir, "single.db"), WithSingleFile())
	defer single.Close()
	q2, _ := single.NewQueue("jobs")
	if q2.QueueID() == q.QueueID() {
		t.Error("Expected two files to have two ids")
	}
	if strings.Contains(q2.QueueID(), dir) {
		t.Error("Expected no path in the id")
	}
}

// stats reads QueueStats, failing on an error.
func stats(t *testing.T, q *Queue) contract.QueueStats {
	t.Helper()
	s, err := q.QueueStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// QueueStats counts rows per state (cancelled under failed), totals every
// add and finish, and survives no Purge; the default deletes ended rows.
func TestQueueStats(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(fmt.Sprintf("keep=%t", keep), func(t *testing.T) {
			q, qs := openQueue(t, WithRemoveOnComplete(!keep))
			defer qs.Close()
			for _, id := range []string{"a", "b", "c", "d", "e"} {
				q.Enqueue(job(id))
			}
			q.EnqueueOnce(job("f"), "f")
			q.EnqueueOnce(job("f"), "f") // repeat: not submitted
			q.CancelJob("d")

			_, a := pop(t, q)
			_, b := pop(t, q)
			_, c := pop(t, q)
			pop(t, q) // drops d (cancelled, in no total), hands out e
			q.Acknowledge(a)
			q.Reject(b, "boom")
			q.AcknowledgeCancelled(c, "stop")

			kept := int64(0)
			if keep {
				kept = 1
			}
			want := contract.QueueStats{
				Counts: contract.JobCounts{Pending: 1, Processing: 1, Completed: kept, Failed: 3 * kept},
				Totals: contract.JobTotals{Submitted: 6, Completed: 1, Failed: 2},
			}
			if got := stats(t, q); got != want {
				t.Errorf("got %+v, want %+v", got, want)
			}
			if got := stats(t, otherProcess(t, q)); got != want {
				t.Errorf("other process: got %+v, want %+v", got, want)
			}

			q.Purge()
			if got := stats(t, q); got != (contract.QueueStats{}) {
				t.Errorf("after Purge: got %+v", got)
			}
		})
	}
}

// jobs reads one page of Jobs, failing on an error.
func jobs(t *testing.T, q *Queue, state contract.JobState, offset, limit int) []contract.JobRecord {
	t.Helper()
	j, err := q.Jobs(context.Background(), state, offset, limit)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func ids(js []contract.JobRecord) []string {
	out := make([]string, len(js))
	for i, j := range js {
		out[i] = j.ID
	}
	return out
}

// Jobs pages each state: pending in hand-out order, processing by ack id,
// completed and failed (with cancelled) newest first; payload is the
// envelope's data, verbatim.
func TestJobs(t *testing.T) {
	q, qs := openQueue(t, WithRemoveOnComplete(false))
	defer qs.Close()
	before := time.Now().UnixMilli()
	q.Enqueue([]byte(`{"id":"a","status":"Created","data":{"n":1.000000000000001},"enqueued_at":7}`))
	q.Enqueue([]byte("plain"))
	q.Enqueue([]byte(`[1,2]`))
	for _, id := range []string{"b", "c", "d", "e", "f"} {
		q.Enqueue(job(id))
	}
	if got := ids(jobs(t, q, contract.JobPending, 0, 100)); !slices.Equal(got, []string{"a", "", "", "b", "c", "d", "e", "f"}) {
		t.Fatalf("pending: got %q", got)
	}
	page := jobs(t, q, contract.JobPending, 0, 3)
	if string(page[0].Payload) != `{"n":1.000000000000001}` || page[0].EnqueuedAt != 7 || page[0].Attempts != 0 || page[0].StartedAt != 0 {
		t.Errorf("envelope: got %+v", page[0])
	}
	if string(page[1].Payload) != `"plain"` || page[1].EnqueuedAt < before {
		t.Errorf("plain bytes: got %+v (enqueued_at from the row)", page[1])
	}
	if string(page[2].Payload) != `[1,2]` {
		t.Errorf("JSON item: got %s", page[2].Payload)
	}
	if got := ids(jobs(t, q, contract.JobPending, 6, 5)); !slices.Equal(got, []string{"e", "f"}) {
		t.Errorf("offset 6: got %q", got)
	}

	acks := map[string]string{}
	for range 7 {
		v, ack := pop(t, q)
		acks[v] = ack
	}
	plainAck := acks["plain"]
	q.Acknowledge(acks[`{"id":"a","status":"Created","data":{"n":1.000000000000001},"enqueued_at":7}`])
	q.Reject(acks["plain"], "no id")
	q.Reject(acks[string(job("b"))], "boom")
	q.AcknowledgeCancelled(acks[string(job("c"))], "stopped")
	q.Acknowledge(acks[string(job("d"))])

	// A running job without an id shows its ack id
	proc := jobs(t, q, contract.JobProcessing, 0, 10)
	if got := ids(proc); !slices.Equal(got, []string{acks["[1,2]"], "e"}) {
		t.Errorf("processing: got %q", got)
	}
	for _, j := range proc {
		if j.State != contract.JobProcessing || j.Attempts != 1 || j.StartedAt < before || j.FinishedAt != 0 {
			t.Errorf("processing job: got %+v", j)
		}
	}

	if got := ids(jobs(t, q, contract.JobCompleted, 0, 10)); !slices.Equal(got, []string{"d", "a"}) {
		t.Errorf("completed: got %q", got)
	}
	failed := jobs(t, q, contract.JobFailed, 0, 10)
	if got := ids(failed); !slices.Equal(got, []string{"c", "b", plainAck}) {
		t.Fatalf("failed: got %q", got)
	}
	if failed[0].State != contract.JobCancelled || len(failed[0].Errors) != 1 || failed[0].Errors[0].Error != "stopped" {
		t.Errorf("cancelled: got %+v", failed[0])
	}
	if b := failed[1]; b.State != contract.JobFailed || b.FinishedAt < before || b.Attempts != 1 ||
		!slices.Equal(b.Errors, []contract.JobError{{Attempt: 1, At: b.FinishedAt, Error: "boom"}}) {
		t.Errorf("failed: got %+v", b)
	}
	if len(failed[2].Errors) != 0 {
		t.Errorf("Expected no errors without an outcome record, got %+v", failed[2].Errors)
	}
	if got := ids(jobs(t, q, contract.JobFailed, 1, 1)); !slices.Equal(got, []string{"b"}) {
		t.Errorf("failed page 2: got %q", got)
	}

	if j := jobs(t, q, contract.JobPending, 0, 0); j == nil || len(j) != 0 {
		t.Errorf("limit 0: got %#v", j)
	}
	if j := jobs(t, q, contract.JobPending, 0, -1); j == nil || len(j) != 0 {
		t.Errorf("limit -1: got %#v", j)
	}
	if _, err := q.Jobs(context.Background(), "bogus", 0, 10); err == nil {
		t.Error("Expected an unknown state to fail")
	}
}

// With the default WithRemoveOnComplete(true) ended jobs are not kept, so
// their pages are empty.
func TestJobsRemoveOnComplete(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	q.Enqueue(job("a"))
	_, ack := pop(t, q)
	q.Acknowledge(ack)
	if j := jobs(t, q, contract.JobCompleted, 0, 10); len(j) != 0 {
		t.Errorf("got %+v", j)
	}
}

// A closed queue answers stats and jobs with an error, as varmq lists it.
func TestInspectClosed(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	q.Close()
	if _, err := q.QueueStats(context.Background()); err == nil {
		t.Error("Expected QueueStats to fail on a closed queue")
	}
	if _, err := q.Jobs(context.Background(), contract.JobPending, 0, 1); err == nil {
		t.Error("Expected Jobs to fail on a closed queue")
	}
}

func TestImplementsPause(t *testing.T) {
	var (
		_ varmq.QueuePauser   = (*Queue)(nil)
		_ varmq.QueuePauser   = (*PriorityQueue)(nil)
		_ varmq.PauseNotifier = (*Queue)(nil)
		_ varmq.PauseNotifier = (*PriorityQueue)(nil)
	)
}

// refusedPops checks that every pop kind hands out nothing and reports a
// pause.
func refusedPops(t *testing.T, q *Queue) {
	t.Helper()
	before := q.PauseRefusals()
	if _, ok := q.Dequeue(); ok {
		t.Error("Dequeue handed out an item while paused")
	}
	if _, ok, _ := q.DequeueWithAckId(); ok {
		t.Error("DequeueWithAckId handed out an item while paused")
	}
	items, acks := make([]any, 4), make([]string, 4)
	if n, rem, err := q.DequeueBatchWithAckIds(items, acks); n != 0 || rem != 0 || err != nil {
		t.Errorf("batch pop while paused: got %d %d %v", n, rem, err)
	}
	if got := q.PauseRefusals() - before; got != 3 {
		t.Errorf("Expected 3 refusals, got %d", got)
	}
	if !q.Paused() {
		t.Error("Expected a refused pop to leave Paused true")
	}
}

// Pause stops every pop, keeps its first time, and is kept in the file;
// Resume lets pops go on. Hooks hear each change once.
func TestPause(t *testing.T) {
	for _, marked := range []bool{false, true} {
		t.Run(fmt.Sprintf("marked=%t", marked), func(t *testing.T) {
			q, qs := openQueue(t)
			defer qs.Close()
			if marked {
				q.CancelJob("other") // gated pops
			}
			var calls []bool
			var mu sync.Mutex
			stop := q.OnPauseChange(func(p bool) {
				mu.Lock()
				calls = append(calls, p)
				mu.Unlock()
			})
			defer stop()
			enqueueN(t, q, 3)

			if q.Paused() {
				t.Fatal("Expected a new queue to run")
			}
			if err := q.Pause(); err != nil {
				t.Fatal(err)
			}
			first := stats(t, q)
			if !q.Paused() || !first.Paused || first.PausedAt == 0 {
				t.Fatalf("after Pause: Paused %t, stats %+v", q.Paused(), first)
			}
			time.Sleep(2 * time.Millisecond)
			if err := q.Pause(); err != nil {
				t.Fatal(err)
			}
			if s := stats(t, q); s.PausedAt != first.PausedAt {
				t.Errorf("Expected a second Pause to keep %d, got %d", first.PausedAt, s.PausedAt)
			}
			refusedPops(t, q)
			if !q.Enqueue([]byte("added while paused")) || q.Len() != 4 {
				t.Error("Expected adds to land while paused")
			}

			if err := q.Resume(); err != nil {
				t.Fatal(err)
			}
			if err := q.Resume(); err != nil {
				t.Fatal(err)
			}
			if q.Paused() || stats(t, q).Paused {
				t.Error("Expected Resume to clear the pause")
			}
			if _, ok := q.Dequeue(); !ok {
				t.Error("Expected pops to go on after Resume")
			}
			mu.Lock()
			defer mu.Unlock()
			if !slices.Equal(calls, []bool{true, false}) {
				t.Errorf("hooks: got %v", calls)
			}
		})
	}
}

// waitPaused waits for q.Paused to become want.
func waitPaused(t *testing.T, q *Queue, want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for q.Paused() != want {
		if time.Now().After(deadline) {
			t.Fatalf("Paused stayed %t", !want)
		}
		time.Sleep(pollEvery / 5)
	}
}

// A pause from another process stops this one's pops at once; its Resume
// reaches this object's Paused and hooks by the poll.
func TestPauseAcrossProcesses(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	other := otherProcess(t, q)
	resumed := make(chan struct{}, 1)
	defer q.OnPauseChange(func(p bool) {
		if !p {
			resumed <- struct{}{}
		}
	})()
	enqueueN(t, q, 2)

	if err := other.Pause(); err != nil {
		t.Fatal(err)
	}
	if q.Paused() {
		t.Fatal("Expected the cache to wait for a pop or the poll")
	}
	if !stats(t, q).Paused {
		t.Error("Expected QueueStats to read the pause from the file")
	}
	refusedPops(t, q)

	if err := other.Resume(); err != nil {
		t.Fatal(err)
	}
	waitPaused(t, q, false)
	select {
	case <-resumed:
	case <-time.After(5 * time.Second):
		t.Fatal("no resume hook")
	}
	if _, ok := q.Dequeue(); !ok {
		t.Error("Expected pops to go on after the other process resumed")
	}
}

// A pause is kept in the file: it outlives the process, and Purge keeps it.
func TestPauseSurvivesReopenAndPurge(t *testing.T) {
	dir := t.TempDir()
	qs := mustNew(t, dir)
	q, _ := qs.NewQueue("q")
	q.Enqueue([]byte("x"))
	if err := q.Pause(); err != nil {
		t.Fatal(err)
	}
	at := stats(t, q).PausedAt
	q.Purge()
	if s := stats(t, q); s.PausedAt != at || s.Totals != (contract.JobTotals{}) {
		t.Errorf("after Purge: got %+v, want paused_at %d and no totals", s, at)
	}
	q.Enqueue([]byte("y"))
	qs.Close()

	qs = mustNew(t, dir)
	defer qs.Close()
	q, _ = qs.NewQueue("q")
	if !q.Paused() || !stats(t, q).Paused {
		t.Fatal("Expected the pause to outlive the process")
	}
	refusedPops(t, q)
	q.Resume()
	if v, ok := q.Dequeue(); !ok || string(v.([]byte)) != "y" {
		t.Errorf("got %v %t, want y", v, ok)
	}
	q.Close()
	if q.Pause() == nil || q.Resume() == nil {
		t.Error("Expected Pause and Resume on a closed queue to fail")
	}
}

// varmq: a paused queue holds the worker in every process; a Resume from
// another process wakes it.
func TestVarmqPause(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	other := otherProcess(t, q)

	ran := make(chan int, 4)
	w := varmq.NewWorker(func(j varmq.Job[int]) { ran <- j.Data() })
	defer w.Stop()
	handle := w.WithQueue(q)
	if err := handle.Pause(); err != nil {
		t.Fatal(err)
	}
	if !handle.IsPaused() {
		t.Fatal("Expected the handle to report the pause")
	}
	handle.Add(1)
	select {
	case v := <-ran:
		t.Fatalf("job %d ran while paused", v)
	case <-time.After(10 * pollEvery):
	}
	if err := other.Resume(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("job did not run after the other process resumed")
	}
}

// An older table's totals start from the rows it holds.
func TestTotalsSeededFromRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	writeV023File(t, path, "q", false)
	qs := mustNew(t, path, WithSingleFile())
	defer qs.Close()
	q, err := qs.NewQueue("q")
	if err != nil {
		t.Fatal(err)
	}
	var rows int64
	q.f.db.QueryRow(`SELECT COUNT(*) FROM "q"`).Scan(&rows)
	if s := stats(t, q); rows == 0 || s.Totals.Submitted != uint64(rows) {
		t.Errorf("got %+v, want submitted %d", s.Totals, rows)
	}
}

// A stopped hook gets no calls; a refusal read before this object's own
// Resume does not undo it.
func TestPauseHookStopAndStaleRefusal(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	var calls []bool
	var mu sync.Mutex
	defer q.OnPauseChange(func(p bool) {
		mu.Lock()
		calls = append(calls, p)
		mu.Unlock()
	})()
	q.OnPauseChange(func(bool) { t.Error("A stopped hook was called") })()

	q.Pause()
	g := q.pz.gen.Load()
	q.Resume()
	q.refused(g)
	if q.Paused() {
		t.Error("Expected a stale refusal to leave the Resume")
	}
	mu.Lock()
	if !slices.Equal(calls, []bool{true, false}) {
		t.Errorf("hooks: got %v", calls)
	}
	mu.Unlock()

	// Racing: a refusal whose gen check passes must not apply after a
	// whole Resume ran
	q2, qs2 := openQueue(t)
	defer qs2.Close()
	for i := range 20000 {
		q2.Pause()
		g := q2.pz.gen.Load()
		done := make(chan struct{})
		go func() {
			q2.refused(g)
			close(done)
		}()
		q2.Resume()
		<-done
		if q2.Paused() {
			t.Fatalf("round %d: a stale refusal undid the Resume", i)
		}
	}
}

// A Resume from another process between an empty pop and its read of the
// pause row is not a failed pop: the pop goes again and hands out the item.
func TestResumeBetweenPopAndRead(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	other := otherProcess(t, q)
	t.Cleanup(func() { afterEmptyPop = func() {} })
	pops := []struct {
		name string
		pop  func() bool
	}{
		{"Dequeue", func() bool { _, ok := q.Dequeue(); return ok }},
		{"DequeueWithAckId", func() bool { _, ok, _ := q.DequeueWithAckId(); return ok }},
		{"DequeueBatchWithAckIds", func() bool {
			n, _, _ := q.DequeueBatchWithAckIds(make([]any, 2), make([]string, 2))
			return n == 1
		}},
	}
	for _, p := range pops {
		q.Enqueue([]byte(p.name))
		if err := other.Pause(); err != nil {
			t.Fatal(err)
		}
		afterEmptyPop = func() {
			afterEmptyPop = func() {}
			if err := other.Resume(); err != nil {
				t.Error(err)
			}
		}
		if !p.pop() {
			t.Errorf("%s: Expected the item after the Resume", p.name)
		}
	}
}

// Completed, failed and cancelled pages sort by finish, not by add.
func TestJobsNewestFinishFirst(t *testing.T) {
	q, qs := openQueue(t, WithRemoveOnComplete(false))
	defer qs.Close()
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		q.Enqueue(job(id))
	}
	acks := make([]string, 6)
	for i := range acks {
		_, acks[i] = pop(t, q)
	}
	q.Acknowledge(acks[1])
	q.Acknowledge(acks[0])
	q.Reject(acks[3], "x")
	q.Reject(acks[2], "x")
	q.AcknowledgeCancelled(acks[5], "x")
	q.AcknowledgeCancelled(acks[4], "y")
	if got := ids(jobs(t, q, contract.JobCompleted, 0, 10)); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("completed: got %q", got)
	}
	if got := ids(jobs(t, q, contract.JobFailed, 0, 10)); !slices.Equal(got, []string{"e", "f", "c", "d"}) {
		t.Errorf("failed: got %q", got)
	}
	cancelled := jobs(t, q, contract.JobCancelled, 0, 10)
	if got := ids(cancelled); !slices.Equal(got, []string{"e", "f"}) {
		t.Fatalf("cancelled: got %q", got)
	}
	if e := cancelled[0]; e.State != contract.JobCancelled || len(e.Errors) != 1 || e.Errors[0].Error != "y" {
		t.Errorf("cancelled job: got %+v", e)
	}
}

// Seeding counts kept ended rows too: cancelled as failed.
func TestTotalsSeededFromEndedRows(t *testing.T) {
	q, qs := openQueue(t, WithRemoveOnComplete(false))
	defer qs.Close()
	enqueueN(t, q, 4)
	_, a := pop(t, q)
	_, b := pop(t, q)
	_, c := pop(t, q)
	q.Acknowledge(a)
	q.Reject(b, "boom")
	q.AcknowledgeCancelled(c, "stop")
	if _, err := q.f.db.Exec("DROP TABLE " + q.meta); err != nil {
		t.Fatal(err)
	}
	want := contract.JobTotals{Submitted: 4, Completed: 1, Failed: 2}
	if got := stats(t, otherProcess(t, q)).Totals; got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

// A finish that ends no row (repeat, stale or unknown ack) counts nothing.
func TestTotalsIgnoreNoopFinish(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(fmt.Sprintf("keep=%t", keep), func(t *testing.T) {
			q, qs := openQueue(t, WithRemoveOnComplete(!keep))
			defer qs.Close()
			q.Enqueue(job("a"))
			_, ack := pop(t, q)
			if !q.Acknowledge(ack) {
				t.Fatal("Acknowledge failed")
			}
			if q.Acknowledge(ack) || q.Reject(ack, "x") || q.AcknowledgeCancelled(ack, "x") ||
				q.Acknowledge("garbage") || q.Reject("999", "x") {
				t.Error("Expected a finish of no processing row to return false")
			}
			if got, want := stats(t, q).Totals, (contract.JobTotals{Submitted: 1, Completed: 1}); got != want {
				t.Errorf("got %+v, want %+v", got, want)
			}
		})
	}
}

// wfEnv is a workflow store over fifo queues "a" and "b" and priority queue
// "p" in the folder layout (own workflow file) or, with single, one file.
type wfEnv struct {
	qs     Queues
	st     *WorkflowStore
	a, b   *Queue
	p      *PriorityQueue
	dir    string
	single bool
}

func newWFEnv(t *testing.T, single bool, opts ...WorkflowOption) *wfEnv {
	t.Helper()
	e := &wfEnv{single: single, dir: t.TempDir()}
	if single {
		e.qs = mustNew(t, filepath.Join(e.dir, "all.db"), WithSingleFile())
	} else {
		e.qs = mustNew(t, e.dir)
	}
	var err error
	if e.a, err = e.qs.NewQueue("a"); err != nil {
		t.Fatal(err)
	}
	if e.b, err = e.qs.NewQueue("b"); err != nil {
		t.Fatal(err)
	}
	if e.p, err = e.qs.NewPriorityQueue("p"); err != nil {
		t.Fatal(err)
	}
	if e.st, err = NewWorkflowStore(e.qs, opts...); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		e.st.Close()
		e.qs.Close()
	})
	return e
}

func eachLayout(t *testing.T, fn func(t *testing.T, single bool)) {
	for _, single := range []bool{false, true} {
		name := "folder"
		if single {
			name = "single"
		}
		t.Run(name, func(t *testing.T) { fn(t, single) })
	}
}

// wfItem is the envelope of task (or its undo) of workflow wf.
func wfItem(wf, task string, undo bool) []byte {
	id, extra := "wf:"+wf+":"+task, ""
	if undo {
		id, extra = id+":undo", `,"undo":true`
	}
	return []byte(`{"id":"` + id + `","status":"Created","data":1,"wf":"` + wf + `","task":"` + task + `"` + extra + `}`)
}

// planTask plans task name of workflow wf on queue q (a *Queue or *PriorityQueue).
func planTask(wf, name string, q any, deps ...string) contract.TaskPlan {
	return contract.TaskPlan{Name: name, JobID: "wf:" + wf + ":" + name, Queue: q, Deps: deps, Item: wfItem(wf, name, false)}
}

func withUndo(tp contract.TaskPlan, wf string, q any) contract.TaskPlan {
	tp.Undo = &contract.UndoPlan{JobID: tp.JobID + ":undo", Queue: q, Item: wfItem(wf, tp.Name, true)}
	return tp
}

func planWF(id string, tasks ...contract.TaskPlan) contract.WorkflowPlan {
	return contract.WorkflowPlan{ID: id, Hash: id + "-hash", Tasks: tasks}
}

// take pops the next job of q and returns its ack ID.
func take(t *testing.T, q interface {
	DequeueWithAckId() (any, bool, string)
}) (task string, ack string) {
	t.Helper()
	v, ok, ack := q.DequeueWithAckId()
	if !ok {
		t.Fatal("Expected a job to take")
	}
	tag, ok := workflowTag(v.([]byte))
	if !ok {
		t.Fatalf("Expected a workflow item, got %s", v)
	}
	return tag.task, ack
}

func (e *wfEnv) finish(t *testing.T, q *Queue, ack string, end contract.TaskEnd) []string {
	t.Helper()
	pushed, ok := e.st.FinishTask(q.QueueID(), ack, end)
	if !ok {
		t.Fatalf("FinishTask %+v failed", end)
	}
	return pushed
}

func (e *wfEnv) state(t *testing.T, id string) contract.WorkflowRecord {
	t.Helper()
	r, err := e.st.Record(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func taskStates(r contract.WorkflowRecord) string {
	var s []string
	for _, tr := range r.Tasks {
		s = append(s, tr.Name+"="+string(tr.State))
	}
	return strings.Join(s, " ")
}

func (e *wfEnv) outbox(t *testing.T) int {
	t.Helper()
	var n int
	if err := e.st.f.db.QueryRow(`SELECT COUNT(*) FROM ` + wfOutbox).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func done(wf, task string, result string) contract.TaskEnd {
	return contract.TaskEnd{Workflow: wf, Task: task, Outcome: contract.OutcomeCompleted, Result: []byte(result)}
}

// The store registers for the queues' scope, one per scope; Close unregisters.
func TestWorkflowStoreRegisterClose(t *testing.T) {
	eachLayout(t, func(t *testing.T, single bool) {
		e := newWFEnv(t, single)
		scope := e.a.ScopeID()
		if !strings.HasPrefix(scope, "sqliteq:/") || e.p.ScopeID() != scope || e.st.ScopeID() != scope {
			t.Fatalf("Expected one absolute sqliteq scope, got %q %q %q", scope, e.p.ScopeID(), e.st.ScopeID())
		}
		if single != strings.HasSuffix(scope, "all.db") {
			t.Errorf("Expected the file as the scope root only under WithSingleFile, got %q", scope)
		}
		if got, ok := contract.LookupWorkflowStore(scope); !ok || got != contract.WorkflowStore(e.st) {
			t.Fatal("Expected the store to be registered")
		}
		if _, err := NewWorkflowStore(e.qs); !errors.Is(err, contract.ErrScopeRegistered) {
			t.Errorf("Expected ErrScopeRegistered for a second store, got %v", err)
		}
		if _, err := os.Stat(filepath.Join(e.dir, workflowsFile)); single == (err == nil) {
			t.Errorf("Expected a workflow file only in the folder layout, stat err %v", err)
		}
		if err := e.st.Close(); err != nil {
			t.Fatal(err)
		}
		if _, ok := contract.LookupWorkflowStore(scope); ok {
			t.Error("Expected Close to unregister")
		}
		if err := e.st.Close(); err != nil {
			t.Errorf("Expected a second Close to do nothing, got %v", err)
		}
		st2, err := NewWorkflowStore(e.qs)
		if err != nil {
			t.Fatalf("Expected a store after Close, got %v", err)
		}
		st2.Close()
	})
}

func TestWorkflowStoreOptions(t *testing.T) {
	qs := mustNew(t, t.TempDir())
	defer qs.Close()
	for name, opt := range map[string]WorkflowOption{
		"limit":     WithWorkflowResultLimit(0),
		"retention": WithWorkflowRetention(0),
		"nil":       nil,
	} {
		if st, err := NewWorkflowStore(qs, opt); err == nil {
			st.Close()
			t.Errorf("Expected an error for the %s option", name)
		}
	}
	if len(contract.WorkflowStores()) != 0 {
		for _, s := range contract.WorkflowStores() {
			t.Errorf("Expected no leftover store, got %s", s.ScopeID())
		}
	}
}

// A chain across fifo and priority queues: Submit pushes the root, each
// finish pushes the next task and returns its queue, results flow through
// DepResult, a re-finish returns every push target.
func TestWorkflowHappyPath(t *testing.T) {
	eachLayout(t, func(t *testing.T, single bool) {
		e := newWFEnv(t, single)
		ctx := context.Background()
		plan := planWF("w1", planTask("w1", "t1", e.a), planTask("w1", "t2", e.p, "t1"), planTask("w1", "t3", e.b, "t2"))
		plan.Tasks[1].Priority = 5
		if err := e.st.Submit(ctx, plan); err != nil {
			t.Fatal(err)
		}
		if err := e.st.Submit(ctx, plan); err != nil {
			t.Errorf("Expected a repeated Submit of the same plan to succeed, got %v", err)
		}
		other := plan
		other.Hash = "other"
		if err := e.st.Submit(ctx, other); !errors.Is(err, contract.ErrWorkflowExists) {
			t.Errorf("Expected ErrWorkflowExists for another plan, got %v", err)
		}
		if e.a.Len() != 1 || e.p.Len() != 0 || e.b.Len() != 0 {
			t.Fatalf("Expected only the root pushed, got %d %d %d", e.a.Len(), e.p.Len(), e.b.Len())
		}
		if got := taskStates(e.state(t, "w1")); got != "t1=pending t2=waiting t3=waiting" {
			t.Fatalf("got %s", got)
		}
		for task, want := range map[string]bool{"t1": true, "t2": false, "nope": false} {
			if ok, err := e.st.Runnable(ctx, "w1", task, false); err != nil || ok != want {
				t.Errorf("Runnable %s = %t %v, want %t", task, ok, err, want)
			}
		}
		if _, err := e.st.Runnable(ctx, "missing", "t1", false); !errors.Is(err, contract.ErrWorkflowNotFound) {
			t.Errorf("Expected ErrWorkflowNotFound, got %v", err)
		}
		if _, err := e.st.DepResult(ctx, "w1", "t1"); !errors.Is(err, contract.ErrNoResult) {
			t.Errorf("Expected ErrNoResult before the end, got %v", err)
		}

		task, ack := take(t, e.a)
		if task != "t1" {
			t.Fatalf("got task %s", task)
		}
		if pushed := e.finish(t, e.a, ack, done("w1", "t1", `{"n":1}`)); !slices.Equal(pushed, []string{e.p.QueueID()}) {
			t.Fatalf("Expected a push into p, got %v", pushed)
		}
		if res, err := e.st.DepResult(ctx, "w1", "t1"); err != nil || string(res) != `{"n":1}` {
			t.Errorf("DepResult = %q %v", res, err)
		}
		if r := e.state(t, "w1"); r.Tasks[0].ResultSize != 7 || taskStates(r) != "t1=completed t2=pending t3=waiting" {
			t.Errorf("got %s size %d", taskStates(r), r.Tasks[0].ResultSize)
		}
		if e.a.Len() != 0 {
			t.Errorf("Expected the job acknowledged")
		}
		if n := stats(t, e.a).Counts.Processing; n != 0 {
			t.Errorf("Expected the job settled, got %d processing", n)
		}
		if n := e.outbox(t); n != 0 {
			t.Errorf("Expected an empty outbox, got %d", n)
		}

		task, ack = take(t, e.p)
		e.finish(t, e.p.Queue, ack, done("w1", task, ""))
		task, ack = take(t, e.b)
		e.finish(t, e.b, ack, done("w1", task, "not json"))
		r := e.state(t, "w1")
		if r.State != contract.WorkflowCompleted || r.FinishedAt < r.CreatedAt || r.FinishedAt == 0 {
			t.Fatalf("Expected a completed workflow, got %s at %d..%d", r.State, r.CreatedAt, r.FinishedAt)
		}
		if _, err := e.st.DepResult(ctx, "w1", "t3"); !errors.Is(err, contract.ErrNoResult) {
			t.Errorf("Expected a non-JSON result dropped, got %v", err)
		}

		// A re-finish changes nothing and names every push target
		pushed := e.finish(t, e.a, "999", done("w1", "t1", `{"n":2}`))
		want := []string{e.a.QueueID(), e.p.QueueID(), e.b.QueueID()}
		slices.Sort(pushed)
		slices.Sort(want)
		if !slices.Equal(pushed, want) {
			t.Errorf("Expected every target %v, got %v", want, pushed)
		}
		if res, _ := e.st.DepResult(ctx, "w1", "t1"); string(res) != `{"n":1}` {
			t.Errorf("Expected the first result kept, got %s", res)
		}
	})
}

func TestWorkflowSubmitChecks(t *testing.T) {
	e := newWFEnv(t, false)
	ctx := context.Background()
	other := mustNew(t, t.TempDir())
	defer other.Close()
	oq, _ := other.NewQueue("a")

	bad := map[string]contract.WorkflowPlan{
		"empty":       {ID: "x", Hash: "h"},
		"bad id":      planWF("a b", planTask("a b", "t", e.a)),
		"index":       planWF("index", planTask("index", "t", e.a)),
		"bad task":    planWF("x", planTask("x", "t t", e.a)),
		"other scope": planWF("x", planTask("x", "t", oq)),
		"not a queue": planWF("x", planTask("x", "t", "q")),
		"bad item":    planWF("x", func() contract.TaskPlan { tp := planTask("x", "t", e.a); tp.Item = []byte("[1]"); return tp }()),
		"dup name":    planWF("x", planTask("x", "t", e.a), planTask("x", "t", e.a)),
	}
	for name, plan := range bad {
		if err := e.st.Submit(ctx, plan); err == nil {
			t.Errorf("Expected an error for %s", name)
		}
	}
	if err := e.st.Submit(ctx, bad["other scope"]); !errors.Is(err, contract.ErrScopeMismatch) {
		t.Errorf("Expected ErrScopeMismatch, got %v", err)
	}
	if _, err := e.st.Record(ctx, "x"); !errors.Is(err, contract.ErrWorkflowNotFound) {
		t.Errorf("Expected nothing stored by a refused Submit, got %v", err)
	}
	if e.a.Len() != 0 {
		t.Errorf("Expected nothing pushed by a refused Submit, got %d", e.a.Len())
	}
}

// Failure follows the dependency policy; FailFast cancels waiting tasks; an
// undo runs in reverse order once the run is over.
func TestWorkflowFailureAndCompensation(t *testing.T) {
	eachLayout(t, func(t *testing.T, single bool) {
		e := newWFEnv(t, single)
		ctx := context.Background()
		plan := planWF("w2",
			withUndo(planTask("w2", "a", e.a), "w2", e.b),
			withUndo(planTask("w2", "b", e.a, "a"), "w2", e.b),
			planTask("w2", "c", e.a, "b"))
		if err := e.st.Submit(ctx, plan); err != nil {
			t.Fatal(err)
		}
		_, ack := take(t, e.a)
		e.finish(t, e.a, ack, done("w2", "a", ""))
		_, ack = take(t, e.a)
		e.finish(t, e.a, ack, done("w2", "b", ""))
		_, ack = take(t, e.a)
		pushed := e.finish(t, e.a, ack, contract.TaskEnd{Workflow: "w2", Task: "c", Outcome: contract.OutcomeFailed, Reason: "boom"})
		r := e.state(t, "w2")
		if r.State != contract.WorkflowCompensating || r.Tasks[2].Reason != "boom" || !slices.Equal(pushed, []string{e.b.QueueID()}) {
			t.Fatalf("Expected compensation started in b, got %s %s %v", r.State, taskStates(r), pushed)
		}
		// b's undo first: a's waits for it
		if e.b.Len() != 1 {
			t.Fatalf("Expected one undo queued, got %d", e.b.Len())
		}
		task, ack := take(t, e.b)
		if task != "b" {
			t.Fatalf("Expected the undo of b first, got %s", task)
		}
		if ok, _ := e.st.Runnable(ctx, "w2", "b", true); !ok {
			t.Error("Expected the undo runnable")
		}
		if ok, _ := e.st.Runnable(ctx, "w2", "a", true); ok {
			t.Error("Expected a's undo to wait")
		}
		e.finish(t, e.b, ack, contract.TaskEnd{Workflow: "w2", Task: "b", Undo: true, Outcome: contract.OutcomeCompleted})
		task, ack = take(t, e.b)
		if task != "a" {
			t.Fatalf("got %s", task)
		}
		e.finish(t, e.b, ack, contract.TaskEnd{Workflow: "w2", Task: "a", Undo: true, Outcome: contract.OutcomeFailed, Reason: "undo broke"})
		r = e.state(t, "w2")
		if r.State != contract.WorkflowCompensationFailed || r.Tasks[0].Undo.State != contract.JobFailed || r.Tasks[0].Undo.Reason != "undo broke" {
			t.Errorf("got %s undo %+v", r.State, r.Tasks[0].Undo)
		}

		// FailFast: the first failure cancels what waits
		ff := planWF("w3", planTask("w3", "a", e.a), planTask("w3", "b", e.a), planTask("w3", "c", e.a, "a"))
		ff.FailFast = true
		if err := e.st.Submit(ctx, ff); err != nil {
			t.Fatal(err)
		}
		_, ack = take(t, e.a)
		e.finish(t, e.a, ack, contract.TaskEnd{Workflow: "w3", Task: "a", Outcome: contract.OutcomeFailed})
		if got := taskStates(e.state(t, "w3")); got != "a=failed b=pending c=cancelled" {
			t.Errorf("got %s", got)
		}
		if ok, _ := e.st.Runnable(ctx, "w3", "b", false); ok {
			t.Error("Expected b stopped by FailFast")
		}
	})
}

func TestWorkflowCancel(t *testing.T) {
	eachLayout(t, func(t *testing.T, single bool) {
		e := newWFEnv(t, single)
		ctx := context.Background()
		if err := e.st.Cancel(ctx, "missing", nil); !errors.Is(err, contract.ErrWorkflowNotFound) {
			t.Errorf("Expected ErrWorkflowNotFound, got %v", err)
		}
		plan := planWF("w1", withUndo(planTask("w1", "a", e.a), "w1", e.b), planTask("w1", "b", e.a, "a"))
		if err := e.st.Submit(ctx, plan); err != nil {
			t.Fatal(err)
		}
		yes := true
		if err := e.st.Cancel(ctx, "w1", &yes); err != nil {
			t.Fatal(err)
		}
		r := e.state(t, "w1")
		if taskStates(r) != "a=pending b=cancelled" || !r.UndoOnCancel || r.State != contract.WorkflowRunning {
			t.Fatalf("got %s %t %s", taskStates(r), r.UndoOnCancel, r.State)
		}
		if ok, _ := e.st.Runnable(ctx, "w1", "a", false); ok {
			t.Error("Expected the queued task stopped by the cancel")
		}
		if err := e.st.Cancel(ctx, "w1", nil); err != nil {
			t.Errorf("Expected a second Cancel to do nothing, got %v", err)
		}
		// The pre-run check finishes it cancelled; no task completed, so no undo
		_, ack := take(t, e.a)
		e.finish(t, e.a, ack, contract.TaskEnd{Workflow: "w1", Task: "a", Outcome: contract.OutcomeCancelled, Reason: "workflow cancelled"})
		if r := e.state(t, "w1"); r.State != contract.WorkflowCancelled || r.FinishedAt == 0 {
			t.Errorf("got %s", r.State)
		}
		if _, err := e.st.DepResult(ctx, "w1", "a"); !errors.Is(err, contract.ErrDepFailed) {
			t.Errorf("Expected ErrDepFailed, got %v", err)
		}
	})
}

// A completed result over the limit fails the task.
func TestWorkflowResultLimit(t *testing.T) {
	e := newWFEnv(t, false, WithWorkflowResultLimit(8))
	ctx := context.Background()
	if err := e.st.Submit(ctx, planWF("w1", planTask("w1", "a", e.a), planTask("w1", "b", e.a, "a"))); err != nil {
		t.Fatal(err)
	}
	_, ack := take(t, e.a)
	e.finish(t, e.a, ack, done("w1", "a", `{"big":"0123456789"}`))
	r := e.state(t, "w1")
	if r.Tasks[0].State != contract.JobFailed || r.Tasks[0].Reason != ErrResultTooLarge.Error() || r.Tasks[0].ResultSize != 0 {
		t.Errorf("got %s %q", taskStates(r), r.Tasks[0].Reason)
	}
}

// Crash after step a: the end and its pushes are in the workflow file's
// outbox only; recovery pushes them.
func TestWorkflowCrashAfterRecord(t *testing.T) {
	e := newWFEnv(t, false)
	ctx := context.Background()
	if err := e.st.Submit(ctx, planWF("w1", planTask("w1", "a", e.a), planTask("w1", "b", e.b, "a"))); err != nil {
		t.Fatal(err)
	}
	_, _ = take(t, e.a)
	end := done("w1", "a", `1`)
	var r endResult
	if err := e.st.write(ctx, func(tx *sql.Tx) (err error) {
		r, err = e.st.endTx(ctx, tx, e.a.QueueID(), end)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if r.minSeq == 0 || e.outbox(t) != 1 || e.b.Len() != 0 {
		t.Fatalf("Expected one outbox row and nothing pushed, got %d rows, b %d", e.outbox(t), e.b.Len())
	}
	e.st.recoverOutbox(0)
	if e.outbox(t) != 0 || e.b.Len() != 1 {
		t.Fatalf("Expected recovery to push the row, got %d rows, b %d", e.outbox(t), e.b.Len())
	}
	// The job is redelivered after the restart, skipped by the pre-run check
	e.a.requeueNoAck()
	task, ack2 := take(t, e.a)
	if ok, _ := e.st.Runnable(ctx, "w1", task, false); ok {
		t.Fatal("Expected the redelivered job to be skipped")
	}
	e.finish(t, e.a, ack2, contract.TaskEnd{Workflow: "w1", Task: "a", Outcome: contract.OutcomeCancelled, Reason: "workflow cancelled"})
	if got := taskStates(e.state(t, "w1")); got != "a=completed b=pending" || e.b.Len() != 1 {
		t.Errorf("Expected the end unchanged and one push, got %s, b %d", got, e.b.Len())
	}
}

// Leftover outbox rows are pushed when a store opens.
func TestWorkflowRecoverOnOpen(t *testing.T) {
	e := newWFEnv(t, false)
	ctx := context.Background()
	if err := e.st.Submit(ctx, planWF("w1", planTask("w1", "a", e.a), planTask("w1", "b", e.b, "a"))); err != nil {
		t.Fatal(err)
	}
	_, _ = take(t, e.a)
	if err := e.st.write(ctx, func(tx *sql.Tx) error {
		_, err := e.st.endTx(ctx, tx, e.a.QueueID(), done("w1", "a", ""))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	e.st.Close()
	st2, err := NewWorkflowStore(e.qs)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if e.b.Len() != 1 {
		t.Errorf("Expected the leftover row pushed at open, got %d", e.b.Len())
	}
}

// The loop pushes rows left by a writer, once they are older than the
// recovery interval.
func TestWorkflowRecoveryLoop(t *testing.T) {
	old := recoverEvery
	recoverEvery = 20 * time.Millisecond
	defer func() { recoverEvery = old }()
	e := newWFEnv(t, false)
	ctx := context.Background()
	if err := e.st.Submit(ctx, planWF("w1", planTask("w1", "a", e.a), planTask("w1", "b", e.b, "a"))); err != nil {
		t.Fatal(err)
	}
	_, _ = take(t, e.a)
	if err := e.st.write(ctx, func(tx *sql.Tx) error {
		_, err := e.st.endTx(ctx, tx, e.a.QueueID(), done("w1", "a", ""))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if e.b.Len() == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("Expected the loop to push the leftover row")
}

// Crash after step b: a duplicate push of the same job ID stores nothing.
func TestWorkflowDuplicatePush(t *testing.T) {
	e := newWFEnv(t, false)
	ctx := context.Background()
	if err := e.st.Submit(ctx, planWF("w1", planTask("w1", "a", e.a), planTask("w1", "b", e.b, "a"))); err != nil {
		t.Fatal(err)
	}
	_, ack := take(t, e.a)
	var r endResult
	if err := e.st.write(ctx, func(tx *sql.Tx) (err error) {
		r, err = e.st.endTx(ctx, tx, e.a.QueueID(), done("w1", "a", ""))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	where, args := `workflow_id = ? AND seq >= ?`, []any{"w1", r.minSeq}
	rows, err := e.st.f.db.Query(`SELECT queue_id, job_id, item, priority FROM `+wfOutbox+` WHERE `+where, args...)
	if err != nil {
		t.Fatal(err)
	}
	var row outRow
	for rows.Next() {
		rows.Scan(&row.queueID, &row.jobID, &row.item, &row.priority)
	}
	rows.Close()
	e.st.drain(ctx, where, args...) // step b done, the crash comes before the ack
	if pushRow(&handle{q: e.b}, row) != nil || e.b.Len() != 1 {
		t.Fatalf("Expected the duplicate push to store nothing, got %d rows", e.b.Len())
	}
	// Redelivery of a: skipped, then finished; b is still queued once
	e.a.requeueNoAck()
	_, ack = take(t, e.a)
	e.finish(t, e.a, ack, contract.TaskEnd{Workflow: "w1", Task: "a", Outcome: contract.OutcomeCancelled})
	if e.b.Len() != 1 || e.a.Len() != 0 {
		t.Errorf("Expected b once and a settled, got b %d a %d", e.b.Len(), e.a.Len())
	}
}

// Single file: the end and the ack are one transaction. A push target this
// client cannot reach gets an outbox row in that transaction; the recovery
// loop pushes it once a client opens the queue.
func TestWorkflowSingleFileAtomic(t *testing.T) {
	e := newWFEnv(t, true)
	ctx := context.Background()
	if err := e.st.Submit(ctx, planWF("w1", planTask("w1", "a", e.a), planTask("w1", "b", e.b, "a"))); err != nil {
		t.Fatal(err)
	}
	if n := e.outbox(t); n != 0 {
		t.Fatalf("Expected no outbox rows while every queue is reachable, got %d", n)
	}
	_, ack := take(t, e.a)
	hide := func(id string) {
		qs := e.qs.(*queues)
		qs.mu.Lock()
		m := maps.Clone(*qs.byID.Load())
		delete(m, id)
		qs.byID.Store(&m)
		qs.mu.Unlock()
	}
	e.b.Close() // closed: the registry still lists it, the table does not
	hide(e.b.QueueID())
	registry.mu.Lock()
	delete(e.a.f.handles, "b")
	registry.mu.Unlock()
	pushed, ok := e.st.FinishTask(e.a.QueueID(), ack, done("w1", "a", ""))
	if !ok || len(pushed) != 0 {
		t.Fatalf("Expected ok with no push, got %v %t", pushed, ok)
	}
	if got := taskStates(e.state(t, "w1")); got != "a=completed b=pending" || e.outbox(t) != 1 {
		t.Errorf("Expected the end recorded and one outbox row, got %s, %d rows", got, e.outbox(t))
	}
	if n := stats(t, e.a).Counts.Processing; n != 0 {
		t.Errorf("Expected the job acknowledged, got %d processing", n)
	}
	// Opened later (here by a second client): the loop pushes the row
	other := mustNew(t, e.qs.(*queues).root, WithSingleFile())
	defer other.Close()
	b2, err := other.NewQueue("b")
	if err != nil {
		t.Fatal(err)
	}
	e.st.recoverOutbox(0)
	if e.outbox(t) != 0 || b2.Len() != 1 {
		t.Errorf("Expected the row pushed, got %d rows, b %d", e.outbox(t), b2.Len())
	}
}

// FinishTask finds a queue opened by any client of the scope in this process.
func TestWorkflowQueueOfAnotherClient(t *testing.T) {
	eachLayout(t, func(t *testing.T, single bool) {
		e := newWFEnv(t, single)
		var other Queues
		if single {
			other = mustNew(t, e.qs.(*queues).root, WithSingleFile())
		} else {
			other = mustNew(t, e.dir)
		}
		defer other.Close()
		c, err := other.NewQueue("c") // known to the second client only
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if err := e.st.Submit(ctx, planWF("w1", planTask("w1", "a", e.a), planTask("w1", "c", c, "a"))); err != nil {
			t.Fatal(err)
		}
		_, ack := take(t, e.a)
		// Forget what Submit learned: the lookup goes through the registry
		qs := e.qs.(*queues)
		qs.mu.Lock()
		qs.byID.Store(&map[string]*handle{})
		qs.mu.Unlock()
		if _, ok := e.st.FinishTask(e.a.QueueID(), ack, done("w1", "a", "")); !ok {
			t.Fatal("FinishTask failed")
		}
		if c.Len() != 1 {
			t.Errorf("Expected the task pushed into the other client's queue, got %d", c.Len())
		}
	})
}

// A plain job cannot hold a wf: job ID, and a row that already holds one is
// not taken for the workflow's push.
func TestWorkflowJobIDHeldByPlainJob(t *testing.T) {
	eachLayout(t, func(t *testing.T, single bool) {
		e := newWFEnv(t, single)
		if e.a.EnqueueOnce(job("wf:w1:a"), "wf:w1:a") || e.a.EnqueueOnce(wfItem("w1", "other", false), "wf:w1:a") {
			t.Error("Expected a plain job refused a wf: job ID")
		}
		// A row planted by an older version, before the guard
		if _, err := e.a.f.db.Exec(`INSERT INTO "a" (data, status, ack, created_at, updated_at, job_id) VALUES (?, 'pending', 0, 0, 0, 'wf:w1:a')`, job("wf:w1:a")); err != nil {
			t.Fatal(err)
		}
		if err := e.st.Submit(context.Background(), planWF("w1", planTask("w1", "a", e.a))); err != nil {
			t.Fatal(err)
		}
		if e.outbox(t) != 1 || e.a.Len() != 1 {
			t.Errorf("Expected the push kept in the outbox, got %d rows, a %d", e.outbox(t), e.a.Len())
		}
		e.st.recoverOutbox(0)
		if e.outbox(t) != 1 {
			t.Error("Expected the row to stay while the job ID is held")
		}
	})
}

// A Deps entry that names no task is refused; an ended, expired, unswept
// workflow does not block a Submit of its ID.
func TestWorkflowSubmitDepsAndExpired(t *testing.T) {
	e := newWFEnv(t, false, WithWorkflowRetention(time.Hour))
	ctx := context.Background()
	if err := e.st.Submit(ctx, planWF("w1", planTask("w1", "a", e.a, "ghost"))); err == nil {
		t.Error("Expected a dependency on no task refused")
	}
	var clock atomic.Int64
	clock.Store(time.Now().UnixMilli())
	e.st.setClock(func() time.Time { return time.UnixMilli(clock.Load()) })
	if err := e.st.Submit(ctx, planWF("w2", planTask("w2", "a", e.a))); err != nil {
		t.Fatal(err)
	}
	_, ack := take(t, e.a)
	e.finish(t, e.a, ack, done("w2", "a", ""))
	clock.Add(2 * time.Hour.Milliseconds())
	again := planWF("w2", planTask("w2", "a", e.a))
	again.Hash = "new"
	if err := e.st.Submit(ctx, again); err != nil {
		t.Fatalf("Expected a new workflow under the expired ID, got %v", err)
	}
	if got := taskStates(e.state(t, "w2")); got != "a=pending" {
		t.Errorf("got %s", got)
	}
}

// Purge with no store registered still ends the purged workflow tasks, from
// the workflow tables on disk; under WithSingleFile in one transaction.
func TestWorkflowPurgeWithoutStore(t *testing.T) {
	eachLayout(t, func(t *testing.T, single bool) {
		e := newWFEnv(t, single)
		ctx := context.Background()
		plan := planWF("w1", planTask("w1", "a", e.a), planTask("w1", "z", e.b, "a"))
		plan.Tasks[1].Always = true
		if err := e.st.Submit(ctx, plan); err != nil {
			t.Fatal(err)
		}
		e.st.Close() // unregisters; the tables stay
		e.a.Purge()
		st, err := NewWorkflowStore(e.qs)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		r, err := st.Record(ctx, "w1")
		if err != nil {
			t.Fatal(err)
		}
		if got := taskStates(r); got != "a=cancelled z=pending" || e.b.Len() != 1 {
			t.Errorf("got %s, b %d", got, e.b.Len())
		}
	})
}

// Folder layout: a purge intent a crash left is applied by the recovery.
func TestWorkflowPurgeIntentRecovery(t *testing.T) {
	e := newWFEnv(t, false)
	ctx := context.Background()
	if err := e.st.Submit(ctx, planWF("w1", planTask("w1", "a", e.a), planTask("w1", "b", e.b, "a"))); err != nil {
		t.Fatal(err)
	}
	e.st.recordPurge(ctx, e.a, []wfItemTag{{wf: "w1", task: "a"}}) // the crash comes after this
	e.a.purge(nil)
	e.st.recoverOutbox(0)
	if got := taskStates(e.state(t, "w1")); got != "a=cancelled b=cancelled" {
		t.Errorf("got %s", got)
	}
	var n int
	e.st.f.db.QueryRow(`SELECT COUNT(*) FROM ` + wfPurged).Scan(&n)
	if n != 0 {
		t.Errorf("Expected the intent deleted, got %d", n)
	}
}

// Ended workflows are dropped after the retention, running ones are kept.
func TestWorkflowRetention(t *testing.T) {
	eachLayout(t, func(t *testing.T, single bool) {
		e := newWFEnv(t, single, WithWorkflowRetention(time.Hour))
		ctx := context.Background()
		var clock atomic.Int64
		clock.Store(time.Now().UnixMilli())
		e.st.setClock(func() time.Time { return time.UnixMilli(clock.Load()) })
		for _, id := range []string{"ended", "running"} {
			if err := e.st.Submit(ctx, planWF(id, planTask(id, "a", e.a))); err != nil {
				t.Fatal(err)
			}
		}
		_, ack := take(t, e.a)
		e.finish(t, e.a, ack, done("ended", "a", `1`))

		clock.Add(30 * time.Minute.Milliseconds())
		if err := e.st.sweep(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := e.st.Record(ctx, "ended"); err != nil {
			t.Fatalf("Expected the workflow kept inside the retention, got %v", err)
		}
		clock.Add(2 * time.Hour.Milliseconds())
		if err := e.st.sweep(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := e.st.Record(ctx, "ended"); !errors.Is(err, contract.ErrWorkflowNotFound) {
			t.Errorf("Expected the ended workflow swept, got %v", err)
		}
		if _, err := e.st.Record(ctx, "running"); err != nil {
			t.Errorf("Expected the running workflow kept, got %v", err)
		}
		var n int
		e.st.f.db.QueryRow(`SELECT COUNT(*) FROM ` + wfTasks + ` WHERE workflow_id = 'ended'`).Scan(&n)
		if n != 0 {
			t.Errorf("Expected the tasks swept too, got %d", n)
		}
		// A finish of a swept workflow settles the job and returns ok
		if err := e.st.Submit(ctx, planWF("gone", planTask("gone", "a", e.a))); err != nil {
			t.Fatal(err)
		}
		_, ack = take(t, e.a)
		e.st.f.db.Exec(`DELETE FROM ` + wfWorkflows + ` WHERE id = 'gone'`)
		if pushed, ok := e.st.FinishTask(e.a.QueueID(), ack, done("gone", "a", "")); !ok || len(pushed) != 0 {
			t.Errorf("Expected ok with no IDs for a gone workflow, got %v %t", pushed, ok)
		}
	})
}

func TestWorkflowListing(t *testing.T) {
	e := newWFEnv(t, false)
	ctx := context.Background()
	var clock atomic.Int64
	clock.Store(1_000_000)
	e.st.setClock(func() time.Time { return time.UnixMilli(clock.Load()) })
	for _, id := range []string{"b", "a", "c"} {
		if err := e.st.Submit(ctx, planWF(id, planTask(id, "t", e.a))); err != nil {
			t.Fatal(err)
		}
		if id == "a" {
			clock.Add(1) // c is newer; a and b share a ms
		}
	}
	_, ack := take(t, e.a)
	e.finish(t, e.a, ack, done("b", "t", ""))

	ids := func(state contract.WorkflowState, off, lim int) string {
		keys, err := e.st.WorkflowKeys(ctx, state, off, lim)
		if err != nil {
			t.Fatal(err)
		}
		recs, err := e.st.Workflows(ctx, state, off, lim)
		if err != nil || len(recs) != len(keys) {
			t.Fatalf("Workflows %v %d vs keys %d", err, len(recs), len(keys))
		}
		var s []string
		for i, k := range keys {
			if recs[i].ID != k.ID || recs[i].CreatedAt != k.CreatedAt {
				t.Errorf("record %+v != key %+v", recs[i], k)
			}
			s = append(s, k.ID)
		}
		return strings.Join(s, ",")
	}
	if got := ids("", 0, 10); got != "c,a,b" {
		t.Errorf("Expected newest first then by ID, got %s", got)
	}
	if got := ids("", 1, 1); got != "a" {
		t.Errorf("got %s", got)
	}
	if got := ids(contract.WorkflowCompleted, 0, 10); got != "b" {
		t.Errorf("got %s", got)
	}
	if got := ids("", 0, 0); got != "" {
		t.Errorf("got %s", got)
	}
	if got := ids("", -3, -1); got != "" {
		t.Errorf("got %s", got)
	}
}

func TestWorkflowTag(t *testing.T) {
	for _, c := range []struct {
		name string
		item string
		ok   bool
	}{
		{"task", `{"id":"wf:w:t","wf":"w","task":"t"}`, true},
		{"undo", `{"id":"wf:w:t:undo","wf":"w","task":"t","undo":true}`, true},
		{"undo false", `{"id":"wf:w:t","wf":"w","task":"t","undo":false}`, true},
		{"undo null", `{"id":"wf:w:t","wf":"w","task":"t","undo":null}`, true},
		{"undo not bool", `{"id":"wf:w:t","wf":"w","task":"t","undo":1}`, false},
		{"plain", `{"id":"x","data":1}`, false},
		{"wrong id", `{"id":"wf:w:u","wf":"w","task":"t"}`, false},
		{"missing :undo", `{"id":"wf:w:t","wf":"w","task":"t","undo":true}`, false},
		{"bad names", `{"id":"wf:w w:t","wf":"w w","task":"t"}`, false},
		{"index", `{"id":"wf:index:t","wf":"index","task":"t"}`, false},
		{"upper WF", `{"id":"wf:w:t","wf":"w","WF":"x","task":"t"}`, false},
		{"upper Task", `{"id":"wf:w:t","wf":"w","Task":"t"}`, false},
		{"dotted I", `{"\u0130d":"x","id":"wf:w:t","wf":"w","task":"t"}`, true},
		{"Id", `{"Id":"wf:w:t","id":"wf:w:t","wf":"w","task":"t"}`, false},
		{"long s", `{"id":"wf:w:t","wf":"w","taſk":"t","task":"t"}`, false},
		{"kelvin", `{"id":"wf:w:t","wf":"w","tasK":"t","task":"t"}`, false},
		{"not object", `[1]`, false},
		{"wf number", `{"id":"wf:w:t","wf":1,"task":"t"}`, false},
	} {
		if _, ok := workflowTag([]byte(c.item)); ok != c.ok {
			t.Errorf("%s: got %t, want %t", c.name, ok, c.ok)
		}
	}
}

// A cancel mark never drops a valid workflow item, whichever pop takes it;
// task JSON whose keys differ in case is a plain job and is dropped.
func TestWorkflowItemSurvivesCancelMark(t *testing.T) {
	eachLayout(t, func(t *testing.T, single bool) {
		e := newWFEnv(t, single)
		ctx := context.Background()
		if err := e.st.Submit(ctx, planWF("w1", planTask("w1", "a", e.a))); err != nil {
			t.Fatal(err)
		}
		if err := e.a.CancelJob("wf:w1:a"); err != nil {
			t.Fatal(err)
		}
		if _, ok := e.a.Dequeue(); !ok {
			t.Fatal("Expected the marked workflow item to be handed out")
		}
		// An envelope whose id does not match its tag is a plain job
		e.a.EnqueueOnce([]byte(`{"id":"wf:w1:x","status":"Created","data":1,"wf":"w1","task":"y"}`), "wf:w1:x")
		e.a.CancelJob("wf:w1:x")
		items, acks := make([]any, 2), make([]string, 2)
		if n, _, err := e.a.DequeueBatchWithAckIds(items, acks); err != nil || n != 0 {
			t.Fatalf("Expected the mismatched item dropped as a plain job, got %d %v", n, err)
		}

		upper := []byte(`{"id":"wf:w1:b","status":"Created","data":1,"WF":"w1","wf":"w1","task":"b"}`)
		e.a.EnqueueOnce(upper, "wf:w1:b")
		e.a.CancelJob("wf:w1:b")
		if _, ok := e.a.Dequeue(); ok {
			t.Error("Expected the case-variant item dropped as a plain job")
		}
		valid := wfItem("w1", "c", false)
		e.a.EnqueueOnce(valid, "wf:w1:c")
		e.a.CancelJob("wf:w1:c")
		n, _, err := e.a.DequeueBatchWithAckIds(items, acks)
		if err != nil || n != 1 {
			t.Errorf("Expected the batch pop to hand out the marked workflow item, got %d %v", n, err)
		}
	})
}

// A purged workflow task ends cancelled; its workflow goes on and what that
// releases is pushed after the delete.
func TestWorkflowPurge(t *testing.T) {
	eachLayout(t, func(t *testing.T, single bool) {
		e := newWFEnv(t, single)
		ctx := context.Background()
		plan := planWF("w1", planTask("w1", "a", e.a), planTask("w1", "n", e.a, "a"), planTask("w1", "z", e.b, "a"))
		plan.Tasks[2].Always = true
		if err := e.st.Submit(ctx, plan); err != nil {
			t.Fatal(err)
		}
		e.a.Purge()
		r := e.state(t, "w1")
		if got := taskStates(r); got != "a=cancelled n=cancelled z=pending" || r.Tasks[0].Reason != purgeReason {
			t.Fatalf("got %s reason %q", got, r.Tasks[0].Reason)
		}
		if e.b.Len() != 1 || e.a.Len() != 0 {
			t.Errorf("Expected the always task pushed into b, got b %d a %d", e.b.Len(), e.a.Len())
		}
		task, ack := take(t, e.b)
		e.finish(t, e.b, ack, done("w1", task, ""))
		if r := e.state(t, "w1"); r.State != contract.WorkflowFailed {
			t.Errorf("Expected failed (a cancelled end before any cancel is a failure), got %s", r.State)
		}
		// A purge with no workflow items, or an already ended task, changes nothing
		e.b.Purge()
		if r := e.state(t, "w1"); r.State != contract.WorkflowFailed {
			t.Errorf("got %s", r.State)
		}
	})
}

// End to end with varmq workers on two queues: the workflow runs through the
// registered store, in both layouts.
func TestWorkflowWithVarmqWorkers(t *testing.T) {
	eachLayout(t, func(t *testing.T, single bool) {
		e := newWFEnv(t, single)
		var mu sync.Mutex
		var order []string
		run := func(name string) func(varmq.Job[int]) error {
			return func(j varmq.Job[int]) error {
				mu.Lock()
				order = append(order, name)
				mu.Unlock()
				return nil
			}
		}
		wa := varmq.NewErrWorker(run("a"))
		wb := varmq.NewErrWorker(run("b"))
		defer wa.Stop()
		defer wb.Stop()
		qa, qb := wa.WithQueue(e.a), wb.WithQueue(e.b)

		wf := varmq.NewWorkflow(e.st, varmq.WithWorkflowID("w1"))
		ta := wf.Add("a", qa, 1)
		wf.Add("b", qb, 2).After(ta)
		if err := wf.Submit(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		r, err := wf.WaitContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if r.State != contract.WorkflowCompleted || !slices.Equal(order, []string{"a", "b"}) {
			t.Errorf("got %s %v", r.State, order)
		}
	})
}

// The loop applies a purge intent only once the job row is gone from its
// queue: a purge slower than the tick waits, and an aborted purge whose job
// then ran leaves the task alone.
func TestWorkflowPurgeIntentWaitsForDelete(t *testing.T) {
	e := newWFEnv(t, false)
	ctx := context.Background()
	if err := e.st.Submit(ctx, planWF("w1", planTask("w1", "a", e.a), planTask("w1", "b", e.b, "a"))); err != nil {
		t.Fatal(err)
	}
	if err := e.st.Submit(ctx, planWF("w2", planTask("w2", "a", e.a))); err != nil {
		t.Fatal(err)
	}
	intents := func() int {
		var n int
		e.st.f.db.QueryRow(`SELECT COUNT(*) FROM ` + wfPurged).Scan(&n)
		return n
	}
	e.st.recordPurge(ctx, e.a, []wfItemTag{{wf: "w1", task: "a"}, {wf: "w2", task: "a"}})

	// The purge is still running: its delete has not committed
	e.st.recoverOutbox(0)
	if got := taskStates(e.state(t, "w1")); got != "a=pending b=waiting" || intents() != 2 {
		t.Fatalf("Expected the intents to wait, got %s, %d intents", got, intents())
	}

	// w2's purge aborted and its job ran; w1's delete commits
	e.a.f.db.Exec(`DELETE FROM "a" WHERE job_id = 'wf:w1:a'`)
	_, ack := take(t, e.a)
	e.finish(t, e.a, ack, done("w2", "a", ""))
	e.st.recoverOutbox(0)
	if got := taskStates(e.state(t, "w1")); got != "a=cancelled b=cancelled" {
		t.Errorf("Expected the delete's intent applied, got %s", got)
	}
	if r := e.state(t, "w2"); r.State != contract.WorkflowCompleted {
		t.Errorf("Expected the aborted purge's task left alone, got %s", r.State)
	}
	if intents() != 0 {
		t.Errorf("Expected every intent cleared, got %d", intents())
	}
}

// WorkflowCounts counts the stored workflows per state, as the listing sees
// them: an end moves a workflow, a purge fails it, the sweep drops it.
func TestWorkflowCounts(t *testing.T) {
	eachLayout(t, func(t *testing.T, single bool) {
		e := newWFEnv(t, single, WithWorkflowRetention(time.Hour))
		ctx := context.Background()
		var clock atomic.Int64
		clock.Store(time.Now().UnixMilli())
		e.st.setClock(func() time.Time { return time.UnixMilli(clock.Load()) })
		check := func(what string, want map[contract.WorkflowState]int64) {
			t.Helper()
			got, err := e.st.WorkflowCounts(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !maps.Equal(got, want) {
				t.Errorf("%s: counts %v, want %v", what, got, want)
			}
			for _, state := range []contract.WorkflowState{contract.WorkflowRunning, contract.WorkflowCompleted, contract.WorkflowFailed,
				contract.WorkflowCompensating, contract.WorkflowCompensated, contract.WorkflowCompensationFailed, contract.WorkflowCancelled} {
				keys, err := e.st.WorkflowKeys(ctx, state, 0, 100)
				if err != nil || int64(len(keys)) != got[state] {
					t.Errorf("%s: %s lists %d (%v), counts %d", what, state, len(keys), err, got[state])
				}
			}
		}
		check("empty store", map[contract.WorkflowState]int64{})

		for _, id := range []string{"run", "done", "done2", "fail", "cancel"} {
			if err := e.st.Submit(ctx, planWF(id, planTask(id, "a", e.a))); err != nil {
				t.Fatal(err)
			}
		}
		if err := e.st.Submit(ctx, planWF("undo", withUndo(planTask("undo", "a", e.a), "undo", e.b), planTask("undo", "z", e.a, "a"))); err != nil {
			t.Fatal(err)
		}
		check("submitted", map[contract.WorkflowState]int64{contract.WorkflowRunning: 6})

		take(t, e.a) // run: taken, never finished
		for _, end := range []contract.TaskEnd{
			done("done", "a", ""),
			done("done2", "a", ""),
			{Workflow: "fail", Task: "a", Outcome: contract.OutcomeFailed},
			{Workflow: "cancel", Task: "a", Outcome: contract.OutcomeCancelled},
			done("undo", "a", ""),
			{Workflow: "undo", Task: "z", Outcome: contract.OutcomeFailed},
		} {
			if end.Workflow == "cancel" {
				if err := e.st.Cancel(ctx, "cancel", nil); err != nil {
					t.Fatal(err)
				}
			}
			_, ack := take(t, e.a)
			e.finish(t, e.a, ack, end)
		}
		check("ended", map[contract.WorkflowState]int64{contract.WorkflowRunning: 1, contract.WorkflowCompleted: 2,
			contract.WorkflowFailed: 1, contract.WorkflowCancelled: 1, contract.WorkflowCompensating: 1})

		_, ack := take(t, e.b)
		e.finish(t, e.b, ack, contract.TaskEnd{Workflow: "undo", Task: "a", Undo: true, Outcome: contract.OutcomeCompleted})
		check("compensated", map[contract.WorkflowState]int64{contract.WorkflowRunning: 1, contract.WorkflowCompleted: 2,
			contract.WorkflowFailed: 1, contract.WorkflowCancelled: 1, contract.WorkflowCompensated: 1})

		e.a.Purge() // ends run's task cancelled: the workflow fails
		check("purged", map[contract.WorkflowState]int64{contract.WorkflowCompleted: 2,
			contract.WorkflowFailed: 2, contract.WorkflowCancelled: 1, contract.WorkflowCompensated: 1})

		if err := e.st.Submit(ctx, planWF("late", planTask("late", "a", e.a))); err != nil {
			t.Fatal(err)
		}
		clock.Add(2 * time.Hour.Milliseconds())
		if err := e.st.sweep(ctx); err != nil {
			t.Fatal(err)
		}
		check("swept", map[contract.WorkflowState]int64{contract.WorkflowRunning: 1})

		// a failed count is an error, never a partial map
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		if got, err := e.st.WorkflowCounts(cctx); err == nil || got != nil {
			t.Errorf("Expected an error and no counts from a cancelled count, got %v, %v", got, err)
		}

		// the scope is a path: a store on another host is another store
		var hl contract.HostLocal = e.st
		hl.HostLocal()

		// the count is an index scan, not a table scan
		rows, err := e.st.f.db.Query(`EXPLAIN QUERY PLAN SELECT state, COUNT(*) FROM ` + wfWorkflows + ` GROUP BY state`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var plan []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plan = append(plan, detail)
		}
		if !slices.ContainsFunc(plan, func(d string) bool { return strings.Contains(d, "__sqliteq_wf_workflows_state") }) {
			t.Errorf("Expected the count to use index __sqliteq_wf_workflows_state, plan %q", plan)
		}
	})
}

// times is r's time fields: enqueued, started, finished.
func times(r contract.TaskRecord) [3]int64 {
	return [3]int64{r.EnqueuedAt, r.StartedAt, r.FinishedAt}
}

// A task record carries when it was pushed, when the pre-run check last let
// it run and when it ended; so does its undo. A refused or unknown check
// writes nothing.
func TestWorkflowTaskTimes(t *testing.T) {
	eachLayout(t, func(t *testing.T, single bool) {
		e := newWFEnv(t, single)
		ctx := context.Background()
		var clock atomic.Int64
		clock.Store(1_000_000)
		e.st.setClock(func() time.Time { return time.UnixMilli(clock.Load()) })
		tick := func() int64 { return clock.Add(1000) }
		task := func(wf, name string) contract.TaskRecord {
			t.Helper()
			for _, tr := range e.state(t, wf).Tasks {
				if tr.Name == name {
					return tr
				}
			}
			t.Fatalf("no task %s/%s", wf, name)
			return contract.TaskRecord{}
		}
		want := func(what string, tr contract.TaskRecord, enqueued, started, finished int64) {
			t.Helper()
			if got, want := times(tr), [3]int64{enqueued, started, finished}; got != want {
				t.Errorf("%s: times %v, want %v", what, got, want)
			}
		}
		runnable := func(wf, name string, undo, want bool) {
			t.Helper()
			if ok, err := e.st.Runnable(ctx, wf, name, undo); err != nil || ok != want {
				t.Fatalf("Runnable %s/%s undo %t = %t %v, want %t", wf, name, undo, ok, err, want)
			}
		}

		t0 := clock.Load()
		plan := planWF("w", withUndo(planTask("w", "a", e.a), "w", e.b), planTask("w", "b", e.a, "a"), planTask("w", "c", e.a, "b"))
		if err := e.st.Submit(ctx, plan); err != nil {
			t.Fatal(err)
		}
		want("root pushed at submit", task("w", "a"), t0, 0, 0)
		want("waiting", task("w", "b"), 0, 0, 0)
		want("undo not reached", *task("w", "a").Undo, 0, 0, 0)

		tick()
		runnable("w", "a", false, true)
		t1 := tick()
		runnable("w", "a", false, true)
		runnable("w", "b", false, false)
		want("last yes wins", task("w", "a"), t0, t1, 0)
		want("a refused check stamps nothing", task("w", "b"), 0, 0, 0)

		t2 := tick()
		_, ack := take(t, e.a)
		e.finish(t, e.a, ack, done("w", "a", ""))
		want("completed", task("w", "a"), t0, t1, t2)
		want("released by a's end", task("w", "b"), t2, 0, 0)

		t3 := tick()
		_, ack = take(t, e.a)
		e.finish(t, e.a, ack, contract.TaskEnd{Workflow: "w", Task: "b", Outcome: contract.OutcomeFailed})
		want("failed without a check", task("w", "b"), t2, 0, t3)
		want("cancelled while waiting", task("w", "c"), 0, 0, t3)
		want("undo pushed", *task("w", "a").Undo, t3, 0, 0)

		t4 := tick()
		runnable("w", "a", false, false)
		runnable("w", "a", true, true)
		t5 := tick()
		_, ack = take(t, e.b)
		e.finish(t, e.b, ack, contract.TaskEnd{Workflow: "w", Task: "a", Undo: true, Outcome: contract.OutcomeCompleted})
		want("undo ended", *task("w", "a").Undo, t3, t4, t5)
		want("the undo leaves the task's own times", task("w", "a"), t0, t1, t2)

		// Cancel ends the waiting task now, the queued one at its finish
		if err := e.st.Submit(ctx, planWF("x", planTask("x", "a", e.a), planTask("x", "b", e.a, "a"))); err != nil {
			t.Fatal(err)
		}
		t6 := tick()
		if err := e.st.Cancel(ctx, "x", nil); err != nil {
			t.Fatal(err)
		}
		runnable("x", "a", false, false)
		want("cancelled by Cancel", task("x", "b"), 0, 0, t6)
		want("queued under a cancel", task("x", "a"), t5, 0, 0)
		t7 := tick()
		_, ack = take(t, e.a)
		e.finish(t, e.a, ack, contract.TaskEnd{Workflow: "x", Task: "a", Outcome: contract.OutcomeCancelled})
		want("cancelled at its pre-run check", task("x", "a"), t5, 0, t7)

		// FailFast ends what waits; the queued task keeps only its push
		ff := planWF("f", planTask("f", "a", e.a), planTask("f", "b", e.a), planTask("f", "c", e.a, "b"))
		ff.FailFast = true
		if err := e.st.Submit(ctx, ff); err != nil {
			t.Fatal(err)
		}
		t8 := tick()
		_, ack = take(t, e.a)
		e.finish(t, e.a, ack, contract.TaskEnd{Workflow: "f", Task: "a", Outcome: contract.OutcomeFailed})
		want("dropped by FailFast", task("f", "c"), 0, 0, t8)
		want("queued, stopped", task("f", "b"), t7, 0, 0)

		// A check of an unknown workflow or task creates no row
		rows := func() (n int) {
			t.Helper()
			if err := e.st.f.db.QueryRow(`SELECT (SELECT COUNT(*) FROM ` + wfWorkflows + `) + (SELECT COUNT(*) FROM ` + wfTasks + `)`).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n
		}
		before := rows()
		for _, undo := range []bool{false, true} {
			if ok, err := e.st.Runnable(ctx, "ghost", "a", undo); ok || !errors.Is(err, contract.ErrWorkflowNotFound) {
				t.Errorf("Expected ErrWorkflowNotFound for an unknown workflow, got %t %v", ok, err)
			}
			runnable("f", "ghost", undo, false)
		}
		if n := rows(); n != before {
			t.Errorf("Expected no row made by a check, got %d rows, had %d", n, before)
		}
	})
}

// Another connection (as another process) holds the write lock: the check
// still answers from a read, fast; a yes stays yes when its stamp cannot be
// written.
func TestRunnableUnderForeignLock(t *testing.T) {
	eachLayout(t, func(t *testing.T, single bool) {
		e := newWFEnv(t, single)
		ctx := context.Background()
		if err := e.st.Submit(ctx, planWF("x", planTask("x", "a", e.a), planTask("x", "b", e.a, "a"))); err != nil {
			t.Fatal(err)
		}
		if err := e.st.Cancel(ctx, "x", nil); err != nil {
			t.Fatal(err)
		}
		if err := e.st.Submit(ctx, planWF("y", planTask("y", "a", e.a))); err != nil {
			t.Fatal(err)
		}
		other, err := sql.Open("sqlite3", e.st.f.key+"?_txlock=immediate&_busy_timeout=100")
		if err != nil {
			t.Fatal(err)
		}
		defer other.Close()
		tx, err := other.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := tx.Exec(`UPDATE ` + wfWorkflows + ` SET hash = hash`); err != nil {
			t.Fatal(err)
		}

		start := time.Now()
		if ok, err := e.st.Runnable(ctx, "x", "a", false); ok || err != nil {
			t.Errorf("Expected false, nil for a cancelled workflow's task, got %t %v", ok, err)
		}
		if d := time.Since(start); d > time.Second {
			t.Errorf("Expected a no without waiting for the lock, took %v", d)
		}
		// ctx ends between answer and stamp (else the stamp waits the 5s
		// busy timeout): the stamp fails, the yes stays
		yctx, cancel := context.WithCancel(ctx)
		defer cancel()
		e.st.setClock(func() time.Time {
			if inRunnable() {
				cancel()
			}
			return time.Now()
		})
		ok, err := e.st.Runnable(yctx, "y", "a", false)
		e.st.setClock(time.Now)
		if !ok || err != nil {
			t.Errorf("Expected true, nil with the stamp failed, got %t %v", ok, err)
		}
	})
}

// inRunnable: the caller runs inside WorkflowStore.Runnable (a clock hook
// that must not fire for the store's loop).
func inRunnable() bool {
	buf := make([]byte, 8<<10)
	return strings.Contains(string(buf[:runtime.Stack(buf, false)]), "(*WorkflowStore).Runnable")
}

// A task that ends between the check's read and its stamp keeps no start
// time; a later check answers no and stamps nothing.
func TestRunnableStampGuard(t *testing.T) {
	eachLayout(t, func(t *testing.T, single bool) {
		e := newWFEnv(t, single)
		ctx := context.Background()
		if err := e.st.Submit(ctx, planWF("w", planTask("w", "a", e.a))); err != nil {
			t.Fatal(err)
		}
		end := func() {
			if _, err := e.st.f.db.Exec(`UPDATE ` + wfTasks + ` SET state = 'completed' WHERE workflow_id = 'w' AND name = 'a'`); err != nil {
				t.Error(err)
			}
		}
		started := func() (n int64) {
			t.Helper()
			if err := e.st.f.db.QueryRow(`SELECT started_at FROM ` + wfTasks + ` WHERE workflow_id = 'w' AND name = 'a'`).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n
		}
		// the clock is read after the answer, before the stamp: end the task there
		e.st.setClock(func() time.Time {
			if inRunnable() {
				end()
			}
			return time.Now()
		})
		ok, err := e.st.Runnable(ctx, "w", "a", false)
		e.st.setClock(time.Now)
		if !ok || err != nil {
			t.Fatalf("Expected true, nil from the read before the end, got %t %v", ok, err)
		}
		if n := started(); n != 0 {
			t.Errorf("Expected no start time on an ended task, got %d", n)
		}
		if ok, err := e.st.Runnable(ctx, "w", "a", false); ok || err != nil {
			t.Errorf("Expected false, nil after the end, got %t %v", ok, err)
		}
		if n := started(); n != 0 {
			t.Errorf("Expected no start time from a late check, got %d", n)
		}
	})
}

// Folder layout, workflow file there but no store can open it: Purge keeps
// the jobs (else their workflows stay running for ever). No workflow file,
// or a single file with no workflow tables: a plain purge.
func TestPurgeKeepsJobsWithoutStore(t *testing.T) {
	t.Run("folder", func(t *testing.T) {
		dir := t.TempDir()
		qs := mustNew(t, dir)
		defer qs.Close()
		a, err := qs.NewQueue("a")
		if err != nil {
			t.Fatal(err)
		}
		a.Enqueue("j1")
		a.Enqueue("j2")
		wfPath := filepath.Join(dir, workflowsFile)
		if err := os.WriteFile(wfPath, []byte(strings.Repeat("not a database ", 512)), 0o600); err != nil {
			t.Fatal(err)
		}
		a.Purge()
		if n := a.Len(); n != 2 {
			t.Errorf("Expected the jobs kept when the workflow file cannot open, got len %d", n)
		}
		if err := os.Remove(wfPath); err != nil {
			t.Fatal(err)
		}
		a.Purge()
		if n := a.Len(); n != 0 {
			t.Errorf("Expected a plain purge with no workflow file, got len %d", n)
		}
	})
	t.Run("single", func(t *testing.T) {
		qs := mustNew(t, filepath.Join(t.TempDir(), "all.db"), WithSingleFile())
		defer qs.Close()
		a, err := qs.NewQueue("a")
		if err != nil {
			t.Fatal(err)
		}
		a.Enqueue("j1")
		a.Purge()
		if n := a.Len(); n != 0 {
			t.Errorf("Expected a plain purge with no workflow tables, got len %d", n)
		}
	})
}
