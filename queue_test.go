package sqliteq

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
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
	db, err := sql.Open(driverName(q.f.pages), dsn(q.f.key))
	if err != nil {
		t.Fatal(err)
	}
	f := &file{key: q.f.key, db: db, pages: q.f.pages, refs: 1, handles: map[string]*handle{}, recovered: map[string]bool{}}
	f.b = &batcher{commit: f.commitRound}
	q2, err := newQueue(f, q.name, q.priority, WithRemoveOnComplete(q.removeOnComplete))
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		q2.Close()
		db.Close()
	})
	return q2
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
		_ varmq.QueuePauser    = (*Queue)(nil)
		_ varmq.QueuePauser    = (*PriorityQueue)(nil)
		_ varmq.PauseNotifier  = (*Queue)(nil)
		_ varmq.PauseNotifier  = (*PriorityQueue)(nil)
		_ varmq.QueueInspector = (*Queue)(nil)
		_ varmq.QueueInspector = (*PriorityQueue)(nil)
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
	q.Purge()
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
	handle := w.WithPersistentQueue(q)
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
