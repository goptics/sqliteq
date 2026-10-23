package sqliteq

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
		for got := 0; got < total; {
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
