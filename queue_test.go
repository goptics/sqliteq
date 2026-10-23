package sqliteq

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
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
