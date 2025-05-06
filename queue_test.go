package sqliteq

import (
	"fmt"
	"os"
	"testing"
)

func TestSQLiteQueue(t *testing.T) {
	// Create a temporary database file
	dbPath := "test_queue.db"

	// Cleanup after test
	defer os.Remove(dbPath)
	queues := New(dbPath)
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
		row := q.client.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = 'completed' OR status = 'processing'", q.tableName))
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
		// Create a temporary database file
		dbPath := "test_keep_completed.db"
		defer os.Remove(dbPath)

		// Create a queue with removeOnComplete = false
		queues := New(dbPath)
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
		row := q.client.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = 'completed'", q.tableName))
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
		// Create a temporary database file
		dbPath := "test_remove_completed.db"
		defer os.Remove(dbPath)

		queues := New(dbPath)
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
		row := q.client.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", q.tableName))
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
		row = q.client.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = 'processing'", q.tableName))
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
		row = q.client.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", q.tableName))
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
	// Create a temporary database file
	dbPath := "test_concurrent.db"

	// Cleanup after test
	defer os.Remove(dbPath)

	// Create a new queue
	queues := New(dbPath)
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
