package main

import (
	"fmt"
	"os"
	"time"

	"github.com/goptics/sqliteq"
)

func priorityQueueExample() {
	// Create a temporary database
	dbPath := "./priority_queue_example.db"
	defer os.Remove(dbPath)

	// Initialize queue manager
	queueManager := sqliteq.New(dbPath)
	defer queueManager.Close()

	// Create a priority queue
	priorityQueue, err := queueManager.NewPriorityQueue("tasks")
	if err != nil {
		panic(fmt.Sprintf("Failed to create priority queue: %v", err))
	}

	// Add items with different priorities
	type Task struct {
		ID          int
		Description string
	}

	fmt.Println("Adding items to priority queue with different priorities:")

	// Add high priority tasks (0 - highest priority)
	priorityQueue.Enqueue(Task{1, "High priority task 1"}, 0)
	fmt.Println("- Added: High priority task 1 (priority 0 - highest)")

	// Add medium priority tasks (5)
	priorityQueue.Enqueue(Task{2, "Medium priority task 1"}, 5)
	fmt.Println("- Added: Medium priority task 1 (priority 5)")

	// Add more high priority tasks
	priorityQueue.Enqueue(Task{3, "High priority task 2"}, 0)
	fmt.Println("- Added: High priority task 2 (priority 0 - highest)")

	// Add low priority tasks (10 - lowest priority)
	priorityQueue.Enqueue(Task{4, "Low priority task 1"}, 10)
	fmt.Println("- Added: Low priority task 1 (priority 10 - lowest)")
	priorityQueue.Enqueue(Task{5, "Low priority task 2"}, 10)
	fmt.Println("- Added: Low priority task 2 (priority 10 - lowest)")

	// Dequeue items - they should come out in priority order (lower priority numbers first)
	fmt.Println("\nDequeuing items from priority queue:")
	for i := 0; i < 5; i++ {
		time.Sleep(100 * time.Millisecond) // Small delay for demonstration
		item, success := priorityQueue.Dequeue()
		if !success {
			fmt.Println("- Queue is empty")
			break
		}

		// When items are serialized/deserialized through JSON, they come back as map[string]interface{}
		if taskMap, ok := item.(map[string]interface{}); ok {
			// Extract ID and description from the map
			id := int(taskMap["ID"].(float64))
			desc := taskMap["Description"].(string)
			fmt.Printf("- Dequeued: ID %d - %s\n", id, desc)
		} else {
			fmt.Printf("- Dequeued item with unexpected type: %T\n", item)
		}
	}

	fmt.Println("\nPriority queue example completed!")
}
