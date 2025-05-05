# SQLiteQ: A Thread-Safe SQLite-Based Queue for Go

SQLiteQ is a thread-safe, persistent queue implementation in Go using SQLite as the storage backend. It provides constant-time enqueue and dequeue operations and maintains persistence across application restarts.

## Features

- Thread-safe queue operations using mutexes and SQLite transactions
- Constant-time enqueue and dequeue operations
- Persistence via SQLite storage
- Support for acknowledgment-based processing
- Simple and clean API following the Queue interface

## Installation

```bash
go get github.com/fahimfaisaal/sqliteq
```

## Usage

```go
package main

import (
    "fmt"
    "log"

    "github.com/fahimfaisaal/sqliteq"
)

func main() {
    // Create a new SQLite queue
    // The first parameter is the path to the SQLite database file
    // The second parameter is the name of the table to use for the queue
    queue, err := sqliteq.NewSQLiteQueue("queue.db", "my_queue")
    if err != nil {
        log.Fatalf("Failed to create queue: %v", err)
    }
    defer queue.Close()

    // Enqueue items
    queue.Enqueue("item 1")
    queue.Enqueue(42)
    queue.Enqueue(map[string]any{"key": "value"})

    // Get queue length
    fmt.Printf("Queue length: %d\n", queue.Len())

    // Get all pending items
    items := queue.Values()
    fmt.Printf("All items: %v\n", items)

    // Simple dequeue
    item, success := queue.Dequeue()
    if success {
        fmt.Printf("Dequeued item: %v\n", item)
    }

    // Dequeue with acknowledgment
    item, success, ackID := queue.DequeueWithAckId()
    if success {
        fmt.Printf("Dequeued item: %v with ack ID: %s\n", item, ackID)

        // Process the item...

        // Acknowledge the item after processing
        acknowledged := queue.Acknowledge(ackID)
        fmt.Printf("Item acknowledged: %v\n", acknowledged)
    }

    // Purge the queue
    queue.Purge()
}
```

## How It Works

SQLiteQ uses a SQLite database to store queue items with the following schema:

- `id`: Unique identifier for each item (autoincrement primary key)
- `data`: The serialized item data (stored as a JSON blob)
- `status`: The status of the item ("pending", "processing", or "completed")
- `ack_id`: A unique ID for acknowledging processed items
- `created_at`: When the item was added to the queue
- `updated_at`: When the item was last updated

## Performance Considerations

- The queue is optimized for constant-time enqueue and dequeue operations
- SQLite's WAL (Write-Ahead Logging) mode is enabled for better concurrent access
- Proper indexing is set up on the status and creation time columns for efficient querying

## License

MIT
