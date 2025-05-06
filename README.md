# SQLiteQ: A SQLite-Based Persistent Queue for Go

[![Go Reference](https://img.shields.io/badge/go-pkg-00ADD8.svg?logo=go)](https://pkg.go.dev/github.com/goptics/sqliteq)
[![Go Report Card](https://goreportcard.com/badge/github.com/goptics/sqliteq)](https://goreportcard.com/report/github.com/goptics/sqliteq)
[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?style=flat-square&logo=go)](https://golang.org/doc/devel/release.html)
[![CI](https://github.com/goptics/sqliteq/actions/workflows/sqliteq.yml/badge.svg)](https://github.com/goptics/sqliteq/actions/workflows/go.yml)
[![codecov](https://codecov.io/gh/goptics/sqliteq/branch/main/graph/badge.svg)](https://codecov.io/gh/goptics/sqliteq)

SQLiteQ is a persistent queue implementation in Go using SQLite as the storage backend. It provides efficient enqueue and dequeue operations and maintains persistence across application restarts.

## Features

- Efficient enqueue and dequeue operations
- Persistence via SQLite storage
- Support for acknowledgment-based processing
- Simple and clean API following the Queue interface
- Built as a persistence adapter for [varmq](https://github.com/goptics/varmq)

## Installation

```bash
go get github.com/goptics/sqliteq
```

## Usage

```go
package main

import (
    "fmt"
    "log"

    "github.com/goptics/sqliteq"
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

    // You can also create a queue with custom options
    // For example, to keep acknowledged items in the database:
    queueWithOptions, err := sqliteq.NewSQLiteQueue("queue.db", "my_other_queue",
        sqliteq.WithRemoveOnComplete(false)) // Set to false to keep acknowledged items
    if err != nil {
        log.Fatalf("Failed to create queue: %v", err)
    }
    defer queueWithOptions.Close()

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

        // Note: By default, acknowledged items are removed from the database
        // With WithRemoveOnComplete(false), they would be marked as completed instead
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

> NOTE: By default, when an item is acknowledged, it is removed from the database. However, you can configure the queue to keep acknowledged items by using the `WithRemoveOnComplete(false)` option when creating the queue. In this case, acknowledged items will be marked as "completed" but will remain in the database.

## Performance Considerations

- The queue is optimized for efficient enqueue and dequeue operations that scale well with queue size
- Operations leverage SQLite's indexing for logarithmic time complexity rather than true constant-time
- SQLite's WAL (Write-Ahead Logging) mode is enabled for better concurrent access
- Proper indexing is set up on the status and creation time columns for efficient querying
