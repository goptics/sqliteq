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
    // The parameter is a folder, created if missing. Each queue gets its own
    // database file in it: data/my_queue.db, data/my_priority_queue.db, ...
    queuesManager, err := sqliteq.New("data")
    if err != nil {
        log.Fatalf("Failed to open queues: %v", err)
    }
    defer queuesManager.Close()

    // Create a new queue
    // The parameter is the name of the queue; it becomes the table and file name
    queue, err := queuesManager.NewQueue("my_queue")
    if err != nil {
        log.Fatalf("Failed to create queue: %v", err)
    }

    // You can also create a queue with custom options
    // For example, to keep acknowledged items in the database:
    queueWithOptions, err := queuesManager.NewQueue("my_other_queue",
        sqliteq.WithRemoveOnComplete(false)) // Set to false to keep acknowledged items
    if err != nil {
        log.Fatalf("Failed to create queue: %v", err)
    }
    _ = queueWithOptions

    // Create a priority queue
    priorityQueue, err := queuesManager.NewPriorityQueue("my_priority_queue")
    if err != nil {
        log.Fatalf("Failed to create priority queue: %v", err)
    }
    _ = priorityQueue

    // Enqueue items
    queue.Enqueue([]byte("item 1"))
    queue.Enqueue([]byte("item 2"))

    // Get queue length
    fmt.Printf("Queue length: %d\n", queue.Len())

    // Get all pending items
    items := queue.Values()
    fmt.Printf("All items: %v\n", items)

    // Simple dequeue
    item, success := queue.Dequeue()
    if success {
        fmt.Printf("Dequeued item: %v\n", string(item.([]byte)))
    }

    // Dequeue with acknowledgment
    item, success, ackID := queue.DequeueWithAckId()
    if success {
        fmt.Printf("Dequeued item: %v with ack ID: %s\n", string(item.([]byte)), ackID)

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

`New` returns an error instead of panicking: for a path it cannot use, an invalid option, or a path that is an existing file when `WithSingleFile()` is not given.

The same name always returns the same queue handle, in any letter case. Asking for `NewQueue("x")` after `NewPriorityQueue("x")` (or the reverse) is an error, and so is reaching a live queue again with different options. `Queue.Close` stops further `Enqueue` and `Dequeue` calls on that handle but still accepts `Acknowledge`, so jobs that are still running can finish. The file stays open until `Queues.Close`.

## Layout on disk

By default `path` is a folder and every queue lives in its own SQLite file, named after the queue in lower case, with SQLite's `-wal` and `-shm` files beside it:

```
data/
├── my_queue.db
├── my_queue.db-shm
├── my_queue.db-wal
├── my_other_queue.db
├── my_other_queue.db-shm
├── my_other_queue.db-wal
├── my_priority_queue.db
├── my_priority_queue.db-shm
└── my_priority_queue.db-wal
```

Each file has its own connection pool, its own write lock and its own checkpoints, so busy queues do not wait on each other. `New` only creates the folder; a file is opened (and created) the first time you ask for its queue.

### Single file

`WithSingleFile()` makes `path` one database file that holds every queue as a table. This is the v0.2 layout, so a file written by v0.2 opens with this option and keeps its data:

```go
queuesManager, err := sqliteq.New("queue.db", sqliteq.WithSingleFile())
if err != nil {
    log.Fatalf("Failed to open queues: %v", err)
}
defer queuesManager.Close()
```

If you upgrade from v0.2, add `WithSingleFile()` to keep reading your existing file, or start with the folder layout for new data.

### Queue names

In the folder layout the queue name becomes a file name, so it may only contain `A-Z a-z 0-9 . _ -`, must not be empty, and must not start with `sqlite_` (SQLite reserves that prefix). Anything else is an error from `NewQueue` / `NewPriorityQueue`. With `WithSingleFile()` any name works, since it is only a table name.

Names are case-insensitive in both layouts: `Jobs` and `jobs` are the same queue, stored in `jobs.db` in the folder layout.

## One process per file

Run one process per database file. sqliteq keeps a registry in the process, keyed by the file's real path (made absolute, with symlinks resolved). Every `New` in the process that reaches the same file shares one connection pool and one write lock, so two `New` calls on one path never write through two competing pools. Two `Queues` values can therefore hold the same queue, and you get the same handle from both. The file closes when the last `Queues` using it is closed.

Another process is not coordinated with this one. When a queue opens, it puts every item still marked as processing back to pending, so a second process opening a file that is in use would rerun the first process's running jobs. sqliteq does not take an OS lock to stop that.

On case-insensitive filesystems (macOS, Windows), spell a path the same way every time: the registry key is case-sensitive, so `Data` and `data` would be two entries for one folder.

## Checkpoints

SQLite appends writes to a write-ahead log (WAL) and folds them back into the database file in a checkpoint. `WithCheckpointPages(n)` sets how many pages the WAL may reach before SQLite checkpoints on its own. The default is 1000 pages, SQLite's own setting, which keeps the WAL near 4 MB per file. With it, an `Enqueue` occasionally waits for a checkpoint's disk sync.

```go
queuesManager, err := sqliteq.New("data", sqliteq.WithCheckpointPages(10000))
```

Raising it makes those waits rarer, which cuts `Enqueue` tail latency and raises throughput, at the price of a larger WAL on disk. For example:

- 4000 pages: the WAL peaks near 16 MB per file.
- 10000 pages: the WAL peaks near 41 MB per file.

The WAL is a log, not extra stored data: the database holds the same items either way. `n` must be at least 1, because `New` returns an error otherwise. Everything that reaches one file must use the same value: a `NewQueue` or `NewPriorityQueue` that reaches a file already open with a different value returns an error naming the value the file is open with. `New` itself does not open files, so it does not detect this.

## Compatibility

sqliteq does not import varmq. Batch pops and `EnqueueOnce` are being added for v0.3 and need varmq ≥ v1.10.0.

## How It Works

SQLiteQ uses a SQLite database to store queue items with the following schema:

- `id`: Unique identifier for each item (autoincrement primary key). Items come out in `id` order, and a priority queue's in `priority`, then `id` order. The ack id `DequeueWithAckId` returns is this id in decimal
- `data`: The serialized item data (stored as a JSON blob)
- `status`: The status of the item ("pending", "processing", or "completed")
- `ack_id`: Kept from v0.2 so old tables need no rebuild; no longer used
- `created_at`: When the item was added to the queue
- `updated_at`: When the item was last updated

> NOTE: By default, when an item is acknowledged, it is removed from the database. However, you can configure the queue to keep acknowledged items by using the `WithRemoveOnComplete(false)` option when creating the queue. In this case, acknowledged items will be marked as "completed" but will remain in the database.

## Performance Considerations

- The queue is optimized for efficient enqueue and dequeue operations that scale well with queue size
- Operations leverage SQLite's indexing for logarithmic time complexity rather than true constant-time
- SQLite's WAL (Write-Ahead Logging) mode is enabled for better concurrent access
- One partial index covers the pending items in queue order; opening a v0.2 table replaces its old indexes with it

## 👤 Author

- GitHub: [@fahimfaisaal](https://github.com/fahimfaisaal)
- LinkedIn: [in/fahimfaisaal](https://www.linkedin.com/in/fahimfaisaal/)
- Twitter: [@FahimFaisaal](https://twitter.com/FahimFaisaal)
