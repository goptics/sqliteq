<div align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="./assets/text-logo-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="./assets/text-logo.svg">
  <img alt="sqliteq" width="320px" src="./assets/text-logo.svg">
</picture>

  <p>
    <a href="https://pkg.go.dev/github.com/goptics/sqliteq"><img src="https://img.shields.io/badge/go-pkg-00ADD8.svg?logo=go" alt="Go Reference" /></a>
    <a href="https://github.com/goptics/sqliteq/actions/workflows/sqliteq.yml"><img src="https://github.com/goptics/sqliteq/actions/workflows/sqliteq.yml/badge.svg" alt="CI" /></a>
    <a href="https://codecov.io/gh/goptics/sqliteq"><img src="https://codecov.io/gh/goptics/sqliteq/branch/main/graph/badge.svg" alt="Codecov" /></a>
    <a href="https://golang.org/doc/devel/release.html"><img src="https://img.shields.io/badge/Go-1.24+-00ADD8?style=flat-square&logo=go" alt="Go Version" /></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square" alt="License" /></a>
  </p>

  <p><strong>SQLite-backed FIFO and priority queues for Go, with acknowledgments, batch pops, group commit and one file per queue.</strong></p>

  <p align="center">
    <a href="#-features">Features</a> ·
    <a href="#installation">Installation</a> ·
    <a href="#quick-start">Quick Start</a> ·
    <a href="#usage">Usage</a> ·
    <a href="#storage">Storage</a> ·
    <a href="#configuration">Configuration</a> ·
    <a href="#delivery-guarantees">Delivery Guarantees</a> ·
    <a href="#upgrading-from-v02">Upgrading</a> ·
    <a href="#development">Development</a> ·
    <a href="#contributing">Contributing</a>
  </p>
</div>

---

## ✨ Features

- **FIFO and priority queues** stored in SQLite; a lower priority number is served first.
- **One file per queue** by default, so busy queues never wait on each other's write lock. `WithSingleFile()` keeps every queue in one file.
- **Acknowledgments**: items taken with an ack id that are never acknowledged go back to the queue when the file is next opened.
- **Batch dequeue** of many items in one SQL statement, each under its own ack id.
- **Group commit**: concurrent `Enqueue` and `Acknowledge` calls on one file share one transaction.
- **Deduplicated enqueue**: `EnqueueOnce` stores a job once per id while its row exists.
- **Automatic WAL checkpoints**, tunable with `WithCheckpointPages`.
- **Thread-safe**: share one queue object across goroutines.
- **Persistence adapter for [varmq](https://github.com/goptics/varmq)**.

## Installation

```bash
go get github.com/goptics/sqliteq
```

Requires Go 1.24+ and cgo: sqliteq uses [mattn/go-sqlite3](https://github.com/mattn/go-sqlite3), so building it needs `CGO_ENABLED=1` and a C compiler.

## Quick Start

```go
package main

import (
	"fmt"
	"log"

	"github.com/goptics/sqliteq"
)

func main() {
	qs, err := sqliteq.New("data") // a folder, created if missing
	if err != nil {
		log.Fatal(err)
	}
	defer qs.Close()

	queue, err := qs.NewQueue("jobs") // stored in data/jobs.db
	if err != nil {
		log.Fatal(err)
	}

	queue.Enqueue([]byte("hello world")) // any value database/sql accepts; false for others

	if data, ok := queue.Dequeue(); ok {
		fmt.Println(string(data.([]byte))) // items come back as []byte
	}
}
```

## Usage

### Queue types

| Type            | Constructor                 | Order                          |
| --------------- | --------------------------- | ------------------------------ |
| `Queue`         | `qs.NewQueue(name)`         | FIFO                           |
| `PriorityQueue` | `qs.NewPriorityQueue(name)` | Lowest first, FIFO among equal |

Both types support the acknowledgment, dedup and batch APIs below. Basic operations:

```go
queue.Enqueue([]byte("message"))
queue.Len()    // pending items
queue.Values() // pending items, in queue order
queue.Purge()  // delete every item
```

While a handle is open, the same name returns the same queue handle, in any letter case. Asking for `NewQueue("x")` after `NewPriorityQueue("x")` (or the reverse) is an error, and so is reaching a live queue again with different options. `Queue.Close` stops further `Enqueue` and `Dequeue` calls on that handle but still accepts `Acknowledge`, so jobs that are still running can finish. The file stays open until `Queues.Close`.

### Priority queue

```go
pq, err := qs.NewPriorityQueue("emails")
if err != nil {
	log.Fatal(err)
}

pq.Enqueue([]byte("newsletter"), 3)
pq.Enqueue([]byte("password reset"), 1)

data, ok := pq.Dequeue() // "password reset"
```

### Acknowledgments

```go
item, ok, ackID := queue.DequeueWithAckId()
if ok {
	process(item)
	queue.Acknowledge(ackID)
}
```

An item taken with `DequeueWithAckId` is marked as processing until it is acknowledged. The first time a queue is opened after its file is opened in the process, every item still marked as processing goes back to pending, so a job that was running when the process stopped runs again. There is no visibility timeout: unacknowledged items return only when the file is next opened, not when a closed queue is opened again on a file that is still open.

By default an acknowledged item is deleted. With `WithRemoveOnComplete(false)` it is kept and marked as completed:

```go
audit, err := qs.NewQueue("audit", sqliteq.WithRemoveOnComplete(false))
```

### Enqueue once per job id

`EnqueueOnce(item, id)` on a queue, and `EnqueueOnce(item, priority, id)` on a priority queue, add an item under an id unless a row with that id is already stored. A repeat stores nothing and returns true, so an add that is retried with the same id is stored once.

```go
queue.EnqueueOnce([]byte("send invoice 42"), "invoice-42") // stored
queue.EnqueueOnce([]byte("send invoice 42"), "invoice-42") // true, nothing stored

pq.EnqueueOnce([]byte("rebuild index"), 1, "rebuild-index")
```

The id is remembered while its row exists: pending, processing, or completed with `WithRemoveOnComplete(false)`. `Dequeue` deletes the row, and so does the ack with the default remove-on-complete; after that the id can be added again. `Purge` forgets every id. An empty id behaves like `Enqueue`.

### Batch dequeue

`DequeueBatchWithAckIds` takes up to `min(len(items), len(ackIDs))` items in one statement, each under its own ack id. `remaining` is the number of pending items left after it, counted up to 256; `Len` gives the exact count.

```go
items := make([]any, 8)
ackIDs := make([]string, 8)

for {
	n, remaining, err := queue.DequeueBatchWithAckIds(items, ackIDs)
	for i := range n {
		process(items[i])
		queue.Acknowledge(ackIDs[i])
	}
	if err != nil || remaining == 0 {
		break
	}
}
```

### With varmq

sqliteq implements varmq's persistent queue interfaces:

```go
q, err := qs.NewQueue("jobs")
if err != nil {
	log.Fatal(err)
}

worker := varmq.NewWorker(func(j varmq.Job[string]) {
	fmt.Println(j.Data())
})
defer worker.WaitAndStop()

queue := worker.WithPersistentQueue(q)
queue.Add("hello")
```

sqliteq does not import varmq. Batch pops and `EnqueueOnce` need varmq ≥ v1.10.0. From that release, a worker whose only source is the sqliteq queue claims several jobs in one statement through `DequeueBatchWithAckIds` (a worker bound to several queues claims one job per call), and a job added with an id (`WithJobId`, or one from `WithJobIdGenerator`) goes through `EnqueueOnce`. An older varmq calls neither: it claims one job per call and adds every job with `Enqueue`, which still works.

## Storage

### Layout on disk

By default `path` is a folder and every queue lives in its own SQLite file, named after the queue in lower case, with SQLite's `-wal` and `-shm` files beside it while the file is open (SQLite removes them when the last connection closes):

```
data/
├── emails.db
├── emails.db-shm
├── emails.db-wal
├── jobs.db
├── jobs.db-shm
└── jobs.db-wal
```

Each file has its own connection pool, its own write lock and its own checkpoints, so busy queues do not wait on each other. `New` only creates the folder; a file is opened (and created) the first time you ask for its queue.

### Single file

`WithSingleFile()` makes `path` one database file that holds every queue as a table. This is the v0.2 layout, so a file written by v0.2 opens with this option and keeps its data:

```go
qs, err := sqliteq.New("queue.db", sqliteq.WithSingleFile())
```

The file is created if it is missing, but its folder must already exist: `New` returns an error otherwise, unlike the folder layout, which creates its folder.

### Queue names

In the folder layout the queue name becomes a file name, so it may only contain `A-Z a-z 0-9 . _ -`, must not be empty, and must not start with `sqlite_` (SQLite reserves that prefix). Anything else is an error from `NewQueue` / `NewPriorityQueue`. On Windows the folder layout also cannot use a reserved device name as a queue name (`CON`, `PRN`, `AUX`, `NUL`, `COM1` to `COM9`, `LPT1` to `LPT9`, in any letter case), because the name would become a file name such as `con.db`. sqliteq does not check for these. With `WithSingleFile()` the name is only a table name, so any name SQLite accepts as a table name works, with two exceptions: SQLite reserves names that start with `sqlite_`, and since a queue `<name>` creates the indexes `<name>__sqliteq_pending` and `<name>__sqliteq_job_id` (SQLite keeps tables and indexes in one namespace), a queue named like one of those cannot open in the same file as `<name>`; whichever opens second gets an error.

Names are case-insensitive in both layouts: `Jobs` and `jobs` are the same queue, stored in `jobs.db` in the folder layout.

### One process per file

Run one process per database file. sqliteq keeps a registry in the process, keyed by the file's real path (made absolute, with symlinks resolved). Every `New` in the process that reaches the same file shares one connection pool and one write lock, so two `New` calls on one path never write through two competing pools. Two `Queues` values can therefore hold the same queue, and you get the same handle from both. The file closes when the last `Queues` using it is closed.

Another process is not coordinated with this one. The first time a process opens a queue, it puts every item still marked as processing back to pending, so a second process opening a file that is in use would rerun the first process's running jobs. sqliteq does not take an OS lock to stop that.

On macOS, spell a path the same way every time: the registry key keeps the letter case you pass, so `Data` and `data` would be two entries for one folder. On Windows, Go resolves existing folders and files to their on-disk letter case, so different spellings of them share one entry.

## Configuration

| Option                        | Passed to                      | Default       | Effect                                                                 |
| ----------------------------- | ------------------------------ | ------------- | ---------------------------------------------------------------------- |
| `WithSingleFile()`            | `New`                          | folder layout | `path` is one database file holding every queue                        |
| `WithCheckpointPages(n)`      | `New`                          | `1000`        | WAL size, in pages, at which SQLite checkpoints automatically          |
| `WithRemoveOnComplete(false)` | `NewQueue`, `NewPriorityQueue` | `true`        | Keep acknowledged items, marked as completed, instead of deleting them |

`New` returns an error instead of panicking: for a path it cannot use, an invalid option (`WithCheckpointPages` below 1, or a nil option), or a path that is an existing file when `WithSingleFile()` is not given. `NewQueue` and `NewPriorityQueue` return an error for a nil option too.

### Checkpoints

SQLite appends writes to a write-ahead log (WAL) and folds them back into the database file in a checkpoint. `WithCheckpointPages(n)` sets how many pages the WAL may reach before SQLite checkpoints on its own. The default is 1000 pages, SQLite's own setting. A page is 4 KB, so a checkpoint starts once the WAL reaches about 4 MB. That size is where a checkpoint is triggered, not a hard limit on the file. A checkpoint runs inside the commit of the write that crosses that size, while that write holds the file's write lock. So with the default, any write on that file (an `Enqueue`, a claim, an ack or a purge) occasionally waits for a checkpoint's disk sync.

```go
qs, err := sqliteq.New("data", sqliteq.WithCheckpointPages(10000))
```

Raising it makes those waits rarer, which cuts `Enqueue` tail latency and raises throughput, at the price of a larger WAL on disk: 10000 pages triggers a checkpoint at about 41 MB.

The WAL is a log, not extra stored data: the database holds the same items either way. Everything that reaches one file must use the same value: a `NewQueue` or `NewPriorityQueue` that reaches a file already open with a different value returns an error naming the value the file is open with. `New` itself does not open files, so it does not detect this.

## Delivery Guarantees

| Call                                         | Guarantee                   | If the process stops                                       |
| -------------------------------------------- | --------------------------- | ---------------------------------------------------------- |
| `DequeueWithAckId`, `DequeueBatchWithAckIds` | At least once               | Unacknowledged items go back to pending on the next open   |
| `Dequeue`                                    | At most once                | The item is deleted when it is returned                    |
| `EnqueueOnce`                                | Once per id                 | Stored once while its row exists; the id survives recovery |
| `Enqueue`                                    | Stored when it returns true | Survives a process crash                                   |

- Use `DequeueWithAckId` or `DequeueBatchWithAckIds` (varmq's persistent queue claims through one of them) for jobs you can't lose.
- Use globally unique ids with `EnqueueOnce` (UUID or nanoid, never a per-process counter).

sqliteq runs SQLite in WAL mode with `synchronous=NORMAL`, unchanged from v0.2. A job whose `Enqueue` returned true survives a crash of the process. After a power loss or an operating system crash, the last commits before the failure may be lost, because SQLite rolls back what the WAL had not yet synced; the database itself stays consistent.

## Upgrading from v0.2

- `New` returns `(Queues, error)` instead of panicking, and takes store options. `NewQueue` and `NewPriorityQueue` keep their signatures.
- `New(path)` now treats `path` as a folder with one file per queue. Add `WithSingleFile()` to keep reading an existing v0.2 file.
- `path` is a file system path, not a DSN. A `file:` URI or `:memory:` is taken as a literal file name and opens a new empty database; a `?` or `#` in the path is an error. Pass the plain path of a v0.2 file.
- While a queue is open, the same name returns the same handle instead of a new one, so `Queue.Close` on it closes it for every holder. Asking for it with different options, or as the other kind of queue, is an error.
- A v0.2 table is migrated when it opens: its old indexes are replaced with a partial index on pending rows, and a `job_id` column is added with a unique index over the rows that have one. Its data is kept.
- Ack ids are row ids. An ack id from v0.2 no longer matches and returns false.
- Items come out in `id` order (a priority queue's in `priority`, then `id`), not `created_at`.
- `RequeueNoAckRows` is no longer exported, and nothing replaces it: unacknowledged items are requeued the first time a queue is opened after its file is opened in the process, not on every `NewQueue` call.

## How It Works

Each queue is a table with these columns:

- `id`: autoincrement primary key. Items come out in `id` order, and a priority queue's in `priority`, then `id` order. The ack id `DequeueWithAckId` returns is this id in decimal.
- `priority`: priority queues only; a lower number comes out first. Opening an older table as a priority queue adds it.
- `data`: the stored item.
- `status`: `pending`, `processing` or `completed`.
- `ack`: 0 when an item is added, 1 once it is acknowledged and kept with `WithRemoveOnComplete(false)`. On open, only `processing` rows with `ack` 0 go back to pending.
- `ack_id`: kept from v0.2 so old tables need no rebuild; no longer used.
- `job_id`: the id an item was added with through `EnqueueOnce`, unique among the rows that have one; NULL for `Enqueue`. Opening an older table adds it.
- `created_at`, `updated_at`: when the item was added and last changed.

On top of that table:

- Two partial indexes: one covers the pending items in queue order, and a unique one covers `job_id` on the rows that have one, which `EnqueueOnce` relies on.
- A claim or a pop is one `UPDATE ... RETURNING` or `DELETE ... RETURNING` statement.
- Writes to one file (adds, claims, acks, purges) take one lock in the process, so they wait for each other in Go rather than in SQLite's busy handler. `Len` and `Values` do not take it.
- Concurrent `Enqueue` and `Acknowledge` calls on one file are committed together in one transaction. Each call still gets its own result, and if the shared transaction fails, each call is retried on its own so one bad call fails alone.

## Development

```bash
go test -race ./...  # tests, with the race detector
go run ./example     # a runnable example
gofmt -l .           # formatting
```

Building and testing need cgo (`CGO_ENABLED=1`) and a C compiler.

## API Reference

See [GoDoc](https://pkg.go.dev/github.com/goptics/sqliteq).

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=goptics/sqliteq&type=Date)](https://www.star-history.com/#goptics/sqliteq&Date)

## Contributing

Open an issue to discuss a change, or send a pull request against `main` with tests (`go test -race ./...`).

## License

[MIT](LICENSE)
