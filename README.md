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
    <a href="https://golang.org/doc/devel/release.html"><img src="https://img.shields.io/badge/Go-1.27+-00ADD8?style=flat-square&logo=go" alt="Go Version" /></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square" alt="License" /></a>
  </p>

  <p align="center">
    <a href="#features">Features</a> ·
    <a href="#quick-start">Quick Start</a> ·
    <a href="#usage">Usage</a> ·
    <a href="#how-it-works">How It Works</a> ·
    <a href="#trade-offs">Trade-offs</a> ·
    <a href="#delivery-guarantees">Delivery Guarantees</a> ·
    <a href="#development">Development</a>
  </p>

  <p>
    SQLite-backed FIFO and priority queues for Go, with acknowledgments, batch pops, job outcomes, job cancel, inspection and a durable pause.
  </p>
</div>

## Features

- **FIFO and priority queues** stored in SQLite; a lower priority number is served first.
- **One file per queue** by default, so busy queues never wait on each other's write lock. `WithSingleFile()` keeps every queue in one file.
- **Acknowledgments**: items taken with an ack ID that are never acknowledged go back to the queue when the file is next opened.
- **Batch dequeue** of many items in one SQL statement, each under its own ack ID.
- **Group commit**: concurrent `Enqueue` and `Acknowledge` calls on one file share one transaction.
- **Deduplicated enqueue**: `EnqueueOnce` stores a job once per job ID while its row exists.
- **Job outcomes**: `WatchOutcome` tells any process on the file when a job completes, fails or is cancelled, with the worker's result.
- **Job cancel**: `CancelJob` drops a waiting job, and `OnCancel` tells every process on the file to stop a running one.
- **Job records and inspection**: counts, totals and job lists for VarMQ's REST API and vartop.
- **Durable queue pause**: `Pause` stops the pops of every process on the file, and the pause outlives restarts.
- **Automatic WAL checkpoints**, tunable with `WithCheckpointPages`.
- **Thread-safe**: share one queue object across goroutines.
- **Storage for [VarMQ](https://github.com/goptics/varmq)** queues.

## Quick Start

```bash
go get github.com/goptics/sqliteq
```

Requires Go 1.27+ and cgo: sqliteq uses [mattn/go-sqlite3](https://github.com/mattn/go-sqlite3), so building it needs `CGO_ENABLED=1` and a C compiler.

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
	if err := process(item); err != nil {
		queue.Reject(ackID, err.Error()) // failed for good: not requeued
	} else {
		queue.Acknowledge(ackID)
	}
}
```

An item taken with `DequeueWithAckId` is marked as processing until it is acknowledged. The first time a queue is opened after its file is opened in the process, every item still marked as processing goes back to pending, so a job that was running when the process stopped runs again. There is no visibility timeout: unacknowledged items return only when the file is next opened, not when a closed queue is opened again on a file that is still open.

`Reject(ackID, reason)` finishes an item that failed for good, as VarMQ does when a job returns an error or panics. It is never requeued. When the job has a job ID, the reason is stored as its outcome error.

By default an acknowledged or rejected item is deleted. With `WithRemoveOnComplete(false)` it is kept and marked as `completed`, `failed` or `cancelled`:

```go
audit, err := qs.NewQueue("audit", sqliteq.WithRemoveOnComplete(false))
```

### Enqueue once per job ID

`EnqueueOnce(item, id)` on a queue, and `EnqueueOnce(item, priority, id)` on a priority queue, add an item under an id unless a row with that id is already stored. A repeat stores nothing and returns true, so an add that is retried with the same id is stored once.

```go
queue.EnqueueOnce([]byte("send invoice 42"), "invoice-42") // stored
queue.EnqueueOnce([]byte("send invoice 42"), "invoice-42") // true, nothing stored

pq.EnqueueOnce([]byte("rebuild index"), 1, "rebuild-index")
```

The id is remembered while its row exists: pending, processing, or completed or failed with `WithRemoveOnComplete(false)`. `Dequeue` deletes the row, and so does the ack or reject with the default remove-on-complete; after that the id can be added again. `Purge` forgets every id. An empty id behaves like `Enqueue`.

### Batch dequeue

`DequeueBatchWithAckIds` takes up to `min(len(items), len(ackIDs))` items in one statement, each under its own ack ID. `remaining` is the number of pending items left after it, counted up to 256; `Len` gives the exact count.

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

### With VarMQ

sqliteq implements VarMQ's queue interfaces, outcome, cancel, inspection and pause included:

```go
q, err := qs.NewQueue("jobs")
if err != nil {
	log.Fatal(err)
}

worker := varmq.NewWorker(func(j varmq.Job[string]) {
	fmt.Println(j.Data())
})
defer worker.WaitAndStop()

queue := worker.WithQueue(q)
queue.Add("hello")
```

Requires VarMQ ≥ v1.11.0. The [VarMQ versions table](https://varmq.goptics.org/start/installation/#versions) shows which release supports which feature.

sqliteq imports VarMQ's `contract` module, and it imports VarMQ itself only in tests. Its `go.mod` still requires VarMQ, so sqliteq needs Go 1.27, as VarMQ does.

A worker whose only source is the sqliteq queue claims several jobs in one statement through `DequeueBatchWithAckIds`. A worker bound to several queues claims one job per call. A job added with an ID (`WithJobId`, or one from `WithJobIdGenerator`) goes through `EnqueueOnce`.

sqliteq has no `JobReader`. So VarMQ's one-job read (`GET /queues/{id}/jobs/{jobId}`) answers 501 for a sqliteq queue. For workflows, see [Workflows](#workflows).

### Job outcome and cancel

A job has a job ID when it was added with `EnqueueOnce`, or when its item is a JSON object with an `id` field. A VarMQ job that has an ID carries it in that field. Only a job with a job ID gets an outcome or can be cancelled. Whoever writes an item controls its `id` field, so accept items only from trusted producers.

```go
cancel := q.WatchOutcome("job-0b7e9c2a", func(o contract.JobOutcome) {
	fmt.Println(o.State, o.Error, string(o.Result)) // "completed", "failed" or "cancelled"
})
defer cancel() // early stop only; after fn the watch ends by itself

q.AcknowledgeResult(ackID, []byte(`{"total":42}`)) // Acknowledge + result JSON; invalid JSON: false, no change

err := q.CancelJob("job-0b7e9c2a") // contract.ErrJobEnded when the job already ended

stop := q.OnCancel(func(jobID string) { /* stop the running job */ })
defer stop()

q.AcknowledgeCancelled(ackID, "cancelled by user") // finish a job that stopped for a cancel
```

Each finish of a job with a job ID writes an outcome record to `<name>__sqliteq_jobs`, in the same transaction. `Acknowledge` and `AcknowledgeResult` record `completed`, and `Reject` records `failed`. `AcknowledgeCancelled` records `cancelled`, and the reason of `Reject` or `AcknowledgeCancelled` becomes the outcome error.

`WatchOutcome` reports a job that ends through any queue object on the file, in this process or another. It also reports a job that ended before the call, while its record is kept. `fn` runs once, on its own goroutine. `Queue.Close` drops watches without calling them.

SQLite has no pub/sub. So a queue object with watches or `OnCancel` hooks polls the file every 50 ms.

Records are kept by count and by age, with fixed limits:

| State                 | Count  | Age      |
| --------------------- | ------ | -------- |
| `completed`           | 1000   | 24 hours |
| `failed`, `cancelled` | 10000  | 7 days   |

The count limit never deletes a record younger than one minute, so a burst cannot trim a record before a watch reads it.

`CancelJob(id)` writes a cancel mark to `<name>__sqliteq_cancelled`, which lasts 24 hours. Every pop drops a marked waiting job: it never runs, and its outcome is `cancelled`. One transaction drops at most 1000 marked jobs. A single-item pop then goes on, and a batch pop reports an item left. `OnCancel` hooks in every process on the file learn of each new mark. VarMQ then cancels the job's context, and a job that stops for it finishes with `AcknowledgeCancelled`.

`CancelJob` returns `contract.ErrJobEnded` when the job's outcome record shows that it ended. An unknown ID is marked too, so a later `Enqueue` of it is dropped. An `EnqueueOnce` that stores the ID deletes its old record and mark. Any ID can be marked, so authorize and rate-limit untrusted callers of `CancelJob`.

### Job records and inspection

Both queue types implement VarMQ's `contract.QueueInspector`. So a bound queue shows up in VarMQ's REST API (`GET /queues`, `GET /queues/{id}/jobs`) and in vartop, as a redisq queue does.

- **Queue ID**: `sqliteq:<h>:fifo:<name>` or `sqliteq:<h>:prio:<name>`, with `<name>` in lower case. `<h>` is the first 8 hex digits of the SHA-256 of the database file's resolved path. So every process on the file agrees, and two files with one queue name almost never collide. No path shows in the ID. sqliteq has no in-memory mode, so every queue has a file. A file reached through two different paths (another mount, say) gets two IDs.
- **Counts**: rows per `status`, read in one statement. Cancelled rows count as failed. Completed and failed rows exist only with `WithRemoveOnComplete(false)`. By default they are deleted, so those counts are 0. Kept rows have no retention.
- **Totals**: `submitted` counts stored adds, and a repeat `EnqueueOnce` is not one. `completed` and `failed` count finishes. `AcknowledgeCancelled` counts as failed, and a waiting job dropped for a cancel counts as neither. The totals live in `<name>__sqliteq_meta`, and `Purge` resets them. When this version first opens an older table, the totals start from the rows that the table holds. So jobs deleted before the upgrade (the default `WithRemoveOnComplete(true)`) are missing from them.
- **Jobs**: pending in hand-out order, processing by ack ID, and completed, failed and cancelled newest finish first. The failed list includes cancelled rows, with `state` `cancelled`. `payload` is the VarMQ envelope's `data`, or the stored item itself (as a JSON string when it is not JSON). A job without a job ID shows its ack ID once handed out. sqliteq does not count attempts: they read 0 while pending and 1 after. A kept row loses its start time when it ends. A failed job's `errors` holds its reason while its outcome record is kept.

### Queue pause

Both queue types implement VarMQ's `QueuePauser` and `PauseNotifier`. So VarMQ's REST API (`PATCH /queues/{id}/actions/pause|resume`) and vartop can pause and resume them.

`Pause()` stores a `paused_at` row in `<name>__sqliteq_meta`. Every pop selects nothing while that row exists. So the pause reaches every process on the file at once, outlives restarts and survives `Purge`. Only `Resume()` ends it. Adds, acks and rejects go on.

`Paused()` reads this queue object's cache, and a pop refused for the pause sets it. While it is set, the queue object polls the row every 50 ms. So a `Resume` from another process wakes this one's workers within that time. `OnPauseChange(fn)` calls `fn` each time the cache changes, and `PauseRefusals()` counts the pops refused for a pause.

### Workflows

A workflow is a graph of VarMQ tasks. A task runs after the tasks it depends on. sqliteq keeps the workflow state in SQLite, so a workflow survives a restart and spans several queues of one store.

Create the store once and keep it open while workers run. The order of the store and the queues does not matter. `NewWorkflowStore` registers the store for the scope of its queues. VarMQ finds it from there. A process that only runs workers needs this one call too.

```go
qs, _ := sqliteq.New("./data")
defer qs.Close()

store, err := sqliteq.NewWorkflowStore(qs)
if err != nil {
	log.Fatal(err)
}
defer store.Close() // before qs.Close

resize, _ := qs.NewQueue("resize")
publish, _ := qs.NewQueue("publish")

wf := varmq.NewWorkflow(store)
worker := varmq.NewErrWorker(func(j varmq.Job[string]) error { return nil })
defer worker.WaitAndStop()

a := wf.Add("resize", worker.WithQueue(resize), "photo.jpg")
wf.Add("publish", worker.WithQueue(publish), "photo.jpg").After(a)

wf.Submit(ctx)
record, _ := wf.Wait()
```

Options of `NewWorkflowStore`:

- `WithWorkflowResultLimit(n)` caps a task result at `n` bytes. The default is 64 KiB. A bigger result fails the task with `ErrResultTooLarge`.
- `WithWorkflowRetention(d)` sets how long an ended workflow is kept. The default is 7 days. A running workflow is kept until it ends.

Every queue of a workflow must have the same scope. The scope of a queue is `ScopeID()`: `sqliteq:` and the absolute path of the store folder, or of the file under `WithSingleFile`, with symlinks resolved. A file already open under one scope cannot be opened under another, for example as a single file and as a queue file of a folder. `NewQueue` returns an error then. `WithSingleFile` refuses the file name `__sqliteq_workflows.db`. One process holds one store for each scope. A second `NewWorkflowStore` for the same scope returns `contract.ErrScopeRegistered`.

The store implements `contract.WorkflowStore`, `WorkflowInspector` and `WorkflowKeyLister`. So VarMQ's REST API lists and cancels sqliteq workflows.

**Where the state lives.** In the folder layout the store uses its own file, `__sqliteq_workflows.db`. Under `WithSingleFile` it uses three tables of the store file. The tables are `__sqliteq_wf_workflows`, `__sqliteq_wf_tasks` and `__sqliteq_wf_outbox`. A queue name must not contain `__sqliteq_`, so a queue never takes these names.

**How a task ends.** In the folder layout, two files cannot share one transaction. So `FinishTask` works in three steps:

1. One transaction in the workflow file records the end and the result. It also writes each task the end releases into the outbox table.
2. Each outbox row is pushed into its queue with `EnqueueOnce` and the job ID `wf:<id>:<task>`. Then the row is deleted.
3. The job is acknowledged in its queue.

A recovery loop pushes leftover outbox rows when the store opens and every second while it is open. A crash between the steps is safe. A row pushed twice stores one job while the first job is still in the queue. Once a worker has finished and removed that job, a second push would queue it again. The pre-run check (`Runnable`) then skips it, because its task already ended. A job that was redelivered is skipped the same way. Under `WithSingleFile` the end, the pushes and the ack are one transaction. A push into a queue that no client in the process has open becomes an outbox row in that transaction, and the loop pushes it later.

**Limits.**

- The recovery loop cannot wake the workers of a process. A task pushed by the loop starts when a worker next polls its queue.
- The loop pushes only into queues that some client in this process has open. Rows for other queues wait, and the store logs each such queue once.
- A job ID that starts with `wf:` is for workflow items. A plain job cannot take one. If an older row already holds the ID, the push stays in the outbox and the store logs it once.
- The stored items of waiting tasks and the undo items stay in the workflow file until the task is pushed or the workflow expires.
- Folder layout: `Purge` stores an intent in the workflow file before it deletes the rows. If the process crashes between them, the recovery loop ends the purged tasks later.

A valid workflow item is never dropped by a cancel mark: the workflow decides how its task ends. A `Purge` of a queue ends each purged workflow task as cancelled with the reason `queue purged`. The workflow goes on. This works also when no store is open in the process, as long as the workflow tables exist. Under `WithSingleFile` the delete and the ends are one transaction. An item whose JSON has keys that differ only in case, such as `WF` next to `wf`, is a plain job.

### Storage

#### Layout on disk

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

#### Single file

`WithSingleFile()` makes `path` one database file that holds every queue as a table. This is the v0.2 layout, so a file written by v0.2 opens with this option and keeps its data:

```go
qs, err := sqliteq.New("queue.db", sqliteq.WithSingleFile())
```

The file is created if it is missing, but its folder must already exist: `New` returns an error otherwise, unlike the folder layout, which creates its folder.

#### Queue names

In the folder layout the queue name becomes a file name. So it may only contain `A-Z a-z 0-9 . _ -`, and it must not be empty. Anything else is an error from `NewQueue` / `NewPriorityQueue`.

On Windows the folder layout also cannot use a reserved device name, in any letter case, as a queue name. These names are `CON`, `PRN`, `AUX`, `NUL`, `COM1` to `COM9` and `LPT1` to `LPT9`. Such a name would become a file name such as `con.db`, and sqliteq does not check for these.

With `WithSingleFile()` the name is only a table name, so any name SQLite accepts as a table name works. Two rules apply in both layouts:

- A name must not start with `sqlite_`, because SQLite reserves that prefix.
- A name must not contain `__sqliteq_`. sqliteq gives the indexes, side tables and triggers of a queue `<name>` names that start with `<name>__sqliteq_`.

Names are case-insensitive in both layouts: `Jobs` and `jobs` are the same queue, stored in `jobs.db` in the folder layout.

#### Processes and files

sqliteq keeps a registry in the process, keyed by the file's real path (made absolute, with symlinks resolved). Every `New` in the process that reaches the same file shares one connection pool and one write lock, so two `New` calls on one path never write through two competing pools. Two `Queues` values can therefore hold the same queue, and you get the same handle from both. The file closes when the last `Queues` using it is closed.

Other processes can open the same file. SQLite's lock orders their writes, and what lives in the file reaches every process: a pause stops every process's pops at once, and a `Resume`, a cancel or a job outcome reaches the others within 50 ms (they poll). Recovery is the limit. Every process that opens a queue runs recovery on its first open of that queue, whatever it does with it: consumers, producers, observers (a process that only serves the REST API) and `Pause`/`Resume` callers alike. Recovery puts every item still marked as processing back to pending, so a process that opens the queue while another one runs its jobs makes those jobs run again. sqliteq does not take an OS lock to stop that. Sharing a file is safe only when every process opens the queue before any of them takes jobs, and no process opens it, or opens it again after a restart, later.

On macOS, spell a path the same way every time: the registry key keeps the letter case you pass, so `Data` and `data` would be two entries for one folder. On Windows, Go resolves existing folders and files to their on-disk letter case, so different spellings of them share one entry.

### Configuration

| Option                        | Passed to                      | Default       | Effect                                                                 |
| ----------------------------- | ------------------------------ | ------------- | ---------------------------------------------------------------------- |
| `WithSingleFile()`            | `New`                          | folder layout | `path` is one database file holding every queue                        |
| `WithCheckpointPages(n)`      | `New`                          | `1000`        | WAL size, in pages, at which SQLite checkpoints automatically          |
| `WithRemoveOnComplete(false)` | `NewQueue`, `NewPriorityQueue` | `true`        | Keep finished items, marked as `completed`, `failed` or `cancelled`, instead of deleting them |

`New` returns an error instead of panicking: for a path it cannot use, an invalid option (`WithCheckpointPages` below 1, or a nil option), or a path that is an existing file when `WithSingleFile()` is not given. `NewQueue` and `NewPriorityQueue` return an error for a nil option too.

#### Checkpoints

SQLite appends writes to a write-ahead log (WAL) and folds them back into the database file in a checkpoint. `WithCheckpointPages(n)` sets how many pages the WAL may reach before SQLite checkpoints on its own. The default is 1000 pages, SQLite's own setting. A page is 4 KB, so a checkpoint starts once the WAL reaches about 4 MB. That size is where a checkpoint is triggered, not a hard limit on the file. A checkpoint runs inside the commit of the write that crosses that size, while that write holds the file's write lock. So with the default, any write on that file (an `Enqueue`, a claim, an ack or a purge) occasionally waits for a checkpoint's disk sync.

```go
qs, err := sqliteq.New("data", sqliteq.WithCheckpointPages(10000))
```

Raising it makes those waits rarer, which cuts `Enqueue` tail latency and raises throughput, at the price of a larger WAL on disk: 10000 pages triggers a checkpoint at about 41 MB.

The WAL is a log, not extra stored data: the database holds the same items either way. Everything that reaches one file must use the same value: a `NewQueue` or `NewPriorityQueue` that reaches a file already open with a different value returns an error naming the value the file is open with. `New` itself does not open files, so it does not detect this.

## How It Works

Each queue is a table with these columns:

- `id`: autoincrement primary key. Items come out in `id` order, and a priority queue's in `priority`, then `id` order. The ack ID that `DequeueWithAckId` returns is this id in decimal.
- `priority`: priority queues only; a lower number comes out first. Opening an older table as a priority queue adds it.
- `data`: the stored item.
- `status`: `pending`, `processing`, or on a kept row `completed`, `failed` or `cancelled`.
- `ack`: 0 when an item is added, 1 once it is acknowledged or rejected and kept with `WithRemoveOnComplete(false)` (`status` `completed`, `failed` or `cancelled`). On open, only `processing` rows with `ack` 0 go back to pending.
- `ack_id`: kept from v0.2 so old tables need no rebuild; no longer used.
- `job_id`: the id an item was added with through `EnqueueOnce`, unique among the rows that have one; NULL for `Enqueue`. Opening an older table adds it.
- `created_at`, `updated_at`: when the item was added and last changed.

On top of that table:

- Two partial indexes: one covers the pending items in queue order. A unique one covers `job_id` on the rows that have one, which `EnqueueOnce` relies on.
- A claim or a pop is one `UPDATE ... RETURNING` or `DELETE ... RETURNING` statement. It selects nothing while the queue is paused.
- `<name>__sqliteq_meta` holds the totals and the pause; a trigger counts each stored add.
- `<name>__sqliteq_jobs` holds one outcome record per job ID: `seq`, `job_id`, `state`, `error`, `result` and `finished_at`. `seq` is `AUTOINCREMENT`, so a poll that reads the records above its last `seq` sees every new one.
- `<name>__sqliteq_cancelled` holds the cancel marks: `seq`, `job_id` and `expires_at`. A repeat cancel replaces the mark under a new `seq`, so the `OnCancel` hooks hear it again.
- The trigger `<name>__sqliteq_readd` fires when `EnqueueOnce` stores a job ID. It deletes the outcome record and the cancel mark of that ID. A repeat `EnqueueOnce` stores nothing, so it fires no trigger.
- A finish (`Acknowledge`, `AcknowledgeResult`, `Reject` or `AcknowledgeCancelled`) always runs in a transaction, and its outcome record commits with it.
- Each pop first checks for a live cancel mark. While one exists, the pop runs in a transaction and drops the marked items it meets.
- Transactions begin `IMMEDIATE` (`_txlock=immediate`). So a transaction that reads before it writes, such as `CancelJob` or a checked pop, waits for the write lock instead of failing with `SQLITE_BUSY`.
- One poll goroutine per queue object reads new outcome records and new cancel marks every 50 ms. While the cache says paused, it also reads the pause row. It runs only while the queue object has watches, `OnCancel` hooks or a paused cache.
- The poll goroutine applies retention and deletes expired marks once a minute. A queue object also applies retention in the finish transaction of its first record and of every 100th record after that.
- Writes to one file (adds, claims, acks, purges) take one lock in the process. So they wait for each other in Go rather than in SQLite's busy handler. `Len` and `Values` do not take it.
- Concurrent `Enqueue` and `Acknowledge` calls on one file are committed together in one transaction. Each call still gets its own result. If the shared transaction fails, each call is retried on its own, so one bad call fails alone.

## Trade-offs

- **At least once, not exactly once.** A job runs again when its process stops before the job is acknowledged. Make handlers idempotent.
- **No visibility timeout.** Unacknowledged items go back to pending only when a process next opens the file. A hung worker keeps its job until then.
- **Sharing a file between processes needs care.** Each process runs recovery on its first open of a queue. So a process that opens a queue late puts the jobs that other processes hold back to pending. See [Processes and files](#processes-and-files).
- **Polling, not push.** A queue object with watches or `OnCancel` hooks reads the file every 50 ms, in every process. Outcomes, cancels and a `Resume` from another process arrive within about 50 ms.
- **Outcomes need the record.** A job without a job ID gets no outcome. A watch that starts after the job's record was trimmed is never called. Pair a watch with a timeout of your own.
- **Fixed retention.** The record limits are not configurable. Kept rows (`WithRemoveOnComplete(false)`) have no retention at all.
- **Cancel marks cost every pop a check.** A pop without live marks pays one extra read. While a mark exists, each pop runs in a transaction and checks its items. A mark for an unknown ID lasts 24 hours.
- **No retry limit or dead-letter queue.** A job that crashes its process runs again after each restart.
- **One host.** SQLite's WAL mode needs every process on the same host, and it does not work over a network file system.

## Delivery Guarantees

| Call                                         | Guarantee                   | If the process stops                                       |
| -------------------------------------------- | --------------------------- | ---------------------------------------------------------- |
| `DequeueWithAckId`, `DequeueBatchWithAckIds` | At least once               | Unacknowledged items go back to pending on the next open   |
| `Dequeue`                                    | At most once                | The item is deleted when it is returned                    |
| `EnqueueOnce`                                | Once per id                 | Stored once while its row exists; the id survives recovery |
| `Enqueue`                                    | Stored when it returns true | Survives a process crash                                   |
| `Acknowledge`, `AcknowledgeResult`, `Reject`, `AcknowledgeCancelled` | Once | The finish and its outcome record commit together, or neither does |

- Use `DequeueWithAckId` or `DequeueBatchWithAckIds` (a VarMQ worker claims through one of them) for jobs you can't lose.
- Use globally unique ids with `EnqueueOnce` (UUID or nanoid, never a per-process counter).

sqliteq runs SQLite in WAL mode with `synchronous=NORMAL`, unchanged from v0.2. A job whose `Enqueue` returned true survives a crash of the process. After a power loss or an operating system crash, the last commits before the failure may be lost, because SQLite rolls back what the WAL had not yet synced; the database itself stays consistent.

## Upgrading from v0.3

- Requires Go 1.27, because `go.mod` requires VarMQ.
- A queue name must not contain `__sqliteq_`. Rename such a queue before the upgrade, or it fails to open.
- The first open of a queue adds the tables `<name>__sqliteq_meta`, `<name>__sqliteq_jobs` and `<name>__sqliteq_cancelled`, and their triggers. The data in the queue is kept.

## Upgrading from v0.2

- `New` returns `(Queues, error)` instead of panicking, and takes store options. `NewQueue` and `NewPriorityQueue` keep their signatures.
- `New(path)` now treats `path` as a folder with one file per queue. Add `WithSingleFile()` to keep reading an existing v0.2 file.
- `path` is a file system path, not a DSN. A `file:` URI or `:memory:` is taken as a literal file name and opens a new empty database; a `?` or `#` in the path is an error. Pass the plain path of a v0.2 file.
- While a queue is open, the same name returns the same handle instead of a new one, so `Queue.Close` on it closes it for every holder. Asking for it with different options, or as the other kind of queue, is an error.
- A v0.2 table is migrated when it opens: its old indexes are replaced with a partial index on pending rows, and a `job_id` column is added with a unique index over the rows that have one. Its data is kept.
- Ack IDs are row IDs. An ack ID from v0.2 no longer matches and returns false.
- Items come out in `id` order (a priority queue's in `priority`, then `id`), not `created_at`.
- `RequeueNoAckRows` is no longer exported, and nothing replaces it: unacknowledged items are requeued the first time a queue is opened after its file is opened in the process, not on every `NewQueue` call.

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
