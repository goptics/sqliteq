# Changelog

All notable changes to the sqliteq project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### ⚠️ Breaking Changes

- Requires Go 1.27 (was 1.24): `go.mod` requires VarMQ, which needs it. (#22)
- Queue name must not contain `__sqliteq_`, in both layouts. (#22)
- Reason: a queue's indexes, side tables and triggers use that part in their names. (#22)

### Added

- `Reject(ackID, reason)` on `Queue` and `PriorityQueue`, for VarMQ's `IAcknowledgeable` (varmq#173). (#21)
- `Reject` finishes an item that failed for good: never requeued. (#21)
- Rejected item deleted like an ack; with `WithRemoveOnComplete(false)` kept with `status` `failed`. (#21)
- `Reject` uses the same group commit as `Acknowledge`. (#21)
- `Reject` reason stored as the job's outcome error. (#22)
- Job outcome (VarMQ spec 6.13): `WatchOutcome` (`contract.OutcomeWatcher`) on both queues. (#22)
- `AcknowledgeResult` (`contract.ResultAcknowledgeable`) on both queues. (#22)
- Job ID = `EnqueueOnce` id, else the item's JSON `id`. (#22)
- Job with a job ID gets a record in `<name>__sqliteq_jobs` when it ends, in the finish's transaction. (#22)
- Watches poll records by seq every 50 ms, from any queue object on the file, any process. (#22)
- Retention: completed 1000 / 24 h; failed and cancelled 10000 / 7 d. (#22)
- Count limit spares records under 1 min old. (#22)
- Polling queue object sweeps retention and expired marks once a minute. (#22)
- Job cancel (VarMQ spec 6.14): `CancelJob` (`contract.JobCanceller`) on both queues. (#22)
- `OnCancel` (`contract.CancelNotifier`) polls new marks every 50 ms. (#22)
- `AcknowledgeCancelled` (`contract.CancelAcknowledgeable`) finishes a running job that stopped for a cancel. (#22)
- Cancel marks in `<name>__sqliteq_cancelled`, 24 h each. (#22)
- Every pop drops a marked waiting job, outcome `cancelled`, at most 1000 per transaction. (#22)
- At the drop cap: single-item pop goes on; batch pop reports an item left. (#22)
- `CancelJob` returns `contract.ErrJobEnded` when the record shows the job ended. (#22)
- `EnqueueOnce` that stores an id drops its old record and mark (trigger). (#22)
- `Purge` clears records and marks too. `Queue.Close` drops watches and hooks. (#22)
- Queue inspection (`contract.QueueInspector`) on both queues: VarMQ's REST API and vartop list sqliteq queues. (#23)
- `QueueID`: `sqliteq:<h>:fifo|prio:<name>`, `<name>` in lower case. (#23)
- `<h>`: first 8 hex of the SHA-256 of the file's resolved path. (#23)
- `QueueStats` counts: per row status, one statement; cancelled under failed. (#23)
- Completed and failed counts only with `WithRemoveOnComplete(false)`. (#23)
- `QueueStats` totals: submitted, completed, failed, in new table `<name>__sqliteq_meta`. (#23)
- Insert trigger counts adds; a finish counts in its transaction; `Purge` resets totals. (#23)
- Older table: totals start from its rows, so jobs deleted before the upgrade are missing. (#23)
- `QueueStats` reports the pause. (#23)
- `Jobs`: pending in hand-out order, processing by ack ID. (#23)
- `Jobs`: kept completed, failed (with cancelled) and cancelled rows, newest first. (#23)
- `Jobs` payload: the envelope's `data` verbatim; errors from the outcome record. (#23)
- Attempts not counted: 0 pending, 1 after. (#23)
- Durable queue pause (VarMQ spec 6.7, 6.8): `Pause`, `Resume`, `Paused`, `PauseRefusals` (`varmq.QueuePauser`). (#23)
- `OnPauseChange` (`varmq.PauseNotifier`). (#23)
- Pause flag: a row of `<name>__sqliteq_meta`. (#23)
- Every pop selects nothing while the flag exists, in every process on the file. (#23)
- Pause outlives restarts and `Purge`. (#23)
- Refused pop sets `Paused`; while set, the queue object polls the row every 50 ms for a remote `Resume`. (#23)

### Changed

- Transactions begin `IMMEDIATE`. (#22)
- Finish (`Acknowledge`, `Reject`) always runs in a transaction. (#22)
- Pop runs in a transaction while any cancel mark is live. (#22)

## [0.3.0] - 2026-10-23

### ⚠️ Breaking Changes

- `New(path, opts ...StoreOption)` returns `(Queues, error)` instead of panicking. `NewQueue` and `NewPriorityQueue` keep their signatures.
- `path` is now a folder, created if missing, holding one `<name>.db` file per queue with `<name>` in lower case (`NewQueue("Emails")` uses `emails.db`). Pass `WithSingleFile()` to keep the v0.2 layout and open an existing v0.2 file.
- `path` is a file system path, not a DSN: a `file:` URI or `:memory:` is taken as a literal file name, and a `?` or `#` in it is an error.
- In the folder layout a queue name may only contain `A-Z a-z 0-9 . _ -`, must not be empty and must not start with `sqlite_`. Names are case-insensitive in both layouts.
- `New` checks the path: a file without `WithSingleFile()`, a folder with it, a missing parent folder for `WithSingleFile()`, a `?` or `#`, or a folder it cannot create is an error from `New`. It opens no files, so a file SQLite cannot open (one that is not a database, say) and a checkpoint setting that differs from the one the file is already open with are reported by `NewQueue` / `NewPriorityQueue`.
- While a queue is open, the same name returns the same handle, so `Queue.Close` closes it for every holder, and asking for it with different options is an error. Asking for a name as the other kind of queue is an error even after `Queue.Close`, until every `Queues` holding its file is closed.
- Ack ids are row ids in decimal, and `Acknowledge` matches only an item in processing. An ack id from v0.2, a repeat ack with `WithRemoveOnComplete(false)` (which used to return true), and an ack for an item that recovery put back to pending (which used to remove or complete it) now return false and change nothing.
- Items come out, and `Values` lists them, in `id` order (a priority queue's in `priority`, then `id`), not `created_at`. A priority queue's `Values` used to ignore priority and list items in insert order.
- `RequeueNoAckRows()` is no longer exported. Unacknowledged items go back to pending the first time a queue is opened after its file is opened in the process, not on every `NewQueue` call.
- Requires Go 1.24 (was 1.20) and SQLite 3.35 or later, for `RETURNING`. go-sqlite3 bundles SQLite 3.49.1, so only a build with its `libsqlite3` tag against an older system SQLite is affected: `NewQueue` and `NewPriorityQueue` fail to prepare their statements there.

### Added

- `WithSingleFile()` keeps every queue in one database file.
- `WithCheckpointPages(n)` sets the WAL size, in pages, at which SQLite checkpoints automatically (default 1000).
- `EnqueueOnce(item, id)` on `Queue` and `EnqueueOnce(item, priority, id)` on `PriorityQueue`, implementing varmq's `IDedupQueue` and `IDedupPriorityQueue`. A repeat id stores nothing and returns true while a row with that id exists.
- `DequeueBatchWithAckIds(items, ackIDs)`, implementing varmq's `IBatchAcknowledgeable`: claims several items in one statement and reports the pending items left, counted up to 256.
- A registry in the process, keyed by each file's resolved absolute path, so every `New` reaching one file shares its connection pool, write lock and handles, and recovery runs once per table.
- Opening a v0.2 table migrates it in place: its old indexes are replaced by one partial index on pending rows, and a `job_id` column is added with a unique partial index over the rows that have one.

### Changed

- Each claim and pop is one `UPDATE ... RETURNING` or `DELETE ... RETURNING` statement.
- Every write on a file takes one write lock in the process, so writers wait in Go instead of in SQLite's busy handler. `Len` and `Values` do not take it.
- Concurrent `Enqueue` and `Acknowledge` calls on one file are committed together in one transaction (group commit). Each call keeps its own result, and if the shared transaction fails, each call is retried on its own.
- Statements are prepared once per queue handle, and the pool keeps up to 8 connections per file.
- Batch pops and `EnqueueOnce` need varmq v1.10.0 or later; an older varmq still works through `DequeueWithAckId` and `Enqueue`.

### Removed

- The `github.com/lucsky/cuid` dependency.

### Fixed

- A priority queue can be reopened on an existing database; it failed with `duplicate column name: priority` (#5).
- `Dequeue` and `DequeueWithAckId` no longer fail with `SQLITE_BUSY` and return empty while items are pending under concurrent load (#6, #7).
- A failed table setup no longer closes the database shared by the other queues (#8).
- Recovery of unacknowledged items no longer panics when a transaction cannot start, and reports its errors (#9).
- A nil `Option` passed to `NewQueue` or `NewPriorityQueue` returns an error instead of panicking.

## [0.2.3] - 2025-01-27

### Changed

- Refactored database transaction handling by removing redundant rollback calls for better code clarity

## [0.2.2] - 2025-05-17

### Fixed

- Added `quoteIdent` function to all SQL identifiers and proper escaping
- Standardized identifier quoting across both Queue and PriorityQueue implementations

## [0.2.1] - 2025-05-07

### Fixed

- Added recovery functionality for unacknowledged queue items via `RequeueNoAckRows()`
- Added boolean column `ack` to track acknowledgment status
- Improved query performance with additional indexes
- Fixed issue with duplicate ack_id generation

## [0.2.0] - 2025-05-06

### Changed

- Modified queue and priority queue to work with byte arrays (`[]byte`) instead of arbitrary types
- Updated the `Values()` method to return byte arrays directly without JSON unmarshaling
- Updated SQLite3 dependency from v2.0.3+incompatible to v1.14.28 to fix compatibility warnings

## [0.1.0] - 2025-05-06

### Added

- Initial release of sqliteq
- Basic queue functionality with SQLite backend
- Support for acknowledgment-based processing
- Priority queue implementation with configurable priority levels
- Option to retain acknowledged items in the database (`WithRemoveOnComplete`)
- Comprehensive test suite for both Queue and PriorityQueue implementations
- Example code demonstrating both regular and priority queue usage
- WAL mode enabled for better concurrent access
- Proper indexing on status and timestamp columns for efficient querying

### Implementation Details

- Queue items have simple lifecycle: pending → processing → completed/removed
- Priority queue dequeuing respects both priority and creation time
