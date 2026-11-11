# Changelog

All notable changes to the sqliteq project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### ⚠️ Breaking Changes

- Requires Go 1.27 (was 1.24): `go.mod` requires VarMQ, which needs it. (#22)
- Queue name must not contain `__sqliteq_`, in both layouts. (#22)
- Reason: a queue's indexes, side tables and triggers use that part in their names. (#22)
- Recovery is by owner and heartbeat, not on open. After a crash, unacknowledged items return one heartbeat timeout (default 30s) after the dead process's last heartbeat, also for a single process that restarts at once. (#28)
- Mixed versions on one file are unsafe: an older process still requeues every processing item on open, and its items carry no owner. Upgrade all processes of a file together. (#28)
- `OnPauseChange` hooks also get `false` when a sweep puts items back into an unpaused queue. (#28)

### Added

- Workflows (VarMQ spec 6.9 to 6.12): `NewWorkflowStore(qs, ...WorkflowOption)` returns the `contract.WorkflowStore` of a store root. (#25)
- The store registers with `contract.RegisterWorkflowStore`; `Close` unregisters it and releases its file. (#25)
- `WithWorkflowResultLimit` (default 64 KiB, `ErrResultTooLarge`) and `WithWorkflowRetention` (default 7 days). (#25)
- `ScopeID()` on `Queue` and `PriorityQueue`: `sqliteq:` and the absolute store folder (symlinks resolved), or the file under `WithSingleFile`. (#25)
- A file open under one scope is refused under another; `WithSingleFile` refuses the name `__sqliteq_workflows.db`. (#25)
- `FinishTask` finds the queue for any client of the scope in this process. (#25)
- `WithSingleFile`: a push into a queue no client has open becomes an outbox row in the same transaction. (#25)
- A plain job cannot take a job ID that starts with `wf:`; a held ID keeps the push in the outbox. (#25)
- `Purge` ends purged workflow tasks without a registered store; one transaction under `WithSingleFile`, a stored intent in the folder layout. (#25)
- `Submit` refuses a dependency that names no task, and replaces an expired, unswept workflow of the same ID. (#25)
- Folder layout: the store keeps its tables in `__sqliteq_workflows.db`. `WithSingleFile`: tables in the same file. (#25)
- Tables `__sqliteq_wf_workflows`, `__sqliteq_wf_tasks`, `__sqliteq_wf_outbox`. (#25)
- `FinishTask` in three steps in the folder layout: record the end and write the outbox, push each row with `EnqueueOnce`, acknowledge the job. (#25)
- A recovery loop pushes leftover outbox rows at open and every second, in both layouts. (#25)
- `WithSingleFile`: the end, the pushes and the ack are one transaction. (#25)
- A redelivered job, or a push repeated after the job was removed, is safe: the pre-run check skips a task that already ended. (#25)
- `Workflows` (`contract.WorkflowInspector`) and `WorkflowKeys` (`contract.WorkflowKeyLister`) list workflows newest first. (#25)
- `WorkflowCounts` (`contract.WorkflowCounter`, contract v0.9.0): stored workflows per state, same rows as `WorkflowKeys`.
- `HostLocal` marker (`contract.HostLocal`): VarMQ puts host name into the store's ID in `GET /workflow-counts`, so same path on two hosts = two stores.
- Task times on `TaskRecord` and its `Undo`: `EnqueuedAt` at push (written to queue or outbox; a push that waits in the outbox keeps it), `StartedAt` when `Runnable` last answered true, `FinishedAt` at end. Unix ms, store clock.
- `Runnable` answers from a lock-free read: a foreign write lock gives no error. A true answer then stamps `StartedAt`, only on a still-pending task; a failed stamp is logged, answer stays true. Unknown workflow: `ErrWorkflowNotFound`, no row made.
- `Purge` keeps the jobs (logs the cause) when the file has workflow tables but no store is ready (check, open or init failed): no task vanishes under a workflow left `running`.
- `__sqliteq_wf_tasks` columns `enqueued_at`, `started_at`, `finished_at`, `undo_enqueued_at`, `undo_started_at`, `undo_finished_at` (INTEGER, default 0). Added at open to older tables; old rows read 0.
- Index `__sqliteq_wf_workflows_state` on `(state, created_at DESC, id)`.
- A cancel mark never drops a valid workflow item; the workflow decides its end. (#25)
- Task JSON whose keys differ only in case (`WF` next to `wf`) is a plain job. (#25)
- `Purge` ends each purged workflow task as cancelled, reason `queue purged`; its workflow goes on. (#25)
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
- `Purge` clears records and marks too. (#22)
- `Queue.Close` drops watches and hooks. (#22)
- Queue inspection (`contract.QueueInspector`) on both queues: VarMQ's REST API and vartop list sqliteq queues. (#23)
- `QueueID`: `sqliteq:<h>:fifo|prio:<name>`, `<name>` in lower case. (#23)
- `<h>`: first 8 hex of the SHA-256 of the file's resolved path. (#23)
- `QueueStats` counts: per row status, one statement; cancelled under failed. (#23)
- Completed and failed counts only with `WithRemoveOnComplete(false)`. (#23)
- `QueueStats` totals: submitted, completed, failed, in new table `<name>__sqliteq_meta`. (#23)
- Insert trigger counts adds. (#23)
- Finish counts in its transaction. (#23)
- `Purge` resets totals. (#23)
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

- Owner id per process and file; column `owner` on queue tables, set by a claim, NULL on pending rows. (#28)
- Tables `__sqliteq_owners` (heartbeats) and `__sqliteq_clock` (last sweep), per database file. Index `<name>__sqliteq_processing`. (#28)
- Only a process that claims writes a heartbeat. Producers, inspectors and pausers write none. (#28)
- Sweep on a queue's first open and every sixth of the timeout, in every process with a queue open: requeues processing items whose owner has no live heartbeat. (#28)
- A dead process's items return about one timeout after its last heartbeat, with no reopen. (#28)
- Requeue count in `__sqliteq_clock`: each process wakes its bound VarMQ workers at its next sweep, whichever process requeued. (#28)
- Last close of a file in the process (last `Queues`, and the `WorkflowStore` under `WithSingleFile`) requeues its own unacknowledged items at once and deletes its heartbeat. (#28)
- Clock-gap guard: a gap between sweeps above a third of the timeout, or a clock step back, moves every heartbeat by the gap, once per heartbeat until its owner renews it. (#28)
- `WithHeartbeatTimeout(d)` store option: default 30s, at least 5s. Must match for every `New` in the process on one file. (#28)

### Changed

- `Acknowledge`, `Reject` and every other finish match the owner: one for an item another process took since returns false. (#28)
- Known limit: a process that froze past the timeout and claimed the same item again can end the second run with the first run's ack. No attempt counter. (#28)
- Transactions begin `IMMEDIATE`. (#22)
- Finish (`Acknowledge`, `Reject`) always runs in a transaction. (#22)
- Pop runs in a transaction while any cancel mark is live. (#22)

### Fixed

- A second process that opened a queue requeued the jobs another live process was running, so they ran twice. (#27, #28)

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
