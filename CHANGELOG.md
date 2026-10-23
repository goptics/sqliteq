# Changelog

All notable changes to the SQLiteQ project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-10-23

### Breaking

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

- Initial release of SQLiteQ
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
