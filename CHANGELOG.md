# Changelog

All notable changes to the SQLiteQ project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
