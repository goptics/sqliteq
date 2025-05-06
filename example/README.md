# SQLiteQ Examples

This directory contains example code demonstrating how to use the SQLiteQ library.

## Available Examples

The example program provides two demonstrations:

1. **Regular Queue Example** - Shows basic queue operations including:
   - Creating a queue
   - Enqueueing various data types
   - Retrieving queue length and values
   - Dequeuing with and without acknowledgment
   - Purging the queue

2. **Priority Queue Example** - Demonstrates priority-based queue functionality:
   - Creating a priority queue
   - Adding items with different priorities (lower number = higher priority)
   - Observing how items are dequeued in priority order

## Running the Examples

To run the examples:

```bash
# From this directory
go run .
```

You'll be presented with a menu to choose which example to run:

```
SQLiteQ Examples
---------------
1. Regular Queue Example
2. Priority Queue Example
Choose an example (default is 1):
```

## What to Expect

- Both examples create temporary SQLite database files that are automatically removed after the example completes
- The regular queue example demonstrates the core queue operations
- The priority queue example shows how items with lower priority numbers are processed first
