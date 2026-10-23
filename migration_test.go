package sqliteq

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
)

// The v0.2.3 schema, pasted from tag v0.2.3's queue.go initTable and
// priority_queue.go initPriorityColumn, so the fixture needs no git.
const (
	v023CreateTable = `
	CREATE TABLE IF NOT EXISTS %[1]s (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		data BLOB NOT NULL,
		status TEXT NOT NULL,
		ack_id TEXT UNIQUE,
		ack BOOLEAN DEFAULT 0,
		created_at TIMESTAMP,
		updated_at TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS %[2]s ON %[1]s (status, created_at);
	CREATE INDEX IF NOT EXISTS %[3]s ON %[1]s (status, ack);
	CREATE INDEX IF NOT EXISTS %[4]s ON %[1]s (ack_id);
	`
	v023AddPriority   = "ALTER TABLE %s ADD COLUMN priority INTEGER NOT NULL DEFAULT 0"
	v023PriorityIndex = "CREATE INDEX IF NOT EXISTS %s ON %s (priority ASC, created_at ASC)"
)

// writeV023File builds a v0.2.3 database at path with one table: 5 rows, ids
// 1-5, where 2 and 4 are processing with cuid ack ids. created_at runs
// backwards, so only id order puts them back in insert order.
func writeV023File(t *testing.T, path, name string, priority bool) {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := db.Exec(query, args...); err != nil {
			t.Fatalf("Fixture statement failed: %v\n%s", err, query)
		}
	}

	exec(fmt.Sprintf(v023CreateTable,
		quoteIdent(name),
		quoteIdent(name+"_status_idx"),
		quoteIdent(name+"_status_ack_idx"),
		quoteIdent(name+"_ack_id_idx")))
	if priority {
		exec(fmt.Sprintf(v023AddPriority, quoteIdent(name)))
		exec(fmt.Sprintf(v023PriorityIndex, quoteIdent(name+"_priority_idx"), quoteIdent(name)))
	}

	ackIDs := map[int]string{2: "cjld2cjxh0000qzrmn831i7rn", 4: "cjld2cyuq0000t3rmniod1foy"}
	for i := 1; i <= 5; i++ {
		status, ackID := "pending", sql.NullString{}
		if id, ok := ackIDs[i]; ok {
			status, ackID = "processing", sql.NullString{String: id, Valid: true}
		}
		created := fmt.Sprintf("2025-01-01 00:00:%02d+00:00", 60-i)
		exec(fmt.Sprintf("INSERT INTO %s (id, data, status, ack_id, ack, created_at, updated_at) VALUES (?, ?, ?, ?, 0, ?, ?)", quoteIdent(name)),
			i, []byte(fmt.Sprintf("job-%d", i)), status, ackID, created, created)
	}
}

// indexNames lists the indexes SQLite holds for a table.
func indexNames(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ?", table)
	if err != nil {
		t.Fatalf("Failed to list indexes: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("Failed to scan index name: %v", err)
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Failed to list indexes: %v", err)
	}
	sort.Strings(names)
	return names
}

func TestMigrateV02File(t *testing.T) {
	for _, priority := range []bool{false, true} {
		name := "jobs"
		if priority {
			name = "pjobs"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "v023.db")
			writeV023File(t, path, name, priority)

			// Opened twice: the migration must hold up, and change nothing, on reopen
			for round := 1; round <= 2; round++ {
				qs := mustNew(t, path, WithSingleFile())
				var (
					dequeue func() (any, bool)
					length  func() int
					db      *sql.DB
				)
				if priority {
					pq, err := qs.NewPriorityQueue(name)
					if err != nil {
						t.Fatalf("Round %d: failed to open v0.2.3 priority queue: %v", round, err)
					}
					dequeue, length, db = pq.Dequeue, pq.Len, pq.f.db
				} else {
					q, err := qs.NewQueue(name)
					if err != nil {
						t.Fatalf("Round %d: failed to open v0.2.3 queue: %v", round, err)
					}
					dequeue, length, db = q.Dequeue, q.Len, q.f.db
				}

				want := []string{name + jobIDIndexSuffix, name + pendingIndexSuffix, "sqlite_autoindex_" + name + "_1"}
				if got := indexNames(t, db, name); fmt.Sprint(got) != fmt.Sprint(want) {
					t.Errorf("Round %d: indexes = %v, want %v", round, got, want)
				}

				if round == 1 {
					// The two processing rows are pending again after recovery
					if n := length(); n != 5 {
						t.Errorf("Expected 5 pending jobs after opening, got %d", n)
					}
				} else {
					for i := 1; i <= 5; i++ {
						item, ok := dequeue()
						if !ok {
							t.Fatalf("Dequeue %d failed", i)
						}
						if got, want := string(item.([]byte)), fmt.Sprintf("job-%d", i); got != want {
							t.Errorf("Dequeue %d = %q, want %q", i, got, want)
						}
					}
					if _, ok := dequeue(); ok {
						t.Error("Expected the queue empty after 5 dequeues")
					}
				}

				if err := qs.Close(); err != nil {
					t.Fatalf("Round %d: failed to close: %v", round, err)
				}
			}
		})
	}
}

// A table first used as a queue and later opened as a priority queue gets
// its pending index rebuilt on (priority, id), and the reverse.
func TestPendingIndexFollowsKind(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kind.db")

	for _, priority := range []bool{false, true, false} {
		qs := mustNew(t, path, WithSingleFile())
		var db *sql.DB
		if priority {
			pq, err := qs.NewPriorityQueue("jobs")
			if err != nil {
				t.Fatalf("Failed to open priority queue: %v", err)
			}
			db = pq.f.db
		} else {
			q, err := qs.NewQueue("jobs")
			if err != nil {
				t.Fatalf("Failed to open queue: %v", err)
			}
			db = q.f.db
		}

		rows, err := db.Query("SELECT name FROM pragma_index_info(?) ORDER BY seqno", "jobs"+pendingIndexSuffix)
		if err != nil {
			t.Fatalf("Failed to read index columns: %v", err)
		}
		var cols []string
		for rows.Next() {
			var c string
			if err := rows.Scan(&c); err != nil {
				t.Fatalf("Failed to scan column: %v", err)
			}
			cols = append(cols, c)
		}
		rows.Close()

		want := "[id]"
		if priority {
			want = "[priority id]"
		}
		if fmt.Sprint(cols) != want {
			t.Errorf("priority=%v: pending index columns = %v, want %s", priority, cols, want)
		}
		if err := qs.Close(); err != nil {
			t.Fatalf("Failed to close: %v", err)
		}
	}
}

// Tables and indexes share one namespace in SQLite, so the pending and
// job_id indexes must not take a name a queue could have: a file with queues
// "orders", "orders_pending" and "orders_job_id" opens in either order, and
// again on reopen.
func TestPendingIndexNameFreeForQueues(t *testing.T) {
	for _, names := range [][]string{{"orders", "orders_pending", "orders_job_id"}, {"orders_job_id", "orders_pending", "orders"}} {
		t.Run(names[0]+"_first", func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "shared.db")
			for round := 1; round <= 2; round++ {
				qs := mustNew(t, path, WithSingleFile())
				for _, name := range names {
					q, err := qs.NewQueue(name)
					if err != nil {
						t.Fatalf("Round %d: failed to open queue %q: %v", round, name, err)
					}
					want := fmt.Sprintf("%s-%d", name, round)
					if !q.Enqueue(want) {
						t.Fatalf("Round %d: enqueue to %q failed", round, name)
					}
					item, ok := q.Dequeue()
					if !ok {
						t.Fatalf("Round %d: dequeue from %q failed", round, name)
					}
					if got := string(item.([]byte)); got != want {
						t.Errorf("Round %d: %q dequeued %q, want %q", round, name, got, want)
					}
				}
				if err := qs.Close(); err != nil {
					t.Fatalf("Round %d: failed to close: %v", round, err)
				}
			}
		})
	}
}
