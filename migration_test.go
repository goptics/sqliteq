package sqliteq

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"path/filepath"
	"sort"
	"testing"

	"github.com/goptics/varmq/contract"
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
				if ok, err := hasColumn(db, name, "owner"); err != nil || !ok {
					t.Errorf("Round %d: expected the owner column on the old table, got %t %v", round, ok, err)
				}
				for _, side := range []string{name + jobsSuffix, name + cancelledSuffix, name + readdSuffix, "__sqliteq_owners", "__sqliteq_clock"} {
					var n int
					if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name = ?", side).Scan(&n); err != nil || n != 1 {
						t.Errorf("Round %d: expected %s on the old file, got %d %v", round, side, n, err)
					}
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

// The workflow tables as #25 (contract v0.8.0) made them: no task times, no
// state index. Pasted from that commit's workflow.go init.
const v08WorkflowTables = `
	CREATE TABLE "__sqliteq_wf_workflows" (
		id TEXT PRIMARY KEY,
		hash TEXT NOT NULL,
		state TEXT NOT NULL,
		created_at INTEGER NOT NULL,
		finished_at INTEGER,
		fail_fast INTEGER NOT NULL,
		undo_on_cancel INTEGER NOT NULL,
		failed INTEGER NOT NULL DEFAULT 0,
		cancelled INTEGER NOT NULL DEFAULT 0
	);
	CREATE INDEX "__sqliteq_wf_workflows_created" ON "__sqliteq_wf_workflows" (created_at DESC, id);
	CREATE INDEX "__sqliteq_wf_workflows_finished" ON "__sqliteq_wf_workflows" (finished_at) WHERE finished_at IS NOT NULL;
	CREATE TABLE "__sqliteq_wf_tasks" (
		workflow_id TEXT NOT NULL,
		name TEXT NOT NULL,
		ord INTEGER NOT NULL,
		state TEXT NOT NULL,
		reason TEXT NOT NULL DEFAULT '',
		result BLOB,
		open INTEGER NOT NULL DEFAULT 0,
		deps TEXT NOT NULL,
		always INTEGER NOT NULL,
		queue_id TEXT NOT NULL,
		job_id TEXT NOT NULL,
		priority INTEGER NOT NULL DEFAULT 0,
		item BLOB,
		has_undo INTEGER NOT NULL DEFAULT 0,
		undo_queue_id TEXT NOT NULL DEFAULT '',
		undo_job_id TEXT NOT NULL DEFAULT '',
		undo_item BLOB,
		undo_state TEXT NOT NULL DEFAULT '',
		undo_open INTEGER NOT NULL DEFAULT 0,
		undo_reason TEXT NOT NULL DEFAULT '',
		PRIMARY KEY (workflow_id, name)
	) WITHOUT ROWID;
	CREATE TABLE "__sqliteq_wf_outbox" (
		seq INTEGER PRIMARY KEY AUTOINCREMENT,
		workflow_id TEXT NOT NULL,
		queue_id TEXT NOT NULL,
		job_id TEXT NOT NULL,
		item BLOB NOT NULL,
		priority INTEGER NOT NULL DEFAULT 0,
		created_at INTEGER NOT NULL
	);
	CREATE TABLE "__sqliteq_wf_purged" (
		seq INTEGER PRIMARY KEY AUTOINCREMENT,
		queue_id TEXT NOT NULL,
		workflow_id TEXT NOT NULL,
		task TEXT NOT NULL,
		undo INTEGER NOT NULL,
		created_at INTEGER NOT NULL
	);
	CREATE INDEX "__sqliteq_wf_outbox_workflow" ON "__sqliteq_wf_outbox" (workflow_id);
`

// writeV08Workflow builds the v0.8.0 workflow tables in the file at path,
// with running workflow "old" on queue queueID: a completed, b pushed, c
// waiting for b.
func writeV08Workflow(t *testing.T, path, queueID string) {
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
	exec(v08WorkflowTables)
	exec(`INSERT INTO "__sqliteq_wf_workflows" (id, hash, state, created_at, fail_fast, undo_on_cancel) VALUES ('old', 'h', 'running', 1000, 0, 0)`)
	for i, r := range []struct {
		name, state, deps string
		open              int
		item              []byte
	}{
		{"a", "completed", `[]`, 0, nil},
		{"b", "pending", `["a"]`, 0, nil},
		{"c", "waiting", `["b"]`, 1, wfItem("old", "c", false)},
	} {
		exec(`INSERT INTO "__sqliteq_wf_tasks" (workflow_id, name, ord, state, open, deps, always, queue_id, job_id, item)
			VALUES ('old', ?, ?, ?, ?, ?, 0, ?, ?, ?)`, r.name, i, r.state, r.open, r.deps, queueID, "wf:old:"+r.name, r.item)
	}
}

// Workflow tables of contract v0.8.0 open: the task time columns are added
// once, old rows read 0, new pushes, starts and ends are stamped. A Purge
// with no store open migrates the tables too.
func TestMigrateWorkflowTaskTimes(t *testing.T) {
	ctx := context.Background()
	open := func(t *testing.T, single bool) (Queues, *Queue) {
		t.Helper()
		dir := t.TempDir()
		root, wfPath, opts := dir, filepath.Join(dir, workflowsFile), []StoreOption(nil)
		if single {
			root = filepath.Join(dir, "all.db")
			wfPath, opts = root, []StoreOption{WithSingleFile()}
		}
		qs := mustNew(t, root, opts...)
		t.Cleanup(func() { qs.Close() })
		a, err := qs.NewQueue("a")
		if err != nil {
			t.Fatal(err)
		}
		writeV08Workflow(t, wfPath, a.QueueID())
		if !a.EnqueueOnce(wfItem("old", "b", false), "wf:old:b") {
			t.Fatal("Failed to queue the old workflow's task")
		}
		return qs, a
	}
	store := func(t *testing.T, qs Queues) *WorkflowStore {
		t.Helper()
		st, err := NewWorkflowStore(qs)
		if err != nil {
			t.Fatalf("Failed to open the v0.8.0 workflow tables: %v", err)
		}
		t.Cleanup(func() { st.Close() })
		for _, col := range taskTimeCols {
			if ok, err := hasColumn(st.f.db, "__sqliteq_wf_tasks", col); err != nil || !ok {
				t.Errorf("Expected column %s, got %t %v", col, ok, err)
			}
		}
		return st
	}

	t.Run("open", func(t *testing.T) {
		eachLayout(t, func(t *testing.T, single bool) {
			qs, a := open(t, single)
			st := store(t, qs)
			r, err := st.Record(ctx, "old")
			if err != nil {
				t.Fatal(err)
			}
			if got := taskStates(r); got != "a=completed b=pending c=waiting" {
				t.Errorf("got %s", got)
			}
			for _, tr := range r.Tasks {
				if times(tr) != [3]int64{} {
					t.Errorf("Expected no times on old task %s, got %v", tr.Name, times(tr))
				}
			}
			counts, err := st.WorkflowCounts(ctx)
			if err != nil || !maps.Equal(counts, map[contract.WorkflowState]int64{contract.WorkflowRunning: 1}) {
				t.Errorf("counts %v %v", counts, err)
			}
			st.Close()

			// Reopened: nothing to add; the old workflow goes on with times
			st = store(t, qs)
			if ok, err := st.Runnable(ctx, "old", "b", false); err != nil || !ok {
				t.Fatalf("Runnable = %t %v", ok, err)
			}
			_, ack := take(t, a)
			if _, ok := st.FinishTask(a.QueueID(), ack, done("old", "b", "")); !ok {
				t.Fatal("FinishTask failed")
			}
			if r, err = st.Record(ctx, "old"); err != nil {
				t.Fatal(err)
			}
			ta, tb, tc := r.Tasks[0], r.Tasks[1], r.Tasks[2]
			if times(ta) != [3]int64{} || tb.EnqueuedAt != 0 || tb.StartedAt == 0 || tb.FinishedAt == 0 || tc.EnqueuedAt == 0 {
				t.Errorf("Expected old times 0 and new ones set, got a %v b %v c %v", times(ta), times(tb), times(tc))
			}
			if got := taskStates(r); got != "a=completed b=completed c=pending" || a.Len() != 1 {
				t.Errorf("got %s, a %d", got, a.Len())
			}
		})
	})

	t.Run("purge", func(t *testing.T) {
		eachLayout(t, func(t *testing.T, single bool) {
			qs, a := open(t, single)
			a.Purge() // no store open: the on-demand one works on the old tables
			r, err := store(t, qs).Record(ctx, "old")
			if err != nil {
				t.Fatal(err)
			}
			if got := taskStates(r); got != "a=completed b=cancelled c=cancelled" || r.State != contract.WorkflowFailed {
				t.Errorf("got %s %s", got, r.State)
			}
			if r.Tasks[1].FinishedAt == 0 || r.Tasks[2].FinishedAt == 0 || r.Tasks[1].EnqueuedAt != 0 {
				t.Errorf("Expected the purge's ends stamped, got b %v c %v", times(r.Tasks[1]), times(r.Tasks[2]))
			}
		})
	})
}
