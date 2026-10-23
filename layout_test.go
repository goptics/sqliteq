package sqliteq

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// mustNew opens a store at path and fails the test on error.
func mustNew(t *testing.T, path string, opts ...StoreOption) Queues {
	t.Helper()
	qs, err := New(path, opts...)
	if err != nil {
		t.Fatalf("Failed to create queues: %v", err)
	}
	return qs
}

// pragmaOnConns reads a single-value pragma on n connections held open at once.
func pragmaOnConns(t *testing.T, db *sql.DB, pragma string, n int) []int {
	t.Helper()
	ctx := context.Background()
	conns := make([]*sql.Conn, 0, n)
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	var got []int
	for i := 0; i < n; i++ {
		c, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("Failed to take connection %d: %v", i, err)
		}
		conns = append(conns, c)

		var v int
		if err := c.QueryRowContext(ctx, "PRAGMA "+pragma).Scan(&v); err != nil {
			t.Fatalf("Failed to read %s: %v", pragma, err)
		}
		got = append(got, v)
	}
	return got
}

func TestCheckpointPagesInvalid(t *testing.T) {
	for _, n := range []int{0, -5} {
		qs, err := New(t.TempDir(), WithCheckpointPages(n))
		if err == nil {
			qs.Close()
			t.Fatalf("Expected an error for WithCheckpointPages(%d)", n)
		}
		if qs != nil {
			t.Fatalf("Expected nil Queues for WithCheckpointPages(%d)", n)
		}
	}
}

func TestCheckpointPagesEveryConn(t *testing.T) {
	cases := []struct {
		name string
		opts []StoreOption
		want int
	}{
		{"custom", []StoreOption{WithCheckpointPages(4000)}, 4000},
		{"default", nil, 1000},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			qs := mustNew(t, filepath.Join(t.TempDir(), "ckpt.db"), append(tc.opts, WithSingleFile())...)
			defer qs.Close()

			q, err := qs.NewQueue("q")
			if err != nil {
				t.Fatalf("Failed to create queue: %v", err)
			}

			for i, v := range pragmaOnConns(t, q.f.db, "wal_autocheckpoint", 3) {
				if v != tc.want {
					t.Errorf("conn %d: wal_autocheckpoint = %d, want %d", i, v, tc.want)
				}
			}
		})
	}
}

func TestSyncAndBusyTimeout(t *testing.T) {
	qs := mustNew(t, filepath.Join(t.TempDir(), "pragmas.db"), WithSingleFile())
	defer qs.Close()

	q, err := qs.NewQueue("q")
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	db := q.f.db

	for _, tc := range []struct {
		pragma string
		want   int
	}{{"synchronous", 1}, {"busy_timeout", 5000}} {
		if got := pragmaOnConns(t, db, tc.pragma, 1)[0]; got != tc.want {
			t.Errorf("%s = %d, want %d", tc.pragma, got, tc.want)
		}
	}
}

func TestNewBadPath(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "f")
	if err := os.WriteFile(file, []byte("not a dir"), 0o600); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	qs, err := New(filepath.Join(file, "x.db"), WithSingleFile())
	if err != nil {
		return
	}
	defer qs.Close()

	if _, err := qs.NewQueue("q"); err == nil {
		t.Fatal("Expected an error from New or NewQueue for a path under a regular file")
	}
}

// dbFiles lists the database files in dir, leaving out SQLite's -wal and -shm files.
func dbFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", dir, err)
	}
	var names []string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), "-wal") && !strings.HasSuffix(e.Name(), "-shm") {
			names = append(names, e.Name())
		}
	}
	return names
}

func TestFolderLayout(t *testing.T) {
	dir := t.TempDir()
	qs := mustNew(t, dir)
	defer qs.Close()

	q, err := qs.NewQueue("emails")
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	pq, err := qs.NewPriorityQueue("Reports")
	if err != nil {
		t.Fatalf("Failed to create priority queue: %v", err)
	}

	for _, name := range []string{"emails.db", "reports.db"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("Expected %s: %v", name, err)
		}
	}

	q.Enqueue([]byte("a"))
	q.Enqueue([]byte("b"))
	pq.Enqueue([]byte("c"), 0)
	if q.Len() != 2 || pq.Len() != 1 {
		t.Errorf("Expected lengths 2 and 1, got %d and %d", q.Len(), pq.Len())
	}
	if q.f == pq.f {
		t.Error("Expected the two queues in different files")
	}
}

func TestFolderCreated(t *testing.T) {
	root := filepath.Join(t.TempDir(), "a", "b")
	qs := mustNew(t, root)
	defer qs.Close()

	if info, err := os.Stat(root); err != nil || !info.IsDir() {
		t.Fatalf("Expected folder %s, got %v", root, err)
	}
}

func TestExistingFileNeedsSingleFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	qs, err := New(path)
	if err == nil {
		qs.Close()
		t.Fatal("Expected an error for a regular file without WithSingleFile")
	}
	if !strings.Contains(err.Error(), "WithSingleFile") {
		t.Errorf("Expected the error to mention WithSingleFile, got %q", err)
	}
	if info, err := os.Stat(path); err != nil || info.IsDir() {
		t.Errorf("Expected %s to stay a regular file", path)
	}

	qs = mustNew(t, path, WithSingleFile())
	qs.Close()
}

func TestSingleFileOnFolderErrors(t *testing.T) {
	qs, err := New(t.TempDir(), WithSingleFile())
	if err == nil {
		qs.Close()
		t.Fatal("Expected an error for WithSingleFile on a folder")
	}
}

func TestNameRules(t *testing.T) {
	qs := mustNew(t, t.TempDir())
	defer qs.Close()

	for _, name := range []string{"", "a/b", ".." + "/x", "a:b", "my queue", "sqlite_x", "SQLite_x"} {
		if _, err := qs.NewQueue(name); err == nil {
			t.Errorf("Expected an error for folder queue name %q", name)
		}
		if _, err := qs.NewPriorityQueue(name); err == nil {
			t.Errorf("Expected an error for folder priority queue name %q", name)
		}
	}
	if _, err := qs.NewQueue("a.b_c-1"); err != nil {
		t.Errorf("Expected a.b_c-1 to be a valid name: %v", err)
	}

	single := mustNew(t, filepath.Join(t.TempDir(), "app.db"), WithSingleFile())
	defer single.Close()
	for _, name := range []string{"jobs:high", "my queue"} {
		q, err := single.NewQueue(name)
		if err != nil {
			t.Fatalf("Expected %q to work under WithSingleFile: %v", name, err)
		}
		if !q.Enqueue([]byte(name)) {
			t.Fatalf("Enqueue on %q failed", name)
		}
		item, ok := q.Dequeue()
		if !ok || string(item.([]byte)) != name {
			t.Errorf("Expected %q back, got %v (ok=%v)", name, item, ok)
		}
	}
}

func TestPathWithQueryErrors(t *testing.T) {
	for _, base := range []string{"a?b", "a#b"} {
		root := filepath.Join(t.TempDir(), base)
		if qs, err := New(root); err == nil {
			qs.Close()
			t.Errorf("Expected an error for folder %q", base)
		}
		if _, err := os.Stat(root); err == nil {
			t.Errorf("Expected no folder created for %q", base)
		}
		if qs, err := New(root+".db", WithSingleFile()); err == nil {
			qs.Close()
			t.Errorf("Expected an error for file %q", base+".db")
		}
	}
}

func TestSameNameSameHandle(t *testing.T) {
	dir := t.TempDir()
	qs := mustNew(t, dir)
	defer qs.Close()

	a, err := qs.NewQueue("Jobs")
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	b, err := qs.NewQueue("jobs")
	if err != nil {
		t.Fatalf("Failed to reopen queue: %v", err)
	}
	if a != b {
		t.Error("Expected the same handle for Jobs and jobs")
	}
	if got := dbFiles(t, dir); len(got) != 1 || got[0] != "jobs.db" {
		t.Errorf("Expected only jobs.db, got %v", got)
	}
	if _, err := qs.NewPriorityQueue("JOBS"); err == nil {
		t.Error("Expected an error for a priority queue over a queue's name")
	}
}

func TestOptionMismatch(t *testing.T) {
	qs := mustNew(t, t.TempDir())
	defer qs.Close()

	if _, err := qs.NewQueue("q"); err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	if _, err := qs.NewQueue("q", WithRemoveOnComplete(false)); err == nil {
		t.Error("Expected an error for a different WithRemoveOnComplete on an open handle")
	}
	if _, err := qs.NewQueue("q", WithRemoveOnComplete(true)); err != nil {
		t.Errorf("Expected the same options to reuse the handle: %v", err)
	}
}

func TestTwoNewOneEntry(t *testing.T) {
	dir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working dir: %v", err)
	}
	rel, err := filepath.Rel(cwd, dir)
	if err != nil {
		t.Skipf("No relative path to %s: %v", dir, err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(dir, link); err != nil {
		t.Skipf("Cannot create a symlink: %v", err)
	}

	var got []*Queue
	for _, root := range []string{"./" + rel, dir, link} {
		qs := mustNew(t, root)
		defer qs.Close()
		q, err := qs.NewQueue("q")
		if err != nil {
			t.Fatalf("Failed to create queue through %s: %v", root, err)
		}
		got = append(got, q)
	}

	for i, q := range got[1:] {
		if q.f != got[0].f {
			t.Errorf("Root %d reached a different file entry", i+1)
		}
		if q != got[0] {
			t.Errorf("Root %d reached a different handle", i+1)
		}
	}
}

func TestSingleFileAndFolderOneEntry(t *testing.T) {
	dir := t.TempDir()
	folder := mustNew(t, dir)
	defer folder.Close()
	single := mustNew(t, filepath.Join(dir, "jobs.db"), WithSingleFile())
	defer single.Close()

	a, err := folder.NewQueue("jobs")
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	b, err := single.NewQueue("JOBS")
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	if a != b {
		t.Error("Expected the folder and single-file stores to share the handle")
	}
}

func TestNoSecondRecovery(t *testing.T) {
	dir := t.TempDir()
	a := mustNew(t, dir)
	defer a.Close()

	qa, err := a.NewQueue("q")
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	qa.Enqueue([]byte("job"))
	_, ok, ackID := qa.DequeueWithAckId()
	if !ok {
		t.Fatal("DequeueWithAckId failed")
	}

	b := mustNew(t, dir)
	defer b.Close()
	qb, err := b.NewQueue("q")
	if err != nil {
		t.Fatalf("Failed to create queue through the second store: %v", err)
	}
	if qb.Len() != 0 {
		t.Errorf("Expected the claimed job to stay claimed, got length %d", qb.Len())
	}
	if !qa.Acknowledge(ackID) {
		t.Error("Acknowledge failed after the second store opened the queue")
	}
}

func TestRefCount(t *testing.T) {
	dir := t.TempDir()
	a := mustNew(t, dir)
	b := mustNew(t, dir)

	qa, err := a.NewQueue("q")
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	qb, err := b.NewQueue("q")
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	f := qa.f

	if err := a.Close(); err != nil {
		t.Fatalf("Failed to close the first store: %v", err)
	}
	if !qb.Enqueue([]byte("still open")) {
		t.Error("Enqueue failed after closing the other store")
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Failed to close the second store: %v", err)
	}
	if err := f.db.Ping(); err == nil {
		t.Error("Expected the db closed after the last store closed")
	}
	registry.mu.Lock()
	_, ok := registry.files[f.key]
	registry.mu.Unlock()
	if ok {
		t.Error("Expected the registry entry dropped after the last store closed")
	}
}

func TestFailedOpenReleases(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.db")

	// A view named like the queue makes table init fail
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	if _, err := db.Exec(`CREATE VIEW "bad" AS SELECT 1 AS x`); err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}
	db.Close()

	qs := mustNew(t, dir)
	defer qs.Close()
	if _, err := qs.NewQueue("bad"); err == nil {
		t.Fatal("Expected table init to fail")
	}

	key, err := resolve(path)
	if err != nil {
		t.Fatalf("Failed to resolve %s: %v", path, err)
	}
	registry.mu.Lock()
	_, ok := registry.files[key]
	registry.mu.Unlock()
	if ok {
		t.Error("Expected no registry entry after a failed open")
	}
	if n := len(qs.(*queues).files); n != 0 {
		t.Errorf("Expected the store to hold no files, got %d", n)
	}
}

func TestCheckpointMismatch(t *testing.T) {
	dir := t.TempDir()
	a := mustNew(t, dir)
	defer a.Close()
	b := mustNew(t, dir, WithCheckpointPages(4000))
	defer b.Close()

	if _, err := a.NewQueue("q"); err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	_, err := b.NewQueue("q")
	if err == nil {
		t.Fatal("Expected an error for a different checkpoint pages value")
	}
	if !strings.Contains(err.Error(), "1000") {
		t.Errorf("Expected the error to name 1000, got %q", err)
	}
}

func TestQueueCloseKeepsAck(t *testing.T) {
	qs := mustNew(t, t.TempDir())
	defer qs.Close()

	q, err := qs.NewQueue("q")
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	q.Enqueue([]byte("a"))
	q.Enqueue([]byte("b"))
	_, ok, ackID := q.DequeueWithAckId()
	if !ok {
		t.Fatal("DequeueWithAckId failed")
	}

	if err := q.Close(); err != nil {
		t.Fatalf("Failed to close queue: %v", err)
	}
	if q.Enqueue([]byte("c")) {
		t.Error("Expected Enqueue to fail on a closed queue")
	}
	if _, ok := q.Dequeue(); ok {
		t.Error("Expected Dequeue to fail on a closed queue")
	}
	if !q.Acknowledge(ackID) {
		t.Error("Expected Acknowledge to work on a closed queue")
	}

	fresh, err := qs.NewQueue("q")
	if err != nil {
		t.Fatalf("Failed to reopen queue: %v", err)
	}
	if fresh == q {
		t.Error("Expected a new handle for a closed queue's name")
	}
	if !fresh.Enqueue([]byte("d")) {
		t.Error("Enqueue failed on the new handle")
	}
}

func TestQueuesClosed(t *testing.T) {
	qs := mustNew(t, t.TempDir())
	if err := qs.Close(); err != nil {
		t.Fatalf("Failed to close: %v", err)
	}
	if _, err := qs.NewQueue("q"); err == nil {
		t.Error("Expected an error from NewQueue after Close")
	}
	if _, err := qs.NewPriorityQueue("pq"); err == nil {
		t.Error("Expected an error from NewPriorityQueue after Close")
	}
}

func TestConcurrentOpen(t *testing.T) {
	dir := t.TempDir()
	keep := mustNew(t, dir)
	defer keep.Close()
	want, err := keep.NewQueue("q")
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			qs, err := New(dir)
			if err != nil {
				t.Errorf("New failed: %v", err)
				return
			}
			defer qs.Close()
			q, err := qs.NewQueue("Q")
			if err != nil {
				t.Errorf("NewQueue failed: %v", err)
				return
			}
			if q != want {
				t.Error("Expected the shared handle")
			}
			if _, err := qs.NewPriorityQueue(fmt.Sprintf("p%d", i)); err != nil {
				t.Errorf("NewPriorityQueue failed: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if !want.Enqueue([]byte("still open")) {
		t.Error("Enqueue failed after the other stores closed")
	}
}
