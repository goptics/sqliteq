package sqliteq

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// mustNew opens a single-file store at path and fails the test on error.
func mustNew(t *testing.T, path string, opts ...StoreOption) Queues {
	t.Helper()
	qs, err := New(path, append([]StoreOption{WithSingleFile()}, opts...)...)
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
			qs := mustNew(t, filepath.Join(t.TempDir(), "ckpt.db"), tc.opts...)
			defer qs.Close()

			if _, err := qs.NewQueue("q"); err != nil {
				t.Fatalf("Failed to create queue: %v", err)
			}

			for i, v := range pragmaOnConns(t, qs.(*queues).client, "wal_autocheckpoint", 3) {
				if v != tc.want {
					t.Errorf("conn %d: wal_autocheckpoint = %d, want %d", i, v, tc.want)
				}
			}
		})
	}
}

func TestSyncAndBusyTimeout(t *testing.T) {
	qs := mustNew(t, filepath.Join(t.TempDir(), "pragmas.db"))
	defer qs.Close()

	if _, err := qs.NewQueue("q"); err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	db := qs.(*queues).client

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
