package sqliteq

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/goptics/varmq"
)

// requeueNoAck is what a restart finds: this process's owner is dead, so a
// sweep puts the rows it claimed back to pending.
func (q *Queue) requeueNoAck() error {
	f := q.f
	f.mu.Lock()
	f.claimed = false
	_, err := f.db.Exec("DELETE FROM __sqliteq_owners WHERE id = ?", f.owner)
	f.mu.Unlock()
	if err != nil {
		return err
	}
	_, err = f.sweep([]*Queue{q}, true)
	return err
}

// fastStore is a store whose files get heartbeat timeout hb, below what New
// takes.
func fastStore(t *testing.T, path string, hb time.Duration, opts ...StoreOption) Queues {
	t.Helper()
	qs := mustNew(t, path, opts...)
	qs.(*queues).cfg.heartbeat = hb
	t.Cleanup(func() { qs.Close() })
	return qs
}

// variants are the queue kinds and layouts every owner case runs on.
var variants = []struct {
	name             string
	priority, single bool
	opts             []Option
}{
	{name: "fifo"},
	{name: "priority", priority: true},
	{name: "keep", opts: []Option{WithRemoveOnComplete(false)}},
	{name: "single file", single: true},
}

// eachVariant runs fn on queue "q" of each variant, opened with timeout hb.
func eachVariant(t *testing.T, hb time.Duration, fn func(t *testing.T, q *Queue, qs Queues)) {
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			path := t.TempDir()
			var so []StoreOption
			if v.single {
				path, so = filepath.Join(path, "one.db"), []StoreOption{WithSingleFile()}
			}
			qs := fastStore(t, path, hb, so...)
			if v.priority {
				pq, err := qs.NewPriorityQueue("q", v.opts...)
				if err != nil {
					t.Fatal(err)
				}
				fn(t, pq.Queue, qs)
				return
			}
			q, err := qs.NewQueue("q", v.opts...)
			if err != nil {
				t.Fatal(err)
			}
			fn(t, q, qs)
		})
	}
}

// kill stops f as a killed process stops: no hand-back, its owner row and
// its processing rows stay. A later release closes nothing twice.
func kill(f *file) {
	f.mu.Lock()
	f.closing = true
	f.mu.Unlock()
	registry.mu.Lock()
	sweeping := f.sweeping
	f.sweeping = false
	registry.mu.Unlock()
	if sweeping {
		close(f.stop)
		<-f.done
	}
	f.db.Close()
}

// claim1 adds one item to q and claims it.
func claim1(t *testing.T, q *Queue) (ack string) {
	t.Helper()
	if !q.enqueue([]byte("job"), 0, "") {
		t.Fatal("enqueue failed")
	}
	_, ok, ack := q.DequeueWithAckId()
	if !ok {
		t.Fatal("claim failed")
	}
	return ack
}

// rowOf reads the status and owner of table q's one row through db.
func rowOf(t *testing.T, db *sql.DB) (status string, owner sql.NullString) {
	t.Helper()
	if err := db.QueryRow(`SELECT status, owner FROM "q"`).Scan(&status, &owner); err != nil {
		t.Fatal(err)
	}
	return status, owner
}

func ownerRows(t *testing.T, db *sql.DB) (n int) {
	t.Helper()
	if err := db.QueryRow("SELECT COUNT(*) FROM __sqliteq_owners").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out: %s", what)
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

// A second process that opens the table and sweeps leaves a live owner's
// row alone.
func TestOwnerLive(t *testing.T) {
	const hb = 300 * time.Millisecond
	eachVariant(t, hb, func(t *testing.T, a *Queue, qs Queues) {
		ack := claim1(t, a)
		b := otherProcess(t, a)
		time.Sleep(2 * hb) // both sweep, a beats
		if status, owner := rowOf(t, b.f.db); status != "processing" || owner.String != a.f.owner {
			t.Fatalf("Expected a's row processing under %s, got %s under %q", a.f.owner, status, owner.String)
		}
		if n := b.Len(); n != 0 {
			t.Fatalf("Expected nothing pending for b, got %d", n)
		}
		if !a.Acknowledge(ack) {
			t.Fatal("Expected a's ack to succeed")
		}
	})
}

// A survivor that opened first and never claims gets a killed owner's row
// back within one timeout, with no reopen, and its hooks are woken.
func TestOwnerDead(t *testing.T) {
	const hb = 300 * time.Millisecond
	eachVariant(t, hb, func(t *testing.T, a *Queue, qs Queues) {
		woke := make(chan bool, 16)
		defer a.OnPauseChange(func(p bool) { woke <- p })()
		b := otherProcess(t, a)
		claim1(t, b)
		kill(b.f)
		if status, _ := rowOf(t, a.f.db); status != "processing" {
			t.Fatalf("Expected the row processing right after the kill, got %s", status)
		}
		start := time.Now()
		eventually(t, "dead owner's row pending", func() bool { return a.Len() == 1 })
		if d := time.Since(start); d > 5*time.Second {
			t.Errorf("Recovery took %s", d)
		}
		if _, owner := rowOf(t, a.f.db); owner.Valid {
			t.Errorf("Expected the owner cleared, got %q", owner.String)
		}
		select {
		case p := <-woke:
			if p {
				t.Error("Expected the wake to report running")
			}
		case <-time.After(5 * time.Second):
			t.Error("Expected the hooks woken after the requeue")
		}
		if a.f.claimed {
			t.Error("Expected the survivor to have no owner row before its first claim")
		}
		_, ok, ack := a.DequeueWithAckId()
		if !ok || !a.Acknowledge(ack) {
			t.Fatal("Expected the survivor to run the recovered job")
		}
		eventually(t, "dead owner's row dropped", func() bool { return ownerRows(t, a.f.db) == 1 })
	})
}

// Processes that open the queue and are gone before their second sweep
// still get a dead owner's row back: its heartbeat moves on once only.
func TestOwnerDeadShortLived(t *testing.T) {
	const hb = 600 * time.Millisecond
	qs := fastStore(t, t.TempDir(), hb)
	a, err := qs.NewQueue("q")
	if err != nil {
		t.Fatal(err)
	}
	claim1(t, a)
	kill(a.f)
	for i := 0; ; i++ {
		time.Sleep(hb / 2) // above the gap limit
		f, err := newFile(a.f.key, strings.TrimPrefix(a.f.scope, "sqliteq:"), a.f.pages, hb)
		if err != nil {
			t.Fatal(err)
		}
		h, err := f.open("q", false, nil)
		if err != nil {
			t.Fatal(err)
		}
		n := h.q.Len()
		f.release()
		if n == 1 {
			return
		}
		if i == 10 {
			t.Fatal("Expected the dead owner's row back within a few opens")
		}
	}
}

// A requeue by another process's sweep, or by its hand-back, is seen by
// this process's next sweep, which wakes its queues.
func TestWakeAfterOthersRequeue(t *testing.T) {
	a, qs := openQueue(t)
	defer qs.Close()
	sweep := func(q *Queue) bool {
		t.Helper()
		wake, err := q.f.sweep([]*Queue{q}, false)
		if err != nil {
			t.Fatal(err)
		}
		return wake
	}
	if sweep(a) {
		t.Fatal("Expected no wake with nothing requeued")
	}
	observer, dead := otherProcess(t, a), otherProcess(t, a)
	claim1(t, dead)
	kill(dead.f)
	mustExec(t, a.f.db, "UPDATE __sqliteq_owners SET expires_at = 0")
	sweep(observer) // requeues
	if a.Len() != 1 || !sweep(a) || sweep(a) {
		t.Fatal("Expected one wake after the observer's requeue")
	}

	leaver := otherProcess(t, a)
	if _, ok, _ := leaver.DequeueWithAckId(); !ok {
		t.Fatal("claim failed")
	}
	leaver.f.release()
	if a.Len() != 1 || !sweep(a) {
		t.Fatal("Expected a wake after another process's hand-back")
	}
}

// Opening another queue of the file does not use up a requeue the sweep
// loop has not seen.
func TestOpenKeepsWake(t *testing.T) {
	qs := mustNew(t, filepath.Join(t.TempDir(), "one.db"), WithSingleFile())
	defer qs.Close()
	a, err := qs.NewQueue("q")
	if err != nil {
		t.Fatal(err)
	}
	observer, dead := otherProcess(t, a), otherProcess(t, a)
	claim1(t, dead)
	kill(dead.f)
	mustExec(t, a.f.db, "UPDATE __sqliteq_owners SET expires_at = 0")
	if _, err := observer.f.sweep([]*Queue{observer}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := qs.NewQueue("y"); err != nil {
		t.Fatal(err)
	}
	if wake, err := a.f.sweep([]*Queue{a}, false); err != nil || !wake {
		t.Fatalf("Expected the next sweep to wake after an open in between, got %t %v", wake, err)
	}
}

// After a crash the restarted process gets its old rows back one timeout
// later, not at open.
func TestOwnerRestart(t *testing.T) {
	const hb = 300 * time.Millisecond
	dir := t.TempDir()
	qs := fastStore(t, dir, hb)
	a, err := qs.NewQueue("q")
	if err != nil {
		t.Fatal(err)
	}
	claim1(t, a)
	kill(a.f)
	qs.Close()

	qs = fastStore(t, dir, hb)
	a, err = qs.NewQueue("q")
	if err != nil {
		t.Fatal(err)
	}
	if n := a.Len(); n != 0 {
		t.Fatalf("Expected the row to wait for the old heartbeat, got %d pending", n)
	}
	eventually(t, "old row pending", func() bool { return a.Len() == 1 })
}

// An ack or reject from the owner that lost a row does not end the run of
// the owner that holds it now.
func TestStaleAck(t *testing.T) {
	eachVariant(t, defaultHeartbeat, func(t *testing.T, a *Queue, qs Queues) {
		ack := claim1(t, a)
		mustExec(t, a.f.db, "UPDATE __sqliteq_owners SET expires_at = 0 WHERE id = ?", a.f.owner) // a froze
		b := otherProcess(t, a)                                                                   // its first sweep requeues
		_, ok, ackB := b.DequeueWithAckId()
		if !ok || ackB != ack {
			t.Fatalf("Expected b to claim the same row, got %t %q", ok, ackB)
		}
		if a.Acknowledge(ack) || a.Reject(ack, "boom") {
			t.Fatal("Expected a's stale ack and reject to fail")
		}
		if found, err := a.finishDirect(ack, "completed", "", nil); found || err != nil {
			t.Fatalf("Expected a's stale direct finish to find nothing, got %t %v", found, err)
		}
		if status, owner := rowOf(t, a.f.db); status != "processing" || owner.String != b.f.owner {
			t.Fatalf("Expected the row processing under b, got %s under %q", status, owner.String)
		}
		if !b.Acknowledge(ackB) {
			t.Fatal("Expected b's ack to succeed")
		}
	})
}

// A processing row without an owner, from an older version, goes back on a
// table's first open only.
func TestLegacyRows(t *testing.T) {
	a, qs := openQueue(t)
	defer qs.Close()
	mustExec(t, a.f.db, `INSERT INTO "q" (data, status, ack) VALUES (x'00', 'processing', 0)`)
	if _, err := a.f.sweep([]*Queue{a}, false); err != nil {
		t.Fatal(err)
	}
	if status, _ := rowOf(t, a.f.db); status != "processing" {
		t.Fatalf("Expected the ticker sweep to leave the row, got %s", status)
	}
	otherProcess(t, a)
	if status, _ := rowOf(t, a.f.db); status != "pending" {
		t.Fatalf("Expected a first open to requeue the row, got %s", status)
	}
}

// The last Close hands this owner's rows back, drops its owner row and
// stops the sweep.
func TestCleanClose(t *testing.T) {
	eachVariant(t, defaultHeartbeat, func(t *testing.T, a *Queue, qs Queues) {
		ack := claim1(t, a)
		claim1(t, a)
		if !a.Acknowledge(ack) {
			t.Fatal("ack failed")
		}
		f := a.f
		kept := !a.removeOnComplete
		if err := qs.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-f.done:
		default:
			t.Fatal("Expected the sweep loop stopped")
		}

		db, err := sql.Open("sqlite3", f.key)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		var pending, owned int
		if err := db.QueryRow(`SELECT COUNT(*) FILTER (WHERE status = 'pending'), COUNT(owner) FILTER (WHERE status <> 'completed') FROM "q"`).Scan(&pending, &owned); err != nil {
			t.Fatal(err)
		}
		if pending != 1 || owned != 0 || ownerRows(t, db) != 0 {
			t.Fatalf("Expected 1 pending row, no owner (kept %t), got %d pending, %d owned, %d owner rows", kept, pending, owned, ownerRows(t, db))
		}
	})
}

// A long gap since the last sweep, a clock set back, or no clock row takes
// no owner for dead.
func TestClockGap(t *testing.T) {
	setup := func(t *testing.T) (a *Queue, dead string) {
		a, qs := openQueue(t)
		t.Cleanup(func() { qs.Close() })
		b := otherProcess(t, a)
		claim1(t, b)
		kill(b.f)
		return a, b.f.owner
	}
	sweep := func(t *testing.T, a *Queue) (status string, expires int64) {
		t.Helper()
		if _, err := a.f.sweep([]*Queue{a}, false); err != nil {
			t.Fatal(err)
		}
		status, _ = rowOf(t, a.f.db)
		a.f.db.QueryRow("SELECT expires_at FROM __sqliteq_owners").Scan(&expires)
		return status, expires
	}
	const hour = int64(time.Hour / time.Millisecond)

	t.Run("long gap", func(t *testing.T) {
		a, dead := setup(t)
		now := time.Now().UnixMilli()
		mustExec(t, a.f.db, "UPDATE __sqliteq_clock SET swept_at = ?", now-hour)
		mustExec(t, a.f.db, "UPDATE __sqliteq_owners SET expires_at = ? WHERE id = ?", now-hour+1000, dead)
		if status, expires := sweep(t, a); status != "processing" || expires < now {
			t.Fatalf("Expected the owner kept after a long gap, got %s, expires %d ms from now", status, expires-now)
		}
		// A second long gap with no beat of the owner between moves nothing
		now = time.Now().UnixMilli()
		mustExec(t, a.f.db, "UPDATE __sqliteq_clock SET swept_at = ?", now-hour)
		mustExec(t, a.f.db, "UPDATE __sqliteq_owners SET expires_at = ?", now-1)
		if status, _ := sweep(t, a); status != "pending" || ownerRows(t, a.f.db) != 0 {
			t.Fatalf("Expected the dead owner reaped, got %s, %d owner rows", status, ownerRows(t, a.f.db))
		}
	})
	t.Run("clock set back", func(t *testing.T) {
		a, dead := setup(t)
		now := time.Now().UnixMilli()
		mustExec(t, a.f.db, "UPDATE __sqliteq_clock SET swept_at = ?", now+hour)
		mustExec(t, a.f.db, "UPDATE __sqliteq_owners SET expires_at = ? WHERE id = ?", now+hour+1000, dead)
		status, expires := sweep(t, a)
		if status != "processing" || expires < now || expires > now+10_000 {
			t.Fatalf("Expected the heartbeat moved back with the clock, got %s, expires %d ms from now", status, expires-now)
		}
	})
	// A first claim right after the clock went back is not moved with it
	t.Run("claim after clock set back", func(t *testing.T) {
		a, _ := openQueue(t)
		b := otherProcess(t, a)
		mustExec(t, a.f.db, "UPDATE __sqliteq_clock SET swept_at = ?", time.Now().UnixMilli()+hour)
		ack := claim1(t, b)
		if status, _ := sweep(t, a); status != "processing" || !b.Acknowledge(ack) {
			t.Fatalf("Expected the fresh owner kept, got %s", status)
		}
	})
	t.Run("unreadable clock", func(t *testing.T) {
		a, _ := setup(t)
		mustExec(t, a.f.db, "UPDATE __sqliteq_clock SET swept_at = 'x'")
		if _, err := a.f.sweep([]*Queue{a}, false); err != nil {
			t.Fatalf("Expected the sweep to repair the clock, got %v", err)
		}
	})
	t.Run("no clock row", func(t *testing.T) {
		a, _ := setup(t)
		mustExec(t, a.f.db, "DELETE FROM __sqliteq_clock")
		mustExec(t, a.f.db, "UPDATE __sqliteq_owners SET expires_at = 0")
		if status, _ := sweep(t, a); status != "processing" {
			t.Fatalf("Expected no owner taken for dead without a clock, got %s", status)
		}
		if status, _ := sweep(t, a); status != "pending" {
			t.Fatalf("Expected the next sweep to reap, got %s", status)
		}
	})
}

// A process that never claims writes no owner row.
func TestProducerOnly(t *testing.T) {
	a, qs := openQueue(t)
	defer qs.Close()
	a.Enqueue([]byte("x"))
	a.Len()
	a.Values()
	if a.Pause() != nil || a.Resume() != nil {
		t.Fatal("pause failed")
	}
	stats(t, a)
	a.Dequeue()
	if _, err := a.f.sweep([]*Queue{a}, false); err != nil {
		t.Fatal(err)
	}
	if n := ownerRows(t, a.f.db); n != 0 || a.f.claimed {
		t.Fatalf("Expected no owner row for a producer, got %d", n)
	}
	a.DequeueWithAckId() // an empty claim is a consumer's
	if n := ownerRows(t, a.f.db); n != 1 {
		t.Fatalf("Expected the first claim to write the owner row, got %d", n)
	}
}

// A process that froze past two intervals beats before it claims again.
func TestBeatAfterStall(t *testing.T) {
	a, qs := openQueue(t)
	defer qs.Close()
	claim1(t, a)
	a.f.mu.Lock()
	a.f.lastBeat -= time.Hour.Milliseconds()
	a.f.mu.Unlock()
	mustExec(t, a.f.db, "DELETE FROM __sqliteq_owners") // another process reaped it
	claim1(t, a)
	var expires int64
	if err := a.f.db.QueryRow("SELECT expires_at FROM __sqliteq_owners WHERE id = ?", a.f.owner).Scan(&expires); err != nil {
		t.Fatalf("Expected the owner row back before the claim: %v", err)
	}
	if left := expires - time.Now().UnixMilli(); left < 20_000 || left > 31_000 {
		t.Errorf("Expected about one timeout left, got %d ms", left)
	}
}

func TestHeartbeatOption(t *testing.T) {
	dir := t.TempDir()
	if _, err := New(dir, WithHeartbeatTimeout(4*time.Second)); err == nil {
		t.Error("Expected New to refuse a timeout under 5s")
	}
	qs := mustNew(t, dir)
	defer qs.Close()
	q, err := qs.NewQueue("q")
	if err != nil || q.f.hb != 30*time.Second {
		t.Fatalf("Expected the 30s default, got %v %v", q.f.hb, err)
	}
	other := mustNew(t, dir, WithHeartbeatTimeout(10*time.Second))
	defer other.Close()
	if _, err := other.NewQueue("q"); err == nil || !strings.Contains(err.Error(), "heartbeat timeout") {
		t.Errorf("Expected a file open under another timeout refused, got %v", err)
	}
	if q2, err := other.NewQueue("fresh"); err != nil || q2.f.hb != 10*time.Second {
		t.Errorf("Expected 10s on a new file, got %v", err)
	}
}

// A finish stays a rowid lookup with the owner match.
func TestFinishQueryPlan(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	for _, query := range []string{
		`DELETE FROM "q" WHERE id = 1 AND status = 'processing' AND owner = 'x' RETURNING job_id, data`,
		`UPDATE "q" SET status = 'completed', ack = 1 WHERE id = 1 AND status = 'processing' AND owner = 'x' RETURNING job_id, data`,
	} {
		var id, parent, unused int
		var detail string
		if err := q.f.db.QueryRow("EXPLAIN QUERY PLAN "+query).Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(detail, "USING INTEGER PRIMARY KEY (rowid=?)") {
			t.Errorf("%s\n\tplan %q", query, detail)
		}
	}
}

// A sweep with no dead owner reads no queue table, and a dead owner dropped
// by one process makes every other process read its tables once.
func TestSweepReadsTablesOnDeath(t *testing.T) {
	a, qs := openQueue(t)
	defer qs.Close()
	sweep := func(q *Queue) {
		t.Helper()
		if _, err := q.f.sweep([]*Queue{q}, false); err != nil {
			t.Fatal(err)
		}
	}
	// A row no live owner holds, with no owner row to drop: only a read of
	// the table finds it
	mustExec(t, a.f.db, `INSERT INTO "q" (data, status, ack, owner) VALUES (x'00', 'processing', 0, 'gone')`)
	sweep(a)
	if status, _ := rowOf(t, a.f.db); status != "processing" {
		t.Fatalf("Expected a sweep with no dead owner to read no table, got %s", status)
	}
	// Another process drops a dead owner: it has the table open too, so
	// take the row out of its reach by the count alone
	b := otherProcess(t, a)
	mustExec(t, a.f.db, `UPDATE "q" SET status = 'pending'`)
	mustExec(t, a.f.db, "INSERT INTO __sqliteq_owners (id, expires_at) VALUES ('dead', 0)")
	sweep(b)
	mustExec(t, a.f.db, `UPDATE "q" SET status = 'processing', owner = 'gone'`)
	sweep(a)
	if status, _ := rowOf(t, a.f.db); status != "pending" {
		t.Fatalf("Expected the count to make this process read its table, got %s", status)
	}
}

// TestHelperProcess is the second process of TestKilledProcess: a VarMQ
// worker that takes one job and never ends it.
func TestHelperProcess(t *testing.T) {
	dir := os.Getenv("SQLITEQ_HELPER_DIR")
	if dir == "" {
		t.Skip("helper of TestKilledProcess")
	}
	qs := mustNew(t, dir)
	qs.(*queues).cfg.heartbeat = 2 * time.Second
	q, err := qs.NewQueue("q")
	if err != nil {
		t.Fatal(err)
	}
	w := varmq.NewWorker(func(j varmq.Job[int]) {
		fmt.Println("claimed")
		select {}
	})
	w.WithQueue(q).Add(7)
	select {}
}

// Two real processes and a VarMQ worker in each: the job of a live process
// is not run twice, and the job of a killed one runs in the survivor, which
// opened first and never claimed, with no reopen.
func TestKilledProcess(t *testing.T) {
	dir := t.TempDir()
	qs := fastStore(t, dir, 2*time.Second)
	q, err := qs.NewQueue("q")
	if err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
	cmd.Env = append(os.Environ(), "SQLITEQ_HELPER_DIR="+dir)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
	}()
	claimed := make(chan bool, 1)
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			if sc.Text() == "claimed" {
				claimed <- true
				return
			}
		}
		claimed <- false
	}()
	select {
	case ok := <-claimed:
		if !ok {
			t.Fatal("helper ended before it claimed")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("helper did not claim")
	}

	ran := make(chan int, 4)
	w := varmq.NewWorker(func(j varmq.Job[int]) { ran <- j.Data() })
	defer w.Stop()
	w.WithQueue(q)
	select {
	case v := <-ran:
		t.Fatalf("job %d ran while its owner is alive", v)
	case <-time.After(3 * time.Second): // past one timeout
	}
	if status, _ := rowOf(t, q.f.db); status != "processing" {
		t.Fatalf("Expected the live owner's row processing, got %s", status)
	}

	cmd.Process.Kill()
	cmd.Wait()
	select {
	case v := <-ran:
		if v != 7 {
			t.Errorf("Expected job 7, got %d", v)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the killed owner's job did not run in the survivor")
	}
}
