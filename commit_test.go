package sqliteq

import (
	"bytes"
	"database/sql"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

// goid returns the calling goroutine's id, parsed from its stack header.
func goid() uint64 {
	buf := make([]byte, 64)
	buf = buf[:runtime.Stack(buf, false)]
	buf = bytes.TrimPrefix(buf, []byte("goroutine "))
	id, _ := strconv.ParseUint(string(buf[:bytes.IndexByte(buf, ' ')]), 10, 64)
	return id
}

// waitAll fails the test if wg is not done within d.
func waitAll(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("Calls did not return within %v", d)
	}
}

// runOps is a fake commit: it runs every op without a transaction and sends
// each its own result.
func runOps(ops []*op) {
	for _, o := range ops {
		ok, err := o.run(nil)
		o.done <- result{ok: ok && err == nil}
	}
}

func TestBatcherEveryCallGetsItsResult(t *testing.T) {
	b := &batcher{commit: runOps}

	const goroutines, calls = 50, 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < calls; i++ {
				want := i%7 != 0
				if got := b.do(func(*sql.Tx) (bool, error) { return want, nil }); got != want {
					t.Errorf("goroutine %d call %d: got %t, want %t", g, i, got, want)
				}
			}
		}(g)
	}
	waitAll(t, &wg, time.Minute)
}

func TestBatcherLeaderHandsOff(t *testing.T) {
	// Only the leader runs commit, so these need no lock of their own
	type round struct {
		leader  uint64
		callers map[uint64]bool // goroutines with an op in the round
	}
	var (
		rounds []round
		caller map[uint64]bool // the round being committed
	)
	b := &batcher{}
	b.commit = func(ops []*op) {
		caller = map[uint64]bool{}
		runOps(ops) // each run records its caller
		rounds = append(rounds, round{goid(), caller})
		time.Sleep(50 * time.Microsecond)
	}

	const goroutines, calls = 32, 300
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			me := goid()
			for i := 0; i < calls; i++ {
				if !b.do(func(*sql.Tx) (bool, error) { caller[me] = true; return true, nil }) {
					t.Errorf("call %d failed", i)
				}
			}
		}()
	}
	waitAll(t, &wg, time.Minute)

	// A goroutine has one call in flight, so a round holding the leader's own
	// op starts its turn; a round without it must be the second of that turn
	n := 0
	for i, r := range rounds {
		n += len(r.callers)
		if r.callers[r.leader] {
			continue
		}
		if i == 0 || rounds[i-1].leader != r.leader || !rounds[i-1].callers[r.leader] {
			t.Fatalf("round %d: goroutine %d led more than its own round plus one", i, r.leader)
		}
	}
	if n != goroutines*calls {
		t.Errorf("Rounds held %d calls, want %d", n, goroutines*calls)
	}
}

// oneRound runs first and, once it leads a round blocked on the file mutex,
// starts rest, so that every call of rest commits in the one round after
// first's. It returns each call's result, first's at index 0.
func oneRound(t *testing.T, f *file, first func() bool, rest []func() bool) []bool {
	t.Helper()
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(time.Millisecond) {
			f.b.mu.Lock()
			ok := cond()
			f.b.mu.Unlock()
			if ok {
				return
			}
			if time.Now().After(deadline) {
				f.mu.Unlock()
				t.Fatalf("Timed out waiting for %s", what)
			}
		}
	}

	got := make([]bool, 1+len(rest))
	var wg sync.WaitGroup
	call := func(i int, fn func() bool) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got[i] = fn()
		}()
	}

	before := f.rounds.Load()
	f.mu.Lock()
	call(0, first)
	waitFor("a leader", func() bool { return f.b.leading && len(f.b.pending) == 0 })
	for i, fn := range rest {
		call(i+1, fn)
	}
	waitFor("the calls to queue up", func() bool { return len(f.b.pending) == len(rest) })
	f.mu.Unlock()
	waitAll(t, &wg, 10*time.Second)

	if n := f.rounds.Load() - before; n != 2 {
		t.Errorf("Expected 2 rounds, got %d", n)
	}
	return got
}

// TestGroupCommitOneBadCall checks that an add that fails inside a shared
// round fails alone.
func TestGroupCommitOneBadCall(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()

	add := func() bool { return q.Enqueue([]byte("x")) }
	rest := []func() bool{func() bool { return q.Enqueue(nil) }}
	for i := 0; i < 63; i++ {
		rest = append(rest, add)
	}

	got := oneRound(t, q.f, add, rest)
	if got[1] {
		t.Error("Enqueue(nil) succeeded")
	}
	for i, ok := range got {
		if i != 1 && !ok {
			t.Errorf("Enqueue %d failed", i)
		}
	}
	if n := q.Len(); n != 64 {
		t.Errorf("Expected 64 pending items, got %d", n)
	}
}

// TestGroupAckAfterFailedRound shares one round between 32 good acks, 4 acks
// of ids no row has, and a failing add, so the round fails and falls back.
func TestGroupAckAfterFailedRound(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	enqueueN(t, q, 32)

	items, ackIDs := make([]any, 32), make([]string, 32)
	if n, _, err := q.DequeueBatchWithAckIds(items, ackIDs); err != nil || n != 32 {
		t.Fatalf("Claimed %d, %v; want 32", n, err)
	}

	ack := func(id string) func() bool { return func() bool { return q.Acknowledge(id) } }
	rest := []func() bool{}
	for _, id := range ackIDs[1:] {
		rest = append(rest, ack(id))
	}
	for i := 0; i < 4; i++ {
		rest = append(rest, ack(strconv.Itoa(1_000_000+i))) // numeric, so they reach the round
	}
	rest = append(rest, func() bool { return q.Enqueue(nil) })

	got := oneRound(t, q.f, ack(ackIDs[0]), rest)
	for i := 0; i < 32; i++ {
		if !got[i] {
			t.Errorf("Acknowledge(%s) failed", ackIDs[i])
		}
	}
	for i := 32; i < len(got); i++ {
		if got[i] {
			t.Errorf("Call %d succeeded, want false", i)
		}
	}
	if n := countRows(t, q.f.db, "1"); n != 0 {
		t.Errorf("Expected no rows left, got %d", n)
	}
}

// TestCommitRoundFallsBackPerOp runs a round whose second op errors: the
// grouped transaction rolls back, and each op then commits alone.
func TestCommitRoundFallsBackPerOp(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()

	add := func(tx *sql.Tx) (bool, error) {
		_, err := tx.Stmt(q.insert).Exec([]byte("x"), time.Now(), time.Now())
		return err == nil, err
	}
	bad := func(*sql.Tx) (bool, error) { return false, errors.New("bad op") }
	ops := []*op{
		{run: add, done: make(chan result, 1)},
		{run: bad, done: make(chan result, 1)},
		{run: add, done: make(chan result, 1)},
	}
	q.f.commitRound(ops)

	for i, want := range []bool{true, false, true} {
		select {
		case r := <-ops[i].done:
			if r.ok != want || r.lead {
				t.Errorf("op %d: got %+v, want ok=%t", i, r, want)
			}
		default:
			t.Errorf("op %d got no result", i)
		}
		if len(ops[i].done) != 0 {
			t.Errorf("op %d got more than one result", i)
		}
	}
	if n := q.Len(); n != 2 {
		t.Errorf("Expected 2 pending items, got %d", n)
	}
}

// TestCommitRoundOneOpAutocommits checks that a round of one op runs without
// a transaction, and a round of more shares one.
func TestCommitRoundOneOpAutocommits(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()

	var txs []*sql.Tx
	record := func(tx *sql.Tx) (bool, error) {
		txs = append(txs, tx)
		return true, nil
	}
	newOp := func() *op { return &op{run: record, done: make(chan result, 1)} }

	q.f.commitRound([]*op{newOp()})
	q.f.commitRound([]*op{newOp(), newOp()})

	if len(txs) != 3 {
		t.Fatalf("Expected 3 runs, got %d", len(txs))
	}
	if txs[0] != nil {
		t.Error("A round of one op ran in a transaction")
	}
	if txs[1] == nil || txs[1] != txs[2] {
		t.Error("A round of two ops did not share one transaction")
	}
}

// TestGroupCommitBatches checks that concurrent writers share rounds.
func TestGroupCommitBatches(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()

	before := q.f.rounds.Load()
	ops := concurrentWriters(t, q)
	rounds := q.f.rounds.Load() - before
	t.Logf("%d writes in %d rounds", ops, rounds)
	if rounds >= int64(ops) {
		t.Errorf("Expected fewer rounds than %d writes, got %d", ops, rounds)
	}
}

// TestGroupCommitAfterClose closes the store under running writers: every
// call still returns.
func TestGroupCommitAfterClose(t *testing.T) {
	q, qs := openQueue(t)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for q.Enqueue([]byte("x")) {
				q.Acknowledge("1")
			}
		}()
	}
	time.Sleep(20 * time.Millisecond)
	if err := qs.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
	waitAll(t, &wg, 10*time.Second)
}
