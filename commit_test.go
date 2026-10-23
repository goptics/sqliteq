package sqliteq

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
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

// TestBatcherHandOffDoesNotWait checks that once a leader has ended its turn,
// the next round does not wait for the waiting op it woke: a caller arriving
// first takes the lead and commits that op with its own.
func TestBatcherHandOffDoesNotWait(t *testing.T) {
	gate := make(chan struct{})
	rounds := 0 // touched by the leader only
	b := &batcher{}
	b.commit = func(ops []*op) {
		if rounds < 2 {
			<-gate
		}
		rounds++
		runOps(ops)
	}
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
			b.mu.Lock()
			ok := cond()
			b.mu.Unlock()
			if ok {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("Timed out waiting for %s", what)
			}
		}
	}
	ok := func(*sql.Tx) (bool, error) { return true, nil }

	go b.do(ok) // leads round 1, held at the gate
	waitFor("round 1", func() bool { return b.leading && len(b.pending) == 0 })
	go b.do(ok) // waits, then commits in round 2
	waitFor("a waiter", func() bool { return len(b.pending) == 1 })
	gate <- struct{}{}
	waitFor("round 2", func() bool { return len(b.pending) == 0 })

	// An op whose caller is slow to wake: nothing reads its done
	slow := &op{run: ok, done: make(chan result, 2)}
	b.mu.Lock()
	b.pending = append(b.pending, slow)
	b.mu.Unlock()
	gate <- struct{}{} // the leader ends its turn with slow waiting
	waitFor("the turn to end", func() bool { return len(slow.done) == 1 })

	// A caller arriving before slow wakes up
	got := make(chan bool, 1)
	go func() { got <- b.do(ok) }()
	select {
	case r := <-got:
		if !r {
			t.Error("The new caller's write failed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("The new caller waited for the woken op to take the lead")
	}
	if r := slow.outcome(); !r {
		t.Error("The slow op's write failed")
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

// TestCommitRoundGroupedResults runs a round that commits: each op gets its
// own outcome, from one shared transaction.
func TestCommitRoundGroupedResults(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()

	want := []bool{true, false, true, true}
	txs := make([]*sql.Tx, len(want))
	ops := make([]*op, len(want))
	for i, ok := range want {
		ops[i] = &op{
			run: func(tx *sql.Tx) (bool, error) {
				txs[i] = tx
				return ok, nil
			},
			done: make(chan result, 2),
		}
	}
	q.f.commitRound(ops)

	for i, w := range want {
		if n := len(ops[i].done); n != 1 {
			t.Errorf("op %d got %d results, want 1", i, n)
			continue
		}
		if r := <-ops[i].done; r.ok != w || r.lead {
			t.Errorf("op %d: got %+v, want ok=%t", i, r, w)
		}
	}
	for i, tx := range txs {
		if tx == nil || tx != txs[0] {
			t.Errorf("op %d did not run in the round's one transaction", i)
		}
	}
}

// panicValuer is an item whose conversion for the driver panics.
type panicValuer struct{}

func (panicValuer) Value() (driver.Value, error) { panic("panicValuer") }

// TestEnqueuePanickingItem checks that an item panicking in its conversion
// panics in its own caller and leaves the file's group commit working.
func TestEnqueuePanickingItem(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()

	func() {
		defer func() {
			if recover() == nil {
				t.Error("Enqueue of a panicking item did not panic")
			}
		}()
		q.Enqueue(panicValuer{})
	}()

	done := make(chan error, 1)
	go func() {
		if !q.Enqueue([]byte("x")) {
			done <- errors.New("Enqueue failed")
			return
		}
		_, ok, ackID := q.DequeueWithAckId()
		if !ok {
			done <- errors.New("DequeueWithAckId failed")
			return
		}
		if !q.Acknowledge(ackID) {
			done <- errors.New("Acknowledge failed")
			return
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Writes after the panic did not return")
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

// TestGroupCommitBatches checks that writes queued behind a running round,
// adds and acks alike, all commit in the one round after it.
func TestGroupCommitBatches(t *testing.T) {
	q, qs := openQueue(t)
	defer qs.Close()
	enqueueN(t, q, 16)

	items, ackIDs := make([]any, 16), make([]string, 16)
	if n, _, err := q.DequeueBatchWithAckIds(items, ackIDs); err != nil || n != 16 {
		t.Fatalf("Claimed %d, %v; want 16", n, err)
	}

	add := func() bool { return q.Enqueue([]byte("x")) }
	rest := []func() bool{}
	for i := 0; i < 15; i++ {
		rest = append(rest, add)
	}
	for _, id := range ackIDs {
		rest = append(rest, func() bool { return q.Acknowledge(id) })
	}

	// oneRound fails the test unless exactly 2 rounds ran
	for i, ok := range oneRound(t, q.f, add, rest) {
		if !ok {
			t.Errorf("Call %d failed", i)
		}
	}
	if n := countRows(t, q.f.db, "1"); n != 16 {
		t.Errorf("Expected 16 rows (the adds), got %d", n)
	}
	if n := q.Len(); n != 16 {
		t.Errorf("Expected 16 pending items, got %d", n)
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
