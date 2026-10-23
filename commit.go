package sqliteq

import (
	"database/sql"
	"sync"
)

// op is one caller's write waiting for a round of the group commit. run
// executes in whichever goroutine leads the round, so it must not call user
// code (such as a driver.Valuer): a panic there would strand the round and
// leave its transaction open. Convert user values before building it.
type op struct {
	run  func(tx *sql.Tx) (bool, error) // one caller's statement(s); a nil tx means autocommit
	done chan result                    // buffered 2: one lead wake-up and the outcome
}

// result is what an op's caller receives: its own outcome, or with lead set,
// the turn to commit for the others
type result struct{ ok, lead bool }

// batcher groups concurrent writes on one file into shared transactions. The
// first caller with no round running leads: it commits what has queued up, at
// most its own round plus one more, then gives up the lead, so no caller
// commits for others for long under steady load. Whoever comes first takes
// the lead next: a new caller, or the oldest waiting one, which is woken for
// it. The next round never waits on that wake-up while a caller is running.
type batcher struct {
	mu      sync.Mutex
	pending []*op
	leading bool
	commit  func(ops []*op) // sends exactly one {ok} result to every op
}

// do runs one write in the next round and returns its own outcome
func (b *batcher) do(run func(*sql.Tx) (bool, error)) bool {
	o := &op{run: run, done: make(chan result, 2)}

	b.mu.Lock()
	b.pending = append(b.pending, o)
	for b.leading {
		b.mu.Unlock()
		r := <-o.done
		if !r.lead {
			return r.ok
		}
		// Woken for the lead: take it if nobody has and o is still first,
		// else o is in someone's round and its outcome follows
		b.mu.Lock()
		if !b.leading && len(b.pending) > 0 && b.pending[0] == o {
			break
		}
		if !b.leading {
			b.mu.Unlock()
			return o.outcome()
		}
	}
	b.leading = true
	b.lead()

	return o.outcome()
}

// outcome waits for the op's result, skipping a wake-up that came too late
func (o *op) outcome() bool {
	for {
		if r := <-o.done; !r.lead {
			return r.ok
		}
	}
}

// lead commits up to two rounds, then gives up the lead and wakes the oldest
// waiting op, if any, to take it. b.mu is held on entry and released on
// return; it is never held across a commit.
func (b *batcher) lead() {
	for range 2 {
		round := b.pending
		if len(round) == 0 {
			break
		}
		b.pending = nil
		b.mu.Unlock()
		b.commit(round)
		b.mu.Lock()
	}

	b.leading = false
	// A pending op holds no outcome, so an empty buffer means no wake-up is unread
	if len(b.pending) > 0 && len(b.pending[0].done) == 0 {
		b.pending[0].done <- result{lead: true}
	}
	b.mu.Unlock()
}

// commitRound runs a round's ops in one transaction under the write mutex.
// If any step fails, nothing of it is kept and each op runs again in a
// transaction of its own, so one bad call fails alone. A round of one op runs
// with a nil tx, as one autocommit statement: a database/sql Tx costs a
// context and a goroutine, which a round with nothing to share does not repay.
func (f *file) commitRound(ops []*op) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rounds.Add(1)

	if len(ops) == 1 {
		ok, err := ops[0].run(nil)
		ops[0].done <- result{ok: err == nil && ok}
		return
	}

	oks := make([]bool, len(ops))
	err := f.runTx(ops, oks)
	if err == nil {
		for i, o := range ops {
			o.done <- result{ok: err == nil && oks[i]}
		}
		return
	}

	for i, o := range ops {
		err := f.runTx(ops[i:i+1], oks[i:i+1])
		o.done <- result{ok: err == nil && oks[i]}
	}
}

// runTx runs ops in one transaction, storing each op's outcome in oks; on an
// error the transaction is rolled back
func (f *file) runTx(ops []*op, oks []bool) error {
	tx, err := f.db.Begin()
	if err != nil {
		return err
	}
	for i, o := range ops {
		if oks[i], err = o.run(tx); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}
