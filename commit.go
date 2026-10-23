package sqliteq

import (
	"database/sql"
	"sync"
)

// op is one caller's write waiting for a round of the group commit
type op struct {
	run  func(tx *sql.Tx) (bool, error) // one caller's statement(s); a nil tx means autocommit
	done chan result                    // buffered 1
}

// result is what an op's caller receives: its own outcome, or with lead set,
// the turn to commit for the others
type result struct{ ok, lead bool }

// batcher groups concurrent writes on one file into shared transactions. The
// first caller with no round running leads: it commits what has queued up, at
// most its own round plus one more, then hands the lead to the oldest waiting
// caller, so no caller commits for others for long under steady load.
type batcher struct {
	mu      sync.Mutex
	pending []*op
	leading bool
	commit  func(ops []*op) // sends exactly one {ok} result to every op
}

// do runs one write in the next round and returns its own outcome
func (b *batcher) do(run func(*sql.Tx) (bool, error)) bool {
	o := &op{run: run, done: make(chan result, 1)}

	b.mu.Lock()
	b.pending = append(b.pending, o)
	if b.leading {
		b.mu.Unlock()
		r := <-o.done
		if !r.lead {
			return r.ok
		}
		// Handed the lead: o is still pending, first in the next round
		b.mu.Lock()
	} else {
		b.leading = true
	}
	b.lead()

	return (<-o.done).ok
}

// lead commits up to two rounds, then hands off to the oldest waiting op or
// ends the lead. b.mu is held on entry and released on return; it is never
// held across a commit.
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

	if len(b.pending) > 0 {
		b.pending[0].done <- result{lead: true}
	} else {
		b.leading = false
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
