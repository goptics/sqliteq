package sqliteq

import (
	"cmp"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// remainingCap bounds the pending count a batch pop reports
const remainingCap = 256

// Queue implements the Queue interface using SQLite as the storage backend
type Queue struct {
	f                *file
	name, table      string // table is quoteIdent(name)
	priority         bool
	order            string // "id" or "priority, id"
	removeOnComplete bool
	closed           atomic.Bool

	// Prepared once per handle; they close with the file's pool
	insert, claim, pop, ack, count, countCapped, values *sql.Stmt
}

// claimed is one row a claim moved to processing
type claimed struct {
	id       int64
	priority int
	data     []byte
}

// newQueue creates a SQLite-based queue on its table: the table itself, the
// index migration and the prepared statements. Recovery runs from file.open,
// once per table per file.
func newQueue(f *file, name string, priority bool, opts ...Option) (*Queue, error) {
	q := &Queue{
		f:                f,
		name:             name,
		table:            quoteIdent(name),
		priority:         priority,
		order:            "id",
		removeOnComplete: true, // Default to removing completed items
	}
	if priority {
		q.order = "priority, id"
	}

	// Apply any provided options
	for _, opt := range opts {
		opt(q)
	}

	// The db is shared by every queue of the file, so it is not closed on errors
	if err := q.initSchema(); err != nil {
		return nil, err
	}
	if err := q.prepare(); err != nil {
		return nil, fmt.Errorf("failed to prepare statements: %w", err)
	}

	return q, nil
}

// initSchema creates or migrates the table and its index under the write mutex
func (q *Queue) initSchema() error {
	q.f.mu.Lock()
	defer q.f.mu.Unlock()

	if err := q.initTable(); err != nil {
		return fmt.Errorf("failed to initialize table: %w", err)
	}
	if err := q.migrate(); err != nil {
		return fmt.Errorf("failed to migrate indexes: %w", err)
	}
	return nil
}

// initTable creates the table as v0.2 defines it, so old tables need no
// rebuild, and adds the priority column to a priority queue's table when
// it is missing
func (q *Queue) initTable() error {
	_, err := q.f.db.Exec(fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		data BLOB NOT NULL,
		status TEXT NOT NULL,
		ack_id TEXT UNIQUE,
		ack BOOLEAN DEFAULT 0,
		created_at TIMESTAMP,
		updated_at TIMESTAMP
	)`, q.table))
	if err != nil || !q.priority {
		return err
	}

	exists, err := hasColumn(q.f.db, q.name, "priority")
	if err != nil || exists {
		return err
	}
	// Add priority column with default value 0
	_, err = q.f.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN priority INTEGER NOT NULL DEFAULT 0", q.table))
	return err
}

// pendingIndexSuffix names a queue's partial pending index. Tables and
// indexes share one namespace, so the suffix is one no queue name would take.
const pendingIndexSuffix = "__sqliteq_pending"

// migrate drops the v0.2 indexes and makes sure the one partial index on
// pending rows matches the queue order. On a migrated table it changes nothing.
func (q *Queue) migrate() error {
	for _, suffix := range []string{"_status_idx", "_status_ack_idx", "_ack_id_idx", "_priority_idx"} {
		if _, err := q.f.db.Exec("DROP INDEX IF EXISTS " + quoteIdent(q.name+suffix)); err != nil {
			return err
		}
	}

	index := q.name + pendingIndexSuffix
	cols, err := indexColumns(q.f.db, index)
	if err != nil {
		return err
	}
	if slices.Equal(cols, strings.Split(q.order, ", ")) {
		return nil
	}

	// The table was last used as the other kind of queue
	if len(cols) > 0 {
		if _, err := q.f.db.Exec("DROP INDEX " + quoteIdent(index)); err != nil {
			return err
		}
	}
	_, err = q.f.db.Exec(fmt.Sprintf("CREATE INDEX %s ON %s (%s) WHERE status = 'pending'", quoteIdent(index), q.table, q.order))
	return err
}

// indexColumns lists an index's columns in order; none if it does not exist
func indexColumns(db *sql.DB, index string) ([]string, error) {
	rows, err := db.Query("SELECT name FROM pragma_index_info(?) ORDER BY seqno", index)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// prepare prepares every statement of the queue once. Each read keeps
// status = 'pending' literal, or SQLite cannot use the partial index.
func (q *Queue) prepare() (err error) {
	defer func() {
		if err != nil {
			q.closeStmts()
		}
	}()

	cols, vals, returning := "data, status, ack, created_at, updated_at", "?, 'pending', 0, ?, ?", "id, data"
	if q.priority {
		cols, vals, returning = cols+", priority", vals+", ?", "id, priority, data"
	}
	pending := fmt.Sprintf("SELECT id FROM %s WHERE status = 'pending' ORDER BY %s", q.table, q.order)

	ack := fmt.Sprintf("DELETE FROM %s WHERE id = ? AND status = 'processing'", q.table)
	if !q.removeOnComplete {
		ack = fmt.Sprintf("UPDATE %s SET status = 'completed', ack = 1, updated_at = ? WHERE id = ? AND status = 'processing'", q.table)
	}

	for _, s := range []struct {
		stmt  **sql.Stmt
		query string
	}{
		{&q.insert, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", q.table, cols, vals)},
		{&q.claim, fmt.Sprintf("UPDATE %s SET status = 'processing', updated_at = ? WHERE id IN (%s LIMIT ?) RETURNING %s", q.table, pending, returning)},
		{&q.pop, fmt.Sprintf("DELETE FROM %s WHERE id IN (%s LIMIT 1) RETURNING data", q.table, pending)},
		{&q.ack, ack},
		{&q.count, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = 'pending'", q.table)},
		{&q.countCapped, fmt.Sprintf("SELECT COUNT(*) FROM (SELECT 1 FROM %s WHERE status = 'pending' LIMIT %d)", q.table, remainingCap)},
		{&q.values, fmt.Sprintf("SELECT data FROM %s WHERE status = 'pending' ORDER BY %s", q.table, q.order)},
	} {
		if *s.stmt, err = q.f.db.Prepare(s.query); err != nil {
			return err
		}
	}

	return nil
}

// closeStmts closes the statements prepared so far
func (q *Queue) closeStmts() {
	for _, s := range []*sql.Stmt{q.insert, q.claim, q.pop, q.ack, q.count, q.countCapped, q.values} {
		if s != nil {
			s.Close()
		}
	}
}

// requeueNoAck puts rows left in processing without an ack back to pending
func (q *Queue) requeueNoAck() error {
	q.f.mu.Lock()
	defer q.f.mu.Unlock()

	_, err := q.f.db.Exec(
		fmt.Sprintf("UPDATE %s SET status = 'pending', updated_at = ? WHERE status = 'processing' AND ack = 0", q.table),
		time.Now().UTC(),
	)
	return err
}

// hasColumn reports whether the table has a column with the given name
func hasColumn(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(table)))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid     int
			name    string
			typ     string
			notNull int
			dflt    sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}

	return false, rows.Err()
}

// parseAckID turns an ack id back into its row id. Only the canonical decimal
// of a positive id is accepted, so a v0.2 cuid or any other spelling acks nothing.
func parseAckID(s string) (int64, bool) {
	if s == "" || s[0] < '1' || s[0] > '9' {
		return 0, false
	}
	id, err := strconv.ParseInt(s, 10, 64)
	return id, err == nil && id > 0
}

// Enqueue adds an item to the queue
// Returns true if the operation was successful
func (q *Queue) Enqueue(item any) bool {
	return q.enqueue(item, 0, "")
}

// enqueue inserts one pending row in the file's next group commit round;
// priority is stored on priority queues only. id is unused for now.
func (q *Queue) enqueue(item any, priority int, id string) bool {
	if q.closed.Load() {
		return false
	}

	// Convert here, as database/sql would inside Exec, so the item's own code
	// (a driver.Valuer) runs in its caller and never in a round's leader
	data, err := driver.DefaultParameterConverter.ConvertValue(item)
	if err != nil {
		return false
	}

	now := time.Now().UTC()
	return q.f.b.do(func(tx *sql.Tx) (bool, error) {
		insert := q.insert
		if tx != nil {
			insert = tx.Stmt(insert)
		}
		var err error
		if q.priority {
			_, err = insert.Exec(data, now, now, priority)
		} else {
			_, err = insert.Exec(data, now, now)
		}
		return err == nil, err
	})
}

// claimN moves up to n pending rows to processing in one statement and
// returns them in queue order
func (q *Queue) claimN(n int) ([]claimed, error) {
	q.f.mu.Lock()
	out, err := q.claimRows(n)
	q.f.mu.Unlock()
	if err != nil {
		return nil, err
	}

	// RETURNING order is unspecified
	slices.SortFunc(out, func(a, b claimed) int {
		if c := cmp.Compare(a.priority, b.priority); c != 0 {
			return c
		}
		return cmp.Compare(a.id, b.id)
	})
	return out, nil
}

// claimRows runs the claim statement and reads every row it returns; the
// caller holds the write mutex
func (q *Queue) claimRows(n int) ([]claimed, error) {
	rows, err := q.claim.Query(time.Now().UTC(), n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]claimed, 0, n)
	for rows.Next() {
		var c claimed
		if q.priority {
			err = rows.Scan(&c.id, &c.priority, &c.data)
		} else {
			err = rows.Scan(&c.id, &c.data)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	// Reading to the end runs the statement to completion, which commits it
	return out, rows.Err()
}

// Dequeue removes and returns the next item from the queue
// Returns the item and a boolean indicating if the operation was successful
func (q *Queue) Dequeue() (any, bool) {
	if q.closed.Load() {
		return nil, false
	}

	q.f.mu.Lock()
	defer q.f.mu.Unlock()

	// Deferred last, so the rows close (and the statement ends) before unlocking
	rows, err := q.pop.Query()
	if err != nil {
		return nil, false
	}
	defer rows.Close()

	var data []byte
	found := false
	for rows.Next() {
		if err := rows.Scan(&data); err != nil {
			return nil, false
		}
		found = true
	}
	if rows.Err() != nil || !found {
		return nil, false
	}
	return data, true
}

// DequeueWithAckId moves the next item to processing and returns it with its
// acknowledgment ID, the row id in decimal
func (q *Queue) DequeueWithAckId() (any, bool, string) {
	if q.closed.Load() {
		return nil, false, ""
	}

	c, err := q.claimN(1)
	if err != nil || len(c) == 0 {
		return nil, false, ""
	}
	return c[0].data, true, strconv.FormatInt(c[0].id, 10)
}

// DequeueBatchWithAckIds implements varmq.IBatchAcknowledgeable: it moves up
// to min(len(items), len(ackIDs)) items to processing in one statement, in
// queue order, each under its row id as ack id. remaining is the pending
// count after the move, capped at remainingCap. n is 0 with a nil error when
// the queue is empty or closed.
func (q *Queue) DequeueBatchWithAckIds(items []any, ackIDs []string) (n, remaining int, err error) {
	want := min(len(items), len(ackIDs))
	if q.closed.Load() || want == 0 {
		return 0, 0, nil
	}

	c, err := q.claimN(want)
	if err != nil {
		return 0, 0, err
	}
	if len(c) == 0 {
		return 0, 0, nil
	}
	for i, row := range c {
		items[i], ackIDs[i] = row.data, strconv.FormatInt(row.id, 10)
	}

	// The rows are claimed already: an error here would make varmq drop them,
	// so a failed count reports items left and the next pop finds out
	if err := q.countCapped.QueryRow().Scan(&remaining); err != nil {
		return len(c), 1, nil
	}
	return len(c), remaining, nil
}

// Acknowledge marks a processing item as completed, deleting it unless
// WithRemoveOnComplete(false) is set, in the file's next group commit round
// Returns true only if exactly one processing row had this ack ID
func (q *Queue) Acknowledge(ackID string) bool {
	id, ok := parseAckID(ackID)
	if !ok {
		return false
	}

	now := time.Now().UTC()
	return q.f.b.do(func(tx *sql.Tx) (bool, error) {
		var (
			result sql.Result
			err    error
		)
		ack := q.ack
		if tx != nil {
			ack = tx.Stmt(ack)
		}
		if q.removeOnComplete {
			result, err = ack.Exec(id)
		} else {
			result, err = ack.Exec(now, id)
		}
		if err != nil {
			return false, err
		}

		// No row is an outcome, not an error: it must not fail the round
		n, err := result.RowsAffected()
		return err == nil && n == 1, err
	})
}

// Len returns the number of pending items in the queue
func (q *Queue) Len() int {
	var count int
	if err := q.count.QueryRow().Scan(&count); err != nil {
		return 0
	}
	return count
}

// Values returns all pending items in the queue, in queue order
func (q *Queue) Values() []any {
	rows, err := q.values.Query()
	if err != nil {
		return nil
	}
	defer rows.Close()

	var items []any
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			continue
		}

		// Now we just add the byte array directly as we're storing byte arrays
		// instead of JSON-serialized data
		items = append(items, data)
	}

	return items
}

// Purge removes all items from the queue
func (q *Queue) Purge() {
	q.f.mu.Lock()
	defer q.f.mu.Unlock()

	q.f.db.Exec(fmt.Sprintf("DELETE FROM %s", q.table))
}

// Close closes the queue and its database connection
func (q *Queue) Close() error {
	q.closed.Store(true)

	return nil
}
