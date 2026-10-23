package sqliteq

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/mattn/go-sqlite3"
)

var (
	driversMu sync.Mutex
	drivers   = map[int]string{}
)

// driverName returns the name of a registered mattn driver whose ConnectHook
// runs "PRAGMA wal_autocheckpoint=<pages>" on every new connection. The
// driver is registered once per pages value, because sql.Register panics on
// a duplicate name.
func driverName(pages int) string {
	driversMu.Lock()
	defer driversMu.Unlock()

	if name, ok := drivers[pages]; ok {
		return name
	}

	name := fmt.Sprintf("sqliteq_ckpt_%d", pages)
	pragma := fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", pages)
	sql.Register(name, &sqlite3.SQLiteDriver{
		ConnectHook: func(c *sqlite3.SQLiteConn) error {
			_, err := c.Exec(pragma, nil)
			return err
		},
	})
	drivers[pages] = name
	return name
}

// dsn returns the connection string for a database file: WAL, relaxed
// fsync (safe under WAL) and a busy timeout so writers wait for each other.
func dsn(path string) string {
	return path + "?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000"
}
