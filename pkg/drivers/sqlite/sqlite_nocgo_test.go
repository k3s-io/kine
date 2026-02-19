//go:build !cgo

package sqlite

import (
	"testing"
)

func Test_getDataSourceName(t *testing.T) {
	dsn, _, _ := getDataSourceName("./db/state.db?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate")
	expEqual(t, "./db/state.db?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&cache=shared&_pragma=busy_timeout(30000)&_txlock=immediate", dsn)
	dsn, _, _ = getDataSourceName("?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate")
	expEqual(t, "./db/state.db?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&cache=shared&_pragma=busy_timeout(30000)&_txlock=immediate", dsn)
	dsn, _, _ = getDataSourceName("?")
	expEqual(t, "./db/state.db?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&cache=shared&_pragma=busy_timeout(30000)&_txlock=immediate", dsn)
	dsn, _, _ = getDataSourceName("./db/state.db?")
	expEqual(t, "./db/state.db?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&cache=shared&_pragma=busy_timeout(30000)&_txlock=immediate", dsn)
	dsn, _, _ = getDataSourceName("./db/state.db")
	expEqual(t, "./db/state.db?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&cache=shared&_pragma=busy_timeout(30000)&_txlock=immediate", dsn)
	dsn, _, _ = getDataSourceName("")
	expEqual(t, "./db/state.db?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&cache=shared&_pragma=busy_timeout(30000)&_txlock=immediate", dsn)
}
