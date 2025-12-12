package sqlite

import (
	"testing"
)

func expEqual[T comparable](t *testing.T, want, got T) {
	t.Helper()
	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}
func Test_getDataSourceName(t *testing.T) {
	dsn, _ := getDataSourceName("")
	expEqual(t, "./db/state.db?_pragma=busy_timeout%2830000%29&_pragma=journal_mode%28wal%29&_pragma=synchronous%28normal%29&_txlock=immediate&cache=shared", dsn)
	dsn, _ = getDataSourceName("?_busy_timeout=5000")
	expEqual(t, "./db/state.db?_pragma=busy_timeout%285000%29&_pragma=journal_mode%28wal%29&_pragma=synchronous%28normal%29&_txlock=immediate&cache=shared", dsn)
	dsn, _ = getDataSourceName("file:test.db")
	expEqual(t, "file:test.db?_pragma=busy_timeout%2830000%29&_pragma=journal_mode%28wal%29&_pragma=synchronous%28normal%29&_txlock=immediate&cache=shared", dsn)
}
