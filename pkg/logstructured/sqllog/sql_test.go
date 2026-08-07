package sqllog

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

// errRowsDriver returns result sets that yield a configurable number of good
// rows and then fail from driver.Rows.Next, the way a killed session /
// cancelled statement / dying connection surfaces mid-resultset. The DSN
// encodes "<goodRows>|<error message>"; an empty message means a normal EOF.
type errRowsDriver struct{}

func (d *errRowsDriver) Open(name string) (driver.Conn, error) {
	parts := strings.SplitN(name, "|", 2)
	goodRows, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, err
	}
	conn := &errRowsConn{goodRows: goodRows}
	if len(parts) == 2 && parts[1] != "" {
		conn.err = errors.New(parts[1])
	}
	return conn, nil
}

type errRowsConn struct {
	goodRows int
	err      error
}

func (c *errRowsConn) Prepare(query string) (driver.Stmt, error) { return &errRowsStmt{c: c}, nil }
func (c *errRowsConn) Close() error                              { return nil }
func (c *errRowsConn) Begin() (driver.Tx, error)                 { return nil, driver.ErrSkip }

type errRowsStmt struct{ c *errRowsConn }

func (s *errRowsStmt) Close() error  { return nil }
func (s *errRowsStmt) NumInput() int { return -1 }
func (s *errRowsStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, driver.ErrSkip
}
func (s *errRowsStmt) Query(args []driver.Value) (driver.Rows, error) {
	return &errRows{remaining: s.c.goodRows, err: s.c.err}, nil
}

type errRows struct {
	remaining int
	err       error
	emitted   int64
}

func (r *errRows) Columns() []string {
	return []string{"id", "compact_revision", "theid", "name", "created", "deleted", "create_revision", "prev_revision", "lease", "value", "old_value"}
}
func (r *errRows) Close() error { return nil }
func (r *errRows) Next(dest []driver.Value) error {
	if r.remaining == 0 {
		if r.err != nil {
			return r.err
		}
		return io.EOF
	}
	r.remaining--
	r.emitted++
	dest[0] = int64(10)          // current revision
	dest[1] = int64(1)           // compact revision
	dest[2] = r.emitted          // theid
	dest[3] = []byte("/key")     // name
	dest[4] = int64(1)           // created
	dest[5] = int64(0)           // deleted
	dest[6] = int64(0)           // create_revision
	dest[7] = int64(0)           // prev_revision
	dest[8] = int64(0)           // lease
	dest[9] = []byte("value")    // value
	dest[10] = []byte(nil)       // old_value
	return nil
}

var errRowsDriverRegistered atomic.Bool

func queryErrRows(t *testing.T, goodRows int, errMsg string) *sql.Rows {
	t.Helper()
	if errRowsDriverRegistered.CompareAndSwap(false, true) {
		sql.Register("err-rows", &errRowsDriver{})
	}
	db, err := sql.Open("err-rows", fmt.Sprintf("%d|%s", goodRows, errMsg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close db: %v", err)
		}
	})
	rows, err := db.Query("SELECT irrelevant")
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestRowsToEventsCompleteResultSet(t *testing.T) {
	_, _, events, err := RowsToEvents(queryErrRows(t, 2, ""), true, true)
	if err != nil {
		t.Fatalf("expected no error for a complete result set, got %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
}

// A query that dies before yielding its first row must surface the error:
// treating it as an empty result makes callers conclude the key does not
// exist, so e.g. logstructured.Delete reports success without writing a
// tombstone (a silent lost delete).
func TestRowsToEventsSurfacesErrorBeforeFirstRow(t *testing.T) {
	_, _, events, err := RowsToEvents(queryErrRows(t, 0, "session is in the kill state"), true, true)
	if err == nil {
		t.Fatalf("expected the mid-flight query error, got nil error and %d events", len(events))
	}
	if !strings.Contains(err.Error(), "kill state") {
		t.Fatalf("expected the driver error, got %v", err)
	}
}

// A query interrupted after some rows must not be mistaken for a complete,
// shorter result set.
func TestRowsToEventsSurfacesMidStreamError(t *testing.T) {
	_, _, events, err := RowsToEvents(queryErrRows(t, 1, "connection reset"), true, true)
	if err == nil {
		t.Fatalf("expected the mid-flight query error, got nil error and %d events", len(events))
	}
	if !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("expected the driver error, got %v", err)
	}
}
