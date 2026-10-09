package sqlite

import (
	"context"
	"database/sql"
	"os"
	"testing"
)

// Opens the special SQLite database names through the connector and checks
// that they behave the same way, no matter which SQLite driver is used: They
// never create files in the working directory, and whether or not pooled
// connections share the same database.
func TestSpecialDatabaseNames(t *testing.T) {
	for _, test := range []struct {
		dsn    string
		shared bool
	}{
		{dsn: ":memory:"},
		{dsn: "file::memory:?cache=shared", shared: true},
		{dsn: ""},
	} {
		t.Run(test.dsn, func(t *testing.T) {
			t.Chdir(t.TempDir())

			connector, err := newConnector("sqlite3", test.dsn)
			if err != nil {
				t.Fatalf("newConnector(%q) failed: %v", test.dsn, err)
			}
			db := sql.OpenDB(connector)
			defer func() {
				if err := db.Close(); err != nil {
					t.Errorf("db.Close() failed: %v", err)
				}
			}()

			ctx := context.Background()
			first, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("failed to open first connection: %v", err)
			}
			defer func() {
				if err := first.Close(); err != nil {
					t.Errorf("first.Close() failed: %v", err)
				}
			}()

			if _, err := first.ExecContext(ctx, "CREATE TABLE t (id INTEGER)"); err != nil {
				t.Fatalf("failed to create table: %v", err)
			}

			second, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("failed to open second connection: %v", err)
			}
			defer func() {
				if err := second.Close(); err != nil {
					t.Errorf("second.Close() failed: %v", err)
				}
			}()
			_, err = second.ExecContext(ctx, "INSERT INTO t VALUES (1)")
			if test.shared && err != nil {
				t.Errorf("table not visible from second connection: %v", err)
			} else if !test.shared && err == nil {
				t.Error("table unexpectedly visible from second connection")
			}

			if entries, err := os.ReadDir("."); err != nil {
				t.Errorf("failed to read working directory: %v", err)
			} else if len(entries) != 0 {
				t.Errorf("files created in working directory: %v", entries)
			}
		})
	}
}
