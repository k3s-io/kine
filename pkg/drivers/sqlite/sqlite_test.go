package sqlite

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	connector, err := newConnector("sqlite3", dbPath+"?"+DefaultParams)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	return sql.OpenDB(connector)
}

// createBloatedDB creates a temporary SQLite database in WAL mode with the kine
// schema, inserts rowCount rows, then deletes them all. The deleted pages remain
// on SQLite's internal freelist so the file stays large on disk despite holding
// no data — simulating the post-compaction bloat that VACUUM is meant to fix.
// It returns the open *sql.DB.
func createBloatedDB(t *testing.T, rowCount int) *sql.DB {
	t.Helper()

	db := openTestDB(t)

	// Create the kine table (same schema as production).
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS kine
		(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT,
			created INTEGER,
			deleted INTEGER,
			create_revision INTEGER,
			prev_revision INTEGER,
			lease INTEGER,
			value BLOB,
			old_value BLOB
		)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert rows with non-trivial payload to grow the file.
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin insert tx: %v", err)
	}

	stmt, err := tx.Prepare(`INSERT INTO kine (name, created, deleted, create_revision, prev_revision, lease, value, old_value)
		VALUES (?, 1, 0, 1, 0, 0, randomblob(512), randomblob(512))`)
	if err != nil {
		t.Fatalf("failed to prepare insert: %v", err)
	}

	for i := range rowCount {
		if _, err := stmt.Exec(fmt.Sprintf("/registry/test/%d", i)); err != nil {
			t.Fatalf("failed to insert row %d: %v", i, err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit insert tx: %v", err)
	}

	stmt.Close()

	// Checkpoint WAL into main DB so the file size reflects all data.
	if _, err := db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("failed to checkpoint: %v", err)
	}

	// Delete all rows - pages go to the freelist, file does not shrink.
	if _, err := db.Exec(`DELETE FROM kine`); err != nil {
		t.Fatalf("failed to delete rows: %v", err)
	}

	// Checkpoint again so the deletes are in the main DB file.
	if _, err := db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("failed to checkpoint after delete: %v", err)
	}

	return db
}

// freelistCount returns the number of pages on SQLite's internal freelist.
func freelistCount(t *testing.T, db *sql.DB) int64 {
	t.Helper()

	var count int64
	if err := db.QueryRow(`SELECT freelist_count FROM pragma_freelist_count()`).Scan(&count); err != nil {
		t.Fatalf("failed to query freelist_count: %v", err)
	}

	return count
}

func TestSetupCreatesTextNameColumn(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	if err := setup(db, false, false, true); err != nil {
		t.Fatalf("setup() failed: %v", err)
	}

	rows, err := db.Query(`PRAGMA table_info(kine)`)
	if err != nil {
		t.Fatalf("failed to query table info: %v", err)
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
			t.Fatalf("failed to scan table info: %v", err)
		}
		if name == "name" {
			if !strings.EqualFold(typ, "TEXT") {
				t.Fatalf("expected kine.name to be TEXT, got %q", typ)
			}
			return
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("failed to read table info: %v", err)
	}

	t.Fatal("kine.name column not found")
}

func TestPrefixRange(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		low     string
		high    string
		ok      bool
	}{
		{
			name:    "resource prefix",
			pattern: "/registry/pods/%",
			low:     "/registry/pods/",
			high:    "/registry/pods0",
			ok:      true,
		},
		{
			name:    "escaped underscore",
			pattern: "/registry/foo^_/%",
			low:     "/registry/foo_/",
			high:    "/registry/foo_0",
			ok:      true,
		},
		{
			name:    "single key",
			pattern: "/registry/pods/default",
			ok:      false,
		},
		{
			name:    "unsupported internal wildcard",
			pattern: "/registry/%/pods/%",
			ok:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			low, high, ok := prefixRange(tt.pattern)
			if ok != tt.ok {
				t.Fatalf("expected ok=%v, got %v", tt.ok, ok)
			}
			if low != tt.low || high != tt.high {
				t.Fatalf("expected range [%q, %q), got [%q, %q)", tt.low, tt.high, low, high)
			}
		})
	}
}

func TestPrefixRangeQueryLimitsResults(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	if err := setup(db, false, false, true); err != nil {
		t.Fatalf("setup() failed: %v", err)
	}

	names := []string{
		"/registry/pods/template-0001",
		"/registry/pods/template-0002",
		"/registry/pods/template-0003",
		"/registry/pods0/out",
		"/registry/podr/out",
	}
	for i, name := range names {
		if _, err := db.Exec(`INSERT INTO kine (name, created, deleted, create_revision, prev_revision, lease) VALUES (?, 1, 0, ?, 0, 0)`, name, i+1); err != nil {
			t.Fatalf("failed to insert %q: %v", name, err)
		}
	}

	low, high, ok := prefixRange("/registry/pods/%")
	if !ok {
		t.Fatal("expected prefix range")
	}
	rows, err := db.Query(`SELECT name FROM kine WHERE name >= ? AND name < ? ORDER BY name ASC`, low, high)
	if err != nil {
		t.Fatalf("failed to query range: %v", err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("failed to scan name: %v", err)
		}
		got = append(got, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("failed to read names: %v", err)
	}

	want := []string{
		"/registry/pods/template-0001",
		"/registry/pods/template-0002",
		"/registry/pods/template-0003",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected names %v, got %v", want, got)
	}
}

func TestSetupVacuumReclaimsDiskSpace(t *testing.T) {
	const rowCount = 10000

	db := createBloatedDB(t, rowCount)
	defer db.Close()

	freelistBefore := freelistCount(t, db)
	if freelistBefore == 0 {
		t.Fatal("freelist is empty before setup - test setup is broken")
	}

	// Run setup with VACUUM enabled (noStartupVacuum=false).
	if err := setup(db, false, false, false); err != nil {
		t.Fatalf("setup() failed: %v", err)
	}

	freelistAfter := freelistCount(t, db)

	// After VACUUM the freelist should be empty — all unused pages are
	// reclaimed and the database file is rewritten without gaps.
	if freelistAfter != 0 {
		t.Errorf("VACUUM did not reclaim freelist pages: before=%d, after=%d (expected 0)", freelistBefore, freelistAfter)
	}

	t.Logf("VACUUM reclaimed freelist pages: %d -> %d", freelistBefore, freelistAfter)
}

func TestSetupVacuumDisabledPreservesFileSize(t *testing.T) {
	const rowCount = 10000

	db := createBloatedDB(t, rowCount)
	defer db.Close()

	freelistBefore := freelistCount(t, db)
	if freelistBefore == 0 {
		t.Fatal("freelist is empty before setup - test setup is broken")
	}

	// Run setup with VACUUM disabled (noStartupVacuum=true).
	if err := setup(db, false, false, true); err != nil {
		t.Fatalf("setup() failed: %v", err)
	}

	freelistAfter := freelistCount(t, db)

	// With VACUUM disabled, the freelist should remain substantially
	// unchanged. Schema/index creation is idempotent but may consume a
	// handful of freelist pages, so allow a small tolerance.
	lowerBound := freelistBefore * 9 / 10
	if freelistAfter < lowerBound {
		t.Errorf("freelist unexpectedly shrank without VACUUM: before=%d, after=%d (expected >= %d)", freelistBefore, freelistAfter, lowerBound)
	}

	t.Logf("No VACUUM: freelist pages before=%d, after=%d", freelistBefore, freelistAfter)
}
