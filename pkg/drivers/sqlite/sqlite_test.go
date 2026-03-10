//go:build cgo

package sqlite

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// createBloatedDB creates a temporary SQLite database in WAL mode with the kine
// schema, inserts rowCount rows, then deletes them all. The deleted pages remain
// on SQLite's internal freelist so the file stays large on disk despite holding
// no data — simulating the post-compaction bloat that VACUUM is meant to fix.
// It returns the open *sql.DB and the path to the database file.
func createBloatedDB(t *testing.T, rowCount int) (*sql.DB, string) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite3", dbPath+"?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// Create the kine table (same schema as production).
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS kine
		(
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name INTEGER,
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
		if _, err := stmt.Exec(i); err != nil {
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

	return db, dbPath
}

// fileSize returns the size of the file at path.
func fileSize(t *testing.T, path string) int64 {
	t.Helper()

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("failed to stat %s: %v", path, err)
	}

	return fi.Size()
}

func TestSetupVacuumReclaimsDiskSpace(t *testing.T) {
	const rowCount = 10000

	db, dbPath := createBloatedDB(t, rowCount)
	defer db.Close()

	sizeBefore := fileSize(t, dbPath)
	if sizeBefore == 0 {
		t.Fatal("database file is empty before setup - test setup is broken")
	}

	// Run setup with VACUUM enabled (noStartupVacuum=false).
	if err := setup(db, false, false, false); err != nil {
		t.Fatalf("setup() failed: %v", err)
	}

	// VACUUM rewrites the database file, but the OS may report the old size
	// while connections using cache=shared still hold the original file
	// descriptor. Close the pool so the old fd is released and the filesystem
	// reflects the new (smaller) file.
	db.Close()

	sizeAfter := fileSize(t, dbPath)

	// After VACUUM the file should be smaller, all rows have been deleted
	// so the file should shrink by at least 50%.
	threshold := sizeBefore / 2
	if sizeAfter >= threshold {
		t.Errorf("VACUUM did not reclaim disk space: size before=%d, after=%d (expected < %d)", sizeBefore, sizeAfter, threshold)
	}

	t.Logf("VACUUM reclaimed space: %d -> %d bytes (%.1f%% reduction)", sizeBefore, sizeAfter, 100*(1-float64(sizeAfter)/float64(sizeBefore)))
}

func TestSetupVacuumDisabledPreservesFileSize(t *testing.T) {
	const rowCount = 10000

	db, dbPath := createBloatedDB(t, rowCount)
	defer db.Close()

	sizeBefore := fileSize(t, dbPath)
	if sizeBefore == 0 {
		t.Fatal("database file is empty before setup - test setup is broken")
	}

	// Run setup with VACUUM disabled (noStartupVacuum=true).
	if err := setup(db, false, false, true); err != nil {
		t.Fatalf("setup() failed: %v", err)
	}

	// Close the pool before measuring, same as the VACUUM-enabled test, so
	// that the OS reports the true file size without stale fd interference.
	db.Close()

	sizeAfter := fileSize(t, dbPath)

	// With VACUUM disabled, the file size should remain the same (or very
	// close - schema/index creation is idempotent and adds negligible data).
	// Allow a small tolerance for WAL checkpoint overhead.
	lowerBound := sizeBefore * 9 / 10
	if sizeAfter < lowerBound {
		t.Errorf("file unexpectedly shrank without VACUUM: size before=%d, after=%d", sizeBefore, sizeAfter)
	}

	t.Logf("No VACUUM: file size before=%d, after=%d bytes", sizeBefore, sizeAfter)
}
