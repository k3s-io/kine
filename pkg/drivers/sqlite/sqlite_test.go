package sqlite

import (
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
)

// createBloatedDB creates a temporary SQLite database in WAL mode with the kine
// schema, inserts rowCount rows, then deletes them all. The deleted pages remain
// on SQLite's internal freelist so the file stays large on disk despite holding
// no data — simulating the post-compaction bloat that VACUUM is meant to fix.
// It returns the open *sql.DB.
func createBloatedDB(t *testing.T, rowCount int) *sql.DB {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	connector, err := newConnector("sqlite3", dbPath+"?"+DefaultParams)
	if err != nil {
		t.Fatalf("Failed to create connector: %v", err)
	}

	db := sql.OpenDB(connector)

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

// A deleted key has no mod revision, so a compare against the revision of
// the delete must fail, as it does in etcd.
func TestTxnAtDeleteRevision(t *testing.T) {
	ctx := t.Context()
	wg := &sync.WaitGroup{}
	t.Cleanup(wg.Wait)
	b, _, err := NewVariant(ctx, wg, "sqlite3", &drivers.Config{
		DataSourceName:   filepath.Join(t.TempDir(), "state.db") + "?" + DefaultParams,
		PollInterval:     time.Second,
		CompactMinRetain: 1000,
		CompactBatchSize: 1000,
		PollBatchSize:    500,
	})
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatalf("Failed to start backend: %v", err)
	}

	for _, op := range []string{"update", "delete"} {
		key := "/test/" + op
		rev, err := b.Create(ctx, key, []byte("v1"), 0)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		deleteRev, _, ok, err := b.Delete(ctx, key, rev)
		if err != nil || !ok {
			t.Fatalf("Delete failed: ok=%v, err=%v", ok, err)
		}

		if op == "update" {
			_, _, ok, err = b.Update(ctx, key, []byte("v2"), deleteRev, 0)
		} else {
			_, _, ok, err = b.Delete(ctx, key, deleteRev)
		}
		if err != nil || ok {
			t.Errorf("%s at delete revision: expected ok=false, got ok=%v, err=%v", op, ok, err)
		}
		if _, kv, err := b.Get(ctx, key, 0, false); err != nil || kv != nil {
			t.Errorf("%s at delete revision: expected no key, got kv=%+v, err=%v", op, kv, err)
		}
		if rev, err := b.CurrentRevision(ctx); err != nil || rev != deleteRev {
			t.Errorf("%s at delete revision: expected current revision %d, got %d, err=%v", op, deleteRev, rev, err)
		}
	}
}
