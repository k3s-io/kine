package sqllog_test

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/sqlite"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/server"
)

func TestCompactionUsesInitialRevisionAcrossBatches(t *testing.T) {
	ctx := t.Context()
	cfg := &drivers.Config{
		DataSourceName:   filepath.Join(t.TempDir(), "kine.db"),
		CompactBatchSize: 100,
		CompactMinRetain: 50,
		CompactTimeout:   time.Second,
	}
	var wg sync.WaitGroup
	t.Cleanup(wg.Wait)
	_, d, err := sqlite.NewVariant(ctx, &wg, "sqlite3", cfg)
	if err != nil {
		t.Fatal(err)
	}
	log := sqllog.New(d, &wg, cfg)
	if err := log.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// Advance the head from 350 to 450 with the first batch. The remaining
	// batches must still leave 50 revisions behind the original head.
	_, err = d.DB.ExecContext(ctx, `
		INSERT INTO kine (id, name) VALUES (350, 'first');
		CREATE TRIGGER append_during_compaction AFTER UPDATE ON kine
		WHEN NEW.name = 'compact_rev_key' AND NEW.prev_revision = 100
		BEGIN INSERT INTO kine (id, name) VALUES (450, 'second'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Compact(ctx, 350); err != nil {
		t.Fatal(err)
	}
	if marker, err := log.CompactRevision(ctx); err != nil || marker != 300 {
		t.Fatalf("compact revision = %d, %v; want 300", marker, err)
	}

	// Manual compaction handles batch failures internally.
	_, err = d.DB.ExecContext(ctx, `
		INSERT INTO kine (id, name) VALUES (650, 'third');
		CREATE TRIGGER fail_next_batch BEFORE UPDATE ON kine WHEN NEW.prev_revision = 500
		BEGIN SELECT RAISE(ABORT, 'batch failed'); END;`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Compact(ctx, 650); err != nil {
		t.Fatal(err)
	}
	if marker, err := log.CompactRevision(ctx); err != nil || marker != 400 {
		t.Fatalf("compact revision after failure = %d, %v; want 400", marker, err)
	}
	if rev, err := log.Compact(ctx, 400); rev != 0 || err != server.ErrCompacted {
		t.Fatalf("already compacted request returned %d, %v; want 0, ErrCompacted", rev, err)
	}
	if rev, err := log.Compact(ctx, 651); rev != 0 || err != server.ErrFutureRev {
		t.Fatalf("future request returned %d, %v; want 0, ErrFutureRev", rev, err)
	}
}
