package pgsql

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/sqlite"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
)

func TestCompactionUsesOneCommittedHead(t *testing.T) {
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
	// The marker is newer than the last event. Only the event at 350 can
	// establish the retention bound, so three batches may compact through 300.
	if _, err := d.DB.ExecContext(ctx, "INSERT INTO kine (id, name, created, deleted, prev_revision) VALUES (350, 'key', 1, 0, 0), (900, 'compact_rev_key', 1, 0, 0)"); err != nil {
		t.Fatal(err)
	}
	log := sqllog.New(d, &wg, cfg)
	reads := 0
	log.ReadCompactionHead = func(ctx context.Context) (int64, error) {
		reads++
		var head int64
		err := d.DB.QueryRowContext(ctx, compactionHeadSQL).Scan(&head)
		return head, err
	}
	if err := log.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Compact(ctx, 900); err != nil {
		t.Fatal(err)
	}
	marker, err := log.CompactRevision(ctx)
	if err != nil || marker != 300 || reads != 1 {
		t.Fatalf("compact marker %d, head reads %d, error %v", marker, reads, err)
	}
}
