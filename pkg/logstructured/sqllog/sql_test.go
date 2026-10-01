package sqllog_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/sqlite"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/server"
)

// newLog opens a fresh SQLite dialect on the given DSN and wraps it in a
// started SQLLog. A second call with the same DSN simulates another kine
// process sharing the same database: it gets its own in-memory revision
// cache, which will lag the head of the log once the other instance writes.
func newLog(t *testing.T, ctx context.Context, wg *sync.WaitGroup, dsn string) *sqllog.SQLLog {
	t.Helper()

	_, dialect, err := sqlite.NewVariant(ctx, wg, "sqlite3", &drivers.Config{DataSourceName: dsn})
	if err != nil {
		t.Fatalf("failed to open dialect: %v", err)
	}
	log := sqllog.New(dialect, 0, 0, time.Minute, 0, 100, 500)
	if err := log.Start(ctx); err != nil {
		t.Fatalf("failed to start log: %v", err)
	}
	return log
}

// TestStaleCurrentRevision verifies that requests carrying a revision that is
// at or below the actual head of the log do not fail with ErrFutureRev when
// the local revision cache is stale - as happens when the database is shared
// with another kine instance, or the poll loop is delayed.
func TestStaleCurrentRevision(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	dsn := filepath.Join(t.TempDir(), "state.db") + "?" + sqlite.DefaultParams

	// logA caches the current revision at startup.
	logA := newLog(t, ctx, &wg, dsn)
	cachedRev, err := logA.CurrentRevision(ctx)
	if err != nil {
		t.Fatalf("CurrentRevision failed: %v", err)
	}

	// logB is a second instance on the same database. Its append advances the
	// head of the log past logA's cached revision.
	logB := newLog(t, ctx, &wg, dsn)
	headRev, err := logB.Append(ctx, &server.Event{
		KV: &server.KeyValue{Key: "/registry/test", Value: []byte("test")},
	})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if headRev <= cachedRev {
		t.Fatalf("test setup failed: head revision %d not ahead of cached revision %d", headRev, cachedRev)
	}

	// A List at the head revision for a key that does not exist must refresh
	// the stale cached revision instead of failing with ErrFutureRev.
	batch, err := logA.List(ctx, "/registry/missing", "", 0, headRev, false, false)
	if err != nil {
		t.Fatalf("List at head revision %d failed: %v", headRev, err)
	}
	if batch.CurrentRev != headRev {
		t.Fatalf("List returned stale current revision %d, want %d", batch.CurrentRev, headRev)
	}

	// ListStream must behave the same way.
	res := logA.ListStream(ctx, "/registry/missing", "", 0, headRev, false, false)
	if err, ok := <-res.Errorc; ok && err != nil {
		t.Fatalf("ListStream at head revision %d failed: %v", headRev, err)
	}
	if res.CurrentRevision != headRev {
		t.Fatalf("ListStream returned stale current revision %d, want %d", res.CurrentRevision, headRev)
	}
	if kv, ok := <-res.KVc; ok {
		t.Fatalf("ListStream unexpectedly returned a key: %v", kv)
	}

	// After has no ErrFutureRev check, but it must not report the stale
	// cached revision either.
	batch, err = logA.After(ctx, "", "", headRev, 0)
	if err != nil {
		t.Fatalf("After failed: %v", err)
	}
	if batch.CurrentRev != headRev {
		t.Fatalf("After returned stale current revision %d, want %d", batch.CurrentRev, headRev)
	}

	// The refresh must have advanced the cached revision.
	if rev, err := logA.CurrentRevision(ctx); err != nil || rev != headRev {
		t.Fatalf("cached revision not refreshed: got %d, want %d (err=%v)", rev, headRev, err)
	}

	// Manual compaction at the head revision must not fail with ErrFutureRev
	// due to a stale cached revision either.
	if rev, err := logA.Compact(ctx, headRev); err != nil {
		t.Fatalf("Compact at head revision %d failed: %v", headRev, err)
	} else if rev != headRev {
		t.Fatalf("Compact returned %d, want %d", rev, headRev)
	}
}

// TestFutureRevision verifies that revisions past the actual head of the log
// still fail with ErrFutureRev once the cached revision has been refreshed
// from the database.
func TestFutureRevision(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	dsn := filepath.Join(t.TempDir(), "state.db") + "?" + sqlite.DefaultParams
	log := newLog(t, ctx, &wg, dsn)

	headRev, err := log.Append(ctx, &server.Event{
		KV: &server.KeyValue{Key: "/registry/test", Value: []byte("test")},
	})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	futureRev := headRev + 1

	if _, err := log.List(ctx, "/registry/missing", "", 0, futureRev, false, false); !errors.Is(err, server.ErrFutureRev) {
		t.Fatalf("List at future revision %d: got %v, want ErrFutureRev", futureRev, err)
	}
	if _, _, err := log.Count(ctx, "/registry/missing", "", futureRev); !errors.Is(err, server.ErrFutureRev) {
		t.Fatalf("Count at future revision %d: got %v, want ErrFutureRev", futureRev, err)
	}
	if res := log.ListStream(ctx, "/registry/missing", "", 0, futureRev, false, false); !errors.Is(<-res.Errorc, server.ErrFutureRev) {
		t.Fatalf("ListStream at future revision %d: want ErrFutureRev", futureRev)
	}
	if _, err := log.Compact(ctx, futureRev); !errors.Is(err, server.ErrFutureRev) {
		t.Fatalf("Compact at future revision %d: got %v, want ErrFutureRev", futureRev, err)
	}
}
