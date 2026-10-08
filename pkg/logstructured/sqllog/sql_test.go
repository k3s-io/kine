package sqllog_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/sqlite"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/server"
)

// newLog wraps a SQLite dialect in a started SQLLog. Two instances on the
// same DSN simulate two kine processes sharing one database, each with its
// own in-memory revision cache.
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

// TestStaleCurrentRevision verifies that a request at or below the actual
// head does not fail with ErrFutureRev when the local revision cache lags.
func TestStaleCurrentRevision(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	dsn := filepath.Join(t.TempDir(), "state.db") + "?" + sqlite.DefaultParams

	logA := newLog(t, ctx, &wg, dsn)
	cachedRev, err := logA.CurrentRevision(ctx)
	if err != nil {
		t.Fatalf("CurrentRevision failed: %v", err)
	}

	// advance writes via logB so logA's cache goes stale before each op.
	logB := newLog(t, ctx, &wg, dsn)
	headRev := cachedRev
	advance := func() int64 {
		rev, err := logB.Append(ctx, &server.Event{
			Create: true,
			KV:     &server.KeyValue{Key: fmt.Sprintf("/registry/test-%d", headRev+1), Value: []byte("test")},
		})
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		if rev <= headRev {
			t.Fatalf("test setup failed: head revision %d not ahead of previous head %d", rev, headRev)
		}
		headRev = rev
		return rev
	}
	headRev = advance()

	// List at head on an empty range must refresh the stale cache.
	batch, err := logA.List(ctx, "/registry/missing", "", 0, headRev, false, false)
	if err != nil {
		t.Fatalf("List at head revision %d failed: %v", headRev, err)
	}
	if batch.CurrentRev != headRev {
		t.Fatalf("List returned stale current revision %d, want %d", batch.CurrentRev, headRev)
	}

	advance()
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

	advance()
	batch, err = logA.After(ctx, "", "", headRev, 0)
	if err != nil {
		t.Fatalf("After failed: %v", err)
	}
	if batch.CurrentRev != headRev {
		t.Fatalf("After returned stale current revision %d, want %d", batch.CurrentRev, headRev)
	}

	advance()
	cntRev, _, err := logA.Count(ctx, "/registry/missing", "", headRev)
	if err != nil {
		t.Fatalf("Count at head revision %d failed: %v", headRev, err)
	}
	if cntRev != headRev {
		t.Fatalf("Count returned stale current revision %d, want %d", cntRev, headRev)
	}

	if rev, err := logA.CurrentRevision(ctx); err != nil || rev != headRev {
		t.Fatalf("cached revision not refreshed: got %d, want %d (err=%v)", rev, headRev, err)
	}

	advance()
	if rev, err := logA.Compact(ctx, headRev); err != nil {
		t.Fatalf("Compact at head revision %d failed: %v", headRev, err)
	} else if rev != headRev {
		t.Fatalf("Compact returned %d, want %d", rev, headRev)
	}
}

// TestFutureRevision verifies that revisions past the actual head still
// fail with ErrFutureRev after the cache is refreshed.
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
