package t4

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	kserver "github.com/k3s-io/kine/pkg/server"
)

// newLocalBackend boots a t4 driver in local-only mode (no S3 bucket) against
// a per-test data directory and returns the kine Backend.
func newLocalBackend(t *testing.T) (kserver.Backend, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	dir := t.TempDir()
	var wg sync.WaitGroup
	_, b, err := New(ctx, &wg, &drivers.Config{
		DataSourceName: dir,
	})
	if err != nil {
		t.Fatalf("t4 New: %v", err)
	}
	if err := b.Start(ctx); err != nil {
		t.Fatalf("t4 Start: %v", err)
	}
	return b, ctx
}

func TestT4Backend_CreateGetUpdateDelete(t *testing.T) {
	b, ctx := newLocalBackend(t)

	rev, err := b.Create(ctx, "/a", []byte("v1"), 0)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if rev == 0 {
		t.Fatal("Create returned zero revision")
	}

	if _, err := b.Create(ctx, "/a", []byte("v1"), 0); !errors.Is(err, kserver.ErrKeyExists) {
		t.Fatalf("Create duplicate: want ErrKeyExists, got %v", err)
	}

	_, kv, err := b.Get(ctx, "/a", "", 0, 0, false)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(kv.Value) != "v1" {
		t.Fatalf("Get value = %q, want %q", kv.Value, "v1")
	}

	rev2, _, ok, err := b.Update(ctx, "/a", []byte("v2"), rev, 0)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !ok {
		t.Fatal("Update returned ok=false")
	}
	if rev2 <= rev {
		t.Fatalf("Update revision %d not greater than %d", rev2, rev)
	}

	_, kv, err = b.Get(ctx, "/a", "", 0, 0, false)
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if string(kv.Value) != "v2" {
		t.Fatalf("Get after update value = %q, want %q", kv.Value, "v2")
	}

	_, _, ok, err = b.Delete(ctx, "/a", rev2)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !ok {
		t.Fatal("Delete returned ok=false")
	}

	_, kv, err = b.Get(ctx, "/a", "", 0, 0, false)
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if kv != nil {
		t.Fatalf("Get after delete returned non-nil kv: %+v", kv)
	}
}

func TestT4Backend_StartRefreshesExistingHealthKey(t *testing.T) {
	dir := t.TempDir()

	start := func(t *testing.T) (*backend, context.CancelFunc, *sync.WaitGroup) {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		_, b, err := New(ctx, wg, &drivers.Config{DataSourceName: dir})
		if err != nil {
			cancel()
			t.Fatalf("t4 New: %v", err)
		}
		tb, ok := b.(*backend)
		if !ok {
			cancel()
			t.Fatalf("backend type = %T, want *backend", b)
		}
		if err := tb.Start(ctx); err != nil {
			cancel()
			t.Fatalf("t4 Start: %v", err)
		}
		return tb, cancel, wg
	}

	first, cancelFirst, wgFirst := start(t)
	firstRev := first.node.CurrentRevision()
	cancelFirst()
	wgFirst.Wait()

	second, cancelSecond, wgSecond := start(t)
	defer func() {
		cancelSecond()
		wgSecond.Wait()
	}()
	if secondRev := second.node.CurrentRevision(); secondRev <= firstRev {
		t.Fatalf("restart current revision = %d, want > %d", secondRev, firstRev)
	}
}

func TestT4Backend_ListAndCount(t *testing.T) {
	b, ctx := newLocalBackend(t)

	for _, k := range []string{"/p/a", "/p/b", "/p/c", "/q/x"} {
		if _, err := b.Create(ctx, k, []byte("v"), 0); err != nil {
			t.Fatalf("Create %s: %v", k, err)
		}
	}

	_, kvs, err := b.List(ctx, "/p/", "", 0, 0, false)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(kvs) != 3 {
		t.Fatalf("List under /p/ returned %d, want 3", len(kvs))
	}

	_, count, err := b.Count(ctx, "/p/", "", 0)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 3 {
		t.Fatalf("Count under /p/ = %d, want 3", count)
	}

	_, kvs, err = b.List(ctx, "/p/", "", 2, 0, false)
	if err != nil {
		t.Fatalf("List limit=2: %v", err)
	}
	if len(kvs) != 2 {
		t.Fatalf("List limit=2 returned %d", len(kvs))
	}
}

func TestT4Backend_Watch(t *testing.T) {
	b, ctx := newLocalBackend(t)

	startRev, err := b.CurrentRevision(ctx)
	if err != nil {
		t.Fatalf("CurrentRevision: %v", err)
	}

	wr := b.Watch(ctx, "/w/", startRev+1)

	doneCh := make(chan struct{})
	gotCreate, gotUpdate, gotDelete := false, false, false
	go func() {
		defer close(doneCh)
		for batch := range wr.Events {
			for _, ev := range batch {
				switch {
				case ev.Create:
					gotCreate = true
				case ev.Delete:
					gotDelete = true
				default:
					gotUpdate = true
				}
				if gotCreate && gotUpdate && gotDelete {
					return
				}
			}
		}
	}()

	rev1, err := b.Create(ctx, "/w/k", []byte("v1"), 0)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rev2, _, _, err := b.Update(ctx, "/w/k", []byte("v2"), rev1, 0)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if _, _, _, err := b.Delete(ctx, "/w/k", rev2); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("Watch timeout: create=%v update=%v delete=%v", gotCreate, gotUpdate, gotDelete)
	}
}

func TestT4Backend_CompactAndCompactedWatch(t *testing.T) {
	b, ctx := newLocalBackend(t)

	rev1, err := b.Create(ctx, "/c/k", []byte("v1"), 0)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	rev2, _, _, err := b.Update(ctx, "/c/k", []byte("v2"), rev1, 0)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	if _, err := b.Compact(ctx, rev2); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	wr := b.Watch(ctx, "/c/", rev1)
	select {
	case err := <-wr.Errorc:
		if !errors.Is(err, kserver.ErrCompacted) {
			t.Fatalf("Watch at compacted rev: want ErrCompacted, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Watch at compacted rev did not return ErrCompacted")
	}
}

func TestT4Backend_FutureRevisionRejected(t *testing.T) {
	b, ctx := newLocalBackend(t)

	if _, err := b.Create(ctx, "/f/k", []byte("v"), 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	cur, err := b.CurrentRevision(ctx)
	if err != nil {
		t.Fatalf("CurrentRevision: %v", err)
	}

	_, _, err = b.Get(ctx, "/f/k", "", 0, cur+100, false)
	if !errors.Is(err, kserver.ErrFutureRev) {
		t.Fatalf("Get future rev: want ErrFutureRev, got %v", err)
	}
}

func TestT4Backend_DbSize(t *testing.T) {
	b, ctx := newLocalBackend(t)

	for i := range 16 {
		key := "/d/" + string(rune('a'+i))
		if _, err := b.Create(ctx, key, []byte("payload-payload-payload"), 0); err != nil {
			t.Fatalf("Create %s: %v", key, err)
		}
	}

	size, err := b.DbSize(ctx)
	if err != nil {
		t.Fatalf("DbSize: %v", err)
	}
	if size <= 0 {
		t.Fatalf("DbSize returned %d, want > 0", size)
	}
}
