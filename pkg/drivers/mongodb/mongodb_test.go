package mongodb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	kserver "github.com/k3s-io/kine/pkg/server"
)

// nonAlphanumRe strips characters not allowed in MongoDB database names.
var nonAlphanumRe = regexp.MustCompile(`[^a-zA-Z0-9]`)

// setupBackend creates a backend connected to a running MongoDB instance.
// Each test gets its own database (derived from t.Name()) which is dropped on cleanup,
// preventing data leakage between tests.
//
// The base connection URI is read from MONGODB_TEST_URL.
// If not set, the test is skipped.
//
// To run the tests locally:
//
//	docker run -d --rm -p 27017:27017 --name kine-mongo mongo:7.0
//	MONGODB_TEST_URL=mongodb://localhost:27017 go test ./pkg/drivers/mongodb/...
func setupBackend(t *testing.T) (context.Context, *sync.WaitGroup, kserver.Backend) {
	t.Helper()

	baseURI := os.Getenv("MONGODB_TEST_URL")
	if baseURI == "" {
		t.Skip("skipping: MONGODB_TEST_URL not set — start a MongoDB instance and set the variable")
	}

	// Build a unique database name from the test name so tests are fully isolated.
	dbName := "kinetest_" + strings.ToLower(nonAlphanumRe.ReplaceAllString(t.Name(), "_"))
	if len(dbName) > 38 {
		dbName = dbName[:38]
	}
	uri := strings.TrimRight(baseURI, "/") + "/" + dbName

	ctx := context.Background()
	wg := &sync.WaitGroup{}

	_, backend, err := New(ctx, wg, &drivers.Config{DataSourceName: uri})
	if err != nil {
		t.Fatalf("creating MongoDB backend: %v", err)
	}
	if err := backend.Start(ctx); err != nil {
		t.Skipf("skipping: could not connect to MongoDB at %s: %v", baseURI, err)
	}

	// Drop the test database after the test completes.
	t.Cleanup(func() {
		if b, ok := backend.(*MongoBackend); ok {
			_ = b.db.Drop(context.Background())
		}
	})

	return ctx, wg, backend
}

// --- helper assertions ---

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func expEqualErr(t *testing.T, want, got error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("expected error %v, got %v", want, got)
	}
}

func expEqual[T comparable](t *testing.T, want, got T) {
	t.Helper()
	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func expSortedKeys(t *testing.T, ents []*kserver.KeyValue) {
	t.Helper()
	var prev string
	for _, ent := range ents {
		if prev != "" && prev > ent.Key {
			t.Fatalf("keys not sorted: %s > %s", prev, ent.Key)
		}
		prev = ent.Key
	}
}

func expEqualKeys(t *testing.T, want []string, got []*kserver.KeyValue) {
	t.Helper()
	expEqual(t, len(want), len(got))
	for i, k := range want {
		expEqual(t, k, got[i].Key)
	}
}

// --- tests ---

func TestMongoDB_Create(t *testing.T) {
	ctx, _, b := setupBackend(t)

	pfx := func(k string) string { return "/test" + k }

	baseRev, err := b.CurrentRevision(ctx)
	noErr(t, err)

	// Create a key — revision must advance.
	rev, err := b.Create(ctx, pfx("/a"), []byte("val-a"), 0)
	noErr(t, err)
	expEqual(t, baseRev+1, rev)

	// Creating the same key again must return ErrKeyExists.
	_, err = b.Create(ctx, pfx("/a"), []byte("dup"), 0)
	expEqualErr(t, kserver.ErrKeyExists, err)

	// Create more keys.
	rev2, err := b.Create(ctx, pfx("/b"), []byte("val-b"), 0)
	noErr(t, err)
	expEqual(t, baseRev+2, rev2)

	rev3, err := b.Create(ctx, pfx("/a/child"), []byte("child"), 0)
	noErr(t, err)
	expEqual(t, baseRev+3, rev3)

	// Count must reflect 3 live keys.
	srev, count, err := b.Count(ctx, pfx("/"), "", 0)
	noErr(t, err)
	expEqual(t, baseRev+3, srev)
	expEqual(t, int64(3), count)
}

func TestMongoDB_Get(t *testing.T) {
	ctx, _, b := setupBackend(t)

	pfx := func(k string) string { return "/test" + k }

	baseRev, err := b.CurrentRevision(ctx)
	noErr(t, err)

	// Create a key.
	createRev, err := b.Create(ctx, pfx("/x"), []byte("hello"), 0)
	noErr(t, err)

	// Get it back — latest revision.
	srev, kv, err := b.Get(ctx, pfx("/x"), "", 0, 0, false)
	noErr(t, err)
	expEqual(t, baseRev+1, srev)
	expEqual(t, pfx("/x"), kv.Key)
	expEqual(t, "hello", string(kv.Value))
	expEqual(t, createRev, kv.CreateRevision)
	expEqual(t, createRev, kv.ModRevision)

	// Update the key.
	updateRev, _, ok, err := b.Update(ctx, pfx("/x"), []byte("world"), createRev, 0)
	noErr(t, err)
	expEqual(t, true, ok)

	// Get at the old revision returns old value.
	srev, kv, err = b.Get(ctx, pfx("/x"), "", 0, createRev, false)
	noErr(t, err)
	expEqual(t, createRev, srev)
	expEqual(t, "hello", string(kv.Value))

	// Get at latest returns new value.
	srev, kv, err = b.Get(ctx, pfx("/x"), "", 0, 0, false)
	noErr(t, err)
	expEqual(t, updateRev, srev)
	expEqual(t, "world", string(kv.Value))
	expEqual(t, createRev, kv.CreateRevision)

	// Get a non-existent key returns nil KV without error.
	srev, kv, err = b.Get(ctx, pfx("/missing"), "", 0, 0, false)
	noErr(t, err)
	if kv != nil {
		t.Fatalf("expected nil KV for missing key, got %+v", kv)
	}

	// Get with keysOnly=true returns no value bytes.
	_, kv, err = b.Get(ctx, pfx("/x"), "", 0, 0, true)
	noErr(t, err)
	if len(kv.Value) != 0 {
		t.Fatalf("expected empty value with keysOnly=true, got %q", kv.Value)
	}
}

func TestMongoDB_Update(t *testing.T) {
	ctx, _, b := setupBackend(t)

	pfx := func(k string) string { return "/test" + k }

	baseRev, err := b.CurrentRevision(ctx)
	noErr(t, err)

	// Create a key.
	createRev, err := b.Create(ctx, pfx("/u"), []byte("v1"), 0)
	noErr(t, err)
	expEqual(t, baseRev+1, createRev)

	// Update successfully.
	rev2, kv, ok, err := b.Update(ctx, pfx("/u"), []byte("v2"), createRev, 0)
	noErr(t, err)
	expEqual(t, true, ok)
	expEqual(t, baseRev+2, rev2)
	expEqual(t, "v2", string(kv.Value))
	expEqual(t, createRev, kv.CreateRevision)
	expEqual(t, rev2, kv.ModRevision)

	// Update again.
	rev3, kv, ok, err := b.Update(ctx, pfx("/u"), []byte("v3"), rev2, 5)
	noErr(t, err)
	expEqual(t, true, ok)
	expEqual(t, baseRev+3, rev3)
	expEqual(t, "v3", string(kv.Value))
	expEqual(t, int64(5), kv.Lease)
	expEqual(t, createRev, kv.CreateRevision)

	// Update with stale revision returns ok=false and the current revision.
	rev4, _, ok, err := b.Update(ctx, pfx("/u"), []byte("stale"), createRev, 0)
	noErr(t, err)
	expEqual(t, false, ok)
	expEqual(t, rev3, rev4)

	// Update non-existent key returns ok=false with zero revision.
	rev5, _, ok, err := b.Update(ctx, pfx("/noexist"), []byte("v"), 99, 0)
	noErr(t, err)
	expEqual(t, false, ok)
	expEqual(t, int64(0), rev5)
}

func TestMongoDB_Delete(t *testing.T) {
	ctx, _, b := setupBackend(t)

	pfx := func(k string) string { return "/test" + k }

	baseRev, err := b.CurrentRevision(ctx)
	noErr(t, err)

	// Create.
	createRev, err := b.Create(ctx, pfx("/d"), []byte("to-delete"), 0)
	noErr(t, err)

	// Delete at wrong revision returns ok=false.
	_, _, ok, err := b.Delete(ctx, pfx("/d"), 9999)
	noErr(t, err)
	expEqual(t, false, ok)

	// Delete at correct revision.
	delRev, kv, ok, err := b.Delete(ctx, pfx("/d"), createRev)
	noErr(t, err)
	expEqual(t, true, ok)
	expEqual(t, baseRev+2, delRev)
	expEqual(t, pfx("/d"), kv.Key)
	expEqual(t, "to-delete", string(kv.Value))

	// Deleting again returns ok=false — key is in tombstone state, maps to IsNotFound.
	_, _, ok, err = b.Delete(ctx, pfx("/d"), 0)
	noErr(t, err)
	expEqual(t, false, ok)

	// After delete, Get returns nil KV.
	_, kv, err = b.Get(ctx, pfx("/d"), "", 0, 0, false)
	noErr(t, err)
	if kv != nil {
		t.Fatalf("expected nil KV after delete, got %+v", kv)
	}

	// Re-create is allowed after delete.
	reCreateRev, err := b.Create(ctx, pfx("/d"), []byte("reborn"), 0)
	noErr(t, err)
	expEqual(t, baseRev+3, reCreateRev)

	// Delete without specifying revision (revision=0) removes the latest.
	_, _, ok, err = b.Delete(ctx, pfx("/d"), 0)
	noErr(t, err)
	expEqual(t, true, ok)
}

func TestMongoDB_List(t *testing.T) {
	ctx, _, b := setupBackend(t)

	pfx := func(k string) string { return "/test" + k }

	baseRev, err := b.CurrentRevision(ctx)
	noErr(t, err)

	// Populate keys in non-alphabetical creation order.
	_, _ = b.Create(ctx, pfx("/a/b/c"), nil, 0)
	_, _ = b.Create(ctx, pfx("/a"), nil, 0)
	_, _ = b.Create(ctx, pfx("/b"), nil, 0)
	_, _ = b.Create(ctx, pfx("/a/b"), nil, 0)
	_, _ = b.Create(ctx, pfx("/c"), nil, 0)
	_, _ = b.Create(ctx, pfx("/d/a"), nil, 0)
	_, _ = b.Create(ctx, pfx("/d/b"), nil, 0)

	// List all keys under the prefix — must be sorted.
	rev, ents, err := b.List(ctx, pfx("/"), "", 0, 0, false)
	noErr(t, err)
	expEqual(t, baseRev+7, rev)
	expEqual(t, 7, len(ents))
	expSortedKeys(t, ents)

	// List sub-prefix /a — must match /a, /a/b, /a/b/c.
	rev, ents, err = b.List(ctx, pfx("/a"), pfx("/a"), 0, 0, false)
	noErr(t, err)
	expEqual(t, baseRev+7, rev)
	expEqual(t, 3, len(ents))
	expSortedKeys(t, ents)

	// List with startKey — only keys >= /test/b.
	rev, ents, err = b.List(ctx, pfx("/"), pfx("/b"), 0, 0, false)
	noErr(t, err)
	expEqual(t, baseRev+7, rev)
	expEqual(t, 4, len(ents))
	expSortedKeys(t, ents)
	expEqualKeys(t, []string{pfx("/b"), pfx("/c"), pfx("/d/a"), pfx("/d/b")}, ents)

	// List at an earlier revision — only 3 keys existed then.
	rev, ents, err = b.List(ctx, pfx("/"), "", 0, baseRev+3, false)
	noErr(t, err)
	expEqual(t, baseRev+3, rev)
	expEqual(t, 3, len(ents))
	expSortedKeys(t, ents)

	// List with keysOnly=true — values must be empty.
	_, ents, err = b.List(ctx, pfx("/"), "", 0, 0, true)
	noErr(t, err)
	for _, e := range ents {
		if len(e.Value) != 0 {
			t.Fatalf("expected empty value with keysOnly=true for key %s", e.Key)
		}
	}

	// List with limit.
	_, ents, err = b.List(ctx, pfx("/"), "", 3, 0, false)
	noErr(t, err)
	expEqual(t, 3, len(ents))
	expSortedKeys(t, ents)
}

func TestMongoDB_Count(t *testing.T) {
	ctx, _, b := setupBackend(t)

	pfx := func(k string) string { return "/test" + k }

	baseRev, err := b.CurrentRevision(ctx)
	noErr(t, err)

	// No keys yet.
	rev, count, err := b.Count(ctx, pfx("/"), "", 0)
	noErr(t, err)
	expEqual(t, baseRev, rev)
	expEqual(t, int64(0), count)

	_, _ = b.Create(ctx, pfx("/a"), nil, 0)
	_, _ = b.Create(ctx, pfx("/b"), nil, 0)
	_, _ = b.Create(ctx, pfx("/a/child"), nil, 0)

	rev, count, err = b.Count(ctx, pfx("/"), "", 0)
	noErr(t, err)
	expEqual(t, baseRev+3, rev)
	expEqual(t, int64(3), count)

	// Sub-prefix count.
	_, count, err = b.Count(ctx, pfx("/a"), "", 0)
	noErr(t, err)
	expEqual(t, int64(2), count)

	// Delete /a — count decreases.
	_, _, _, _ = b.Delete(ctx, pfx("/a"), 0)
	_, count, err = b.Count(ctx, pfx("/"), "", 0)
	noErr(t, err)
	expEqual(t, int64(2), count)

	// Count at an earlier revision.
	_, count, err = b.Count(ctx, pfx("/"), "", baseRev+3)
	noErr(t, err)
	expEqual(t, int64(3), count)
}

func TestMongoDB_Watch(t *testing.T) {
	ctx, _, b := setupBackend(t)

	pfx := func(k string) string { return "/test" + k }

	baseRev, err := b.CurrentRevision(ctx)
	noErr(t, err)

	// Write events before Watch is created — the Watch must still deliver them.
	rev1, _ := b.Create(ctx, pfx("/a"), []byte("v1"), 0)
	rev2, _ := b.Create(ctx, pfx("/a/1"), []byte("v2"), 0)
	rev1, _, _, _ = b.Update(ctx, pfx("/a"), []byte("v1-updated"), rev1, 0)
	_, _, _, _ = b.Delete(ctx, pfx("/a"), rev1)
	_, _, _, _ = b.Update(ctx, pfx("/a/1"), []byte("v2-updated"), rev2, 0)

	// Watch everything from baseRev+1 — must receive all 5 events.
	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	wr := b.Watch(wctx, "/", baseRev+1)

	var events []*kserver.Event
	for batch := range wr.Events {
		events = append(events, batch...)
		if len(events) >= 5 {
			break
		}
	}
	wcancel()

	expEqual(t, 5, len(events))
	expEqual(t, true, events[0].Create)
	expEqual(t, false, events[0].Delete)
	expEqual(t, true, events[3].Delete)

	// Watch only /test/a/ prefix — must receive 2 events (create + update of /test/a/1).
	wctx2, wcancel2 := context.WithTimeout(ctx, 5*time.Second)
	wr2 := b.Watch(wctx2, pfx("/a/"), baseRev)

	events = nil
	for batch := range wr2.Events {
		events = append(events, batch...)
		if len(events) >= 2 {
			break
		}
	}
	wcancel2()

	expEqual(t, 2, len(events))

	// Watch for future events — Write AFTER Watch is registered.
	wctx3, wcancel3 := context.WithTimeout(ctx, 5*time.Second)
	curRev, _ := b.CurrentRevision(ctx)
	wr3 := b.Watch(wctx3, pfx("/future/"), curRev)

	// Small pause to ensure Watch goroutine is blocked waiting for notifications.
	time.Sleep(20 * time.Millisecond)

	_, _ = b.Create(ctx, pfx("/future/x"), []byte("new"), 0)
	_, _ = b.Create(ctx, pfx("/future/y"), []byte("new"), 0)

	events = nil
	for batch := range wr3.Events {
		events = append(events, batch...)
		if len(events) >= 2 {
			break
		}
	}
	wcancel3()

	expEqual(t, 2, len(events))
}

func TestMongoDB_Compact(t *testing.T) {
	ctx, _, b := setupBackend(t)

	pfx := func(k string) string { return "/test" + k }

	// Create and update keys to generate multiple revisions.
	createRev, err := b.Create(ctx, pfx("/compact"), []byte("v1"), 0)
	noErr(t, err)

	upRev, _, _, err := b.Update(ctx, pfx("/compact"), []byte("v2"), createRev, 0)
	noErr(t, err)

	_, _, _, err = b.Update(ctx, pfx("/compact"), []byte("v3"), upRev, 0)
	noErr(t, err)

	createRev2, err := b.Create(ctx, pfx("/todelete"), []byte("gone"), 0)
	noErr(t, err)

	delRev, _, _, err := b.Delete(ctx, pfx("/todelete"), createRev2)
	noErr(t, err)

	// Compact up to delRev — should remove superseded and tombstone docs.
	deleted, err := b.Compact(ctx, delRev)
	noErr(t, err)
	if deleted == 0 {
		t.Fatal("expected Compact to delete at least one document")
	}

	// After compaction, the latest value must still be readable.
	_, kv, err := b.Get(ctx, pfx("/compact"), "", 0, 0, false)
	noErr(t, err)
	if kv == nil {
		t.Fatal("expected key to still exist after compaction")
	}
	expEqual(t, "v3", string(kv.Value))

	// Deleted key must remain absent.
	_, kv, err = b.Get(ctx, pfx("/todelete"), "", 0, 0, false)
	noErr(t, err)
	if kv != nil {
		t.Fatalf("expected deleted key to be absent after compaction, got %+v", kv)
	}
}

func TestMongoDB_DbSize(t *testing.T) {
	ctx, _, b := setupBackend(t)

	pfx := func(k string) string { return "/test" + k }

	size, err := b.DbSize(ctx)
	noErr(t, err)

	// Insert some data and verify size grows (or at least doesn't error).
	for i := range 20 {
		_, _ = b.Create(ctx, fmt.Sprintf("%s/%d", pfx("/sz"), i), make([]byte, 1024), 0)
	}

	size2, err := b.DbSize(ctx)
	noErr(t, err)
	if size2 < size {
		t.Fatalf("expected DbSize to grow after inserts: before=%d after=%d", size, size2)
	}
}
