package memory

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/ttl"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/btree"
)

//revive:disable:unhandled-error

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func expEqualErr(t *testing.T, want, got error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func expEqual[T comparable](t *testing.T, want, got T) {
	t.Helper()
	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func expSortedKeys(t *testing.T, ents []*server.KeyValue) {
	t.Helper()
	var prev string
	for _, ent := range ents {
		if prev != "" && prev > ent.Key {
			t.Fatalf("keys not sorted: %s > %s", prev, ent.Key)
		}
		prev = ent.Key
	}
}

func expEqualKeys(t *testing.T, want []string, got []*server.KeyValue) {
	t.Helper()
	expEqual(t, len(want), len(got))
	for i, k := range want {
		expEqual(t, k, got[i].Key)
	}
}

func setupBackend(t *testing.T) (*Memory, context.Context) {
	t.Helper()
	logrus.SetLevel(logrus.TraceLevel)
	logrus.SetOutput(t.Output())
	b := &Memory{
		keys:     btree.NewMap[string, []*entry](0),
		notifyCh: make(chan struct{}),
	}
	ctx := t.Context()
	t.Cleanup(func() { logrus.SetOutput(os.Stdout) })

	// Skip Start() to avoid the production seed entries (compact_rev_key,
	// /registry/health) so tests can assert exact revision values.
	go ttl.Run(ctx, b)
	return b, ctx
}

func TestCreate(t *testing.T) {
	b, ctx := setupBackend(t)

	rev, err := b.Create(ctx, "/test/a", []byte("val-a"), 0)
	noErr(t, err)
	expEqual(t, int64(1), rev)

	_, err = b.Create(ctx, "/test/a", []byte("val-a"), 0)
	expEqualErr(t, server.ErrKeyExists, err)

	rev, err = b.Create(ctx, "/test/a/b", nil, 0)
	noErr(t, err)
	expEqual(t, int64(2), rev)

	rev, err = b.Create(ctx, "/test/a/b/c", nil, 0)
	noErr(t, err)
	expEqual(t, int64(3), rev)

	_, count, err := b.Count(ctx, "/test/", "/test/", 0)
	noErr(t, err)
	expEqual(t, int64(3), count)
}

func TestCreateAfterDelete(t *testing.T) {
	b, ctx := setupBackend(t)

	rev, err := b.Create(ctx, "/test/a", []byte("v1"), 0)
	noErr(t, err)

	_, _, ok, err := b.Delete(ctx, "/test/a", rev)
	noErr(t, err)
	expEqual(t, true, ok)

	rev, err = b.Create(ctx, "/test/a", []byte("v2"), 0)
	noErr(t, err)
	expEqual(t, int64(3), rev)

	_, kv, err := b.Get(ctx, "/test/a", "", 0, 0, false)
	noErr(t, err)
	expEqual(t, "v2", string(kv.Value))
	expEqual(t, int64(3), kv.CreateRevision)
	expEqual(t, int64(1), kv.Version)
}

func TestGet(t *testing.T) {
	b, ctx := setupBackend(t)

	_, err := b.Create(ctx, "/test/a", []byte("b"), 5)
	noErr(t, err)

	rev, kv, err := b.Get(ctx, "/test/a", "", 0, 0, false)
	noErr(t, err)
	expEqual(t, int64(1), rev)
	expEqual(t, "/test/a", kv.Key)
	expEqual(t, "b", string(kv.Value))
	expEqual(t, int64(5), kv.Lease)
	expEqual(t, int64(1), kv.ModRevision)
	expEqual(t, int64(1), kv.CreateRevision)

	_, kv, err = b.Get(ctx, "/test/nonexistent", "", 0, 0, false)
	noErr(t, err)
	if kv != nil {
		t.Fatal("expected nil for nonexistent key")
	}

	_, _, err = b.Get(ctx, "/test/a", "", 0, 20, false)
	expEqualErr(t, server.ErrFutureRev, err)
}

func TestGetAtRevision(t *testing.T) {
	b, ctx := setupBackend(t)

	rev1, err := b.Create(ctx, "/test/a", []byte("v1"), 0)
	noErr(t, err)

	_, _, _, err = b.Update(ctx, "/test/a", []byte("v2"), rev1, 0)
	noErr(t, err)

	rev, kv, err := b.Get(ctx, "/test/a", "", 0, rev1, false)
	noErr(t, err)
	expEqual(t, rev1, rev)
	expEqual(t, "v1", string(kv.Value))

	rev, kv, err = b.Get(ctx, "/test/a", "", 0, 0, false)
	noErr(t, err)
	expEqual(t, int64(2), rev)
	expEqual(t, "v2", string(kv.Value))
}

func TestUpdate(t *testing.T) {
	b, ctx := setupBackend(t)

	rev, err := b.Create(ctx, "/test/a", []byte("b"), 1)
	noErr(t, err)
	expEqual(t, int64(1), rev)

	rev, kv, ok, err := b.Update(ctx, "/test/a", []byte("c"), rev, 0)
	noErr(t, err)
	expEqual(t, int64(2), rev)
	expEqual(t, true, ok)
	expEqual(t, "/test/a", kv.Key)
	expEqual(t, "c", string(kv.Value))
	expEqual(t, int64(0), kv.Lease)
	expEqual(t, int64(2), kv.ModRevision)
	expEqual(t, int64(1), kv.CreateRevision)
	expEqual(t, int64(2), kv.Version)

	rev, kv, ok, err = b.Update(ctx, "/test/a", []byte("d"), rev, 3)
	noErr(t, err)
	expEqual(t, int64(3), rev)
	expEqual(t, true, ok)
	expEqual(t, "d", string(kv.Value))
	expEqual(t, int64(3), kv.Lease)
	expEqual(t, int64(1), kv.CreateRevision)
	expEqual(t, int64(3), kv.Version)

	// Wrong revision.
	rev, _, ok, err = b.Update(ctx, "/test/a", []byte("e"), 1, 0)
	noErr(t, err)
	expEqual(t, int64(3), rev)
	expEqual(t, false, ok)

	// Nonexistent key.
	_, _, ok, err = b.Update(ctx, "/test/nope", []byte("e"), 1, 0)
	noErr(t, err)
	expEqual(t, false, ok)
}

func TestDelete(t *testing.T) {
	b, ctx := setupBackend(t)

	rev, err := b.Create(ctx, "/test/a", []byte("b"), 1)
	noErr(t, err)
	expEqual(t, int64(1), rev)

	rev, kv, ok, err := b.Delete(ctx, "/test/a", int64(1))
	noErr(t, err)
	expEqual(t, int64(2), rev)
	expEqual(t, true, ok)
	expEqual(t, "/test/a", kv.Key)
	expEqual(t, "b", string(kv.Value))
	expEqual(t, int64(1), kv.Lease)
	expEqual(t, int64(1), kv.ModRevision)
	expEqual(t, int64(1), kv.CreateRevision)

	// Already deleted.
	_, _, ok, err = b.Delete(ctx, "/test/a", 0)
	noErr(t, err)
	expEqual(t, false, ok)

	// Re-create and try wrong revision.
	rev, err = b.Create(ctx, "/test/a", []byte("b"), 0)
	noErr(t, err)
	expEqual(t, int64(3), rev)

	_, _, ok, err = b.Delete(ctx, "/test/a", 1)
	noErr(t, err)
	expEqual(t, false, ok)

	// Force delete (revision=0).
	rev, _, ok, err = b.Delete(ctx, "/test/a", 0)
	noErr(t, err)
	expEqual(t, int64(4), rev)
	expEqual(t, true, ok)
}

func TestList(t *testing.T) {
	b, ctx := setupBackend(t)

	b.Create(ctx, "/test/a/b/c", nil, 0)
	b.Create(ctx, "/test/a", nil, 0)
	b.Create(ctx, "/test/b", nil, 0)
	b.Create(ctx, "/test/a/b", nil, 0)
	b.Create(ctx, "/test/c", nil, 0)
	b.Create(ctx, "/test/d/a", nil, 0)
	b.Create(ctx, "/test/d/b", nil, 0)

	// All keys under prefix.
	rev, ents, err := b.List(ctx, "/test/", "/test/", 0, 0, false)
	noErr(t, err)
	expEqual(t, int64(7), rev)
	expEqual(t, 7, len(ents))
	expSortedKeys(t, ents)

	// Prefix sub-tree.
	rev, ents, err = b.List(ctx, "/test/a", "/test/a", 0, 0, false)
	noErr(t, err)
	expEqual(t, int64(7), rev)
	expEqual(t, 3, len(ents))
	expSortedKeys(t, ents)

	// Start key.
	rev, ents, err = b.List(ctx, "/test/", "/test/b", 0, 0, false)
	noErr(t, err)
	expEqual(t, int64(7), rev)
	expEqual(t, 4, len(ents))
	expSortedKeys(t, ents)

	// At a revision.
	rev, ents, err = b.List(ctx, "/test/", "/test/", 0, 3, false)
	noErr(t, err)
	expEqual(t, int64(3), rev)
	expEqual(t, 3, len(ents))
	expSortedKeys(t, ents)
	expEqualKeys(t, []string{"/test/a", "/test/a/b/c", "/test/b"}, ents)

	// Full list with limit
	rev, ents, err = b.List(ctx, "/test/", "/test/", 4, 0, false)
	noErr(t, err)
	expEqual(t, int64(7), rev)
	expEqual(t, 4, len(ents))
	expSortedKeys(t, ents)
	expEqualKeys(t, []string{"/test/a", "/test/a/b", "/test/a/b/c", "/test/b"}, ents)

	// Start key with range.
	rev, ents, err = b.List(ctx, "/test/", "/test/c", 0, 0, false)
	noErr(t, err)
	expEqual(t, int64(7), rev)
	expEqual(t, 3, len(ents))
	expSortedKeys(t, ents)
	expEqualKeys(t, []string{"/test/c", "/test/d/a", "/test/d/b"}, ents)
}

func TestListExcludesDeleted(t *testing.T) {
	b, ctx := setupBackend(t)

	rev, _ := b.Create(ctx, "/test/a", []byte("val"), 0)
	b.Create(ctx, "/test/b", []byte("val"), 0)
	b.Delete(ctx, "/test/a", rev)

	_, ents, err := b.List(ctx, "/test/", "/test/", 0, 0, false)
	noErr(t, err)
	expEqual(t, 1, len(ents))
	expEqual(t, "/test/b", ents[0].Key)
}

func TestCount(t *testing.T) {
	b, ctx := setupBackend(t)

	b.Create(ctx, "/test/a", nil, 0)
	b.Create(ctx, "/test/b", nil, 0)
	b.Create(ctx, "/test/c", nil, 0)

	rev, count, err := b.Count(ctx, "/test/", "/test/", 0)
	noErr(t, err)
	expEqual(t, int64(3), rev)
	expEqual(t, int64(3), count)

	// Count at an earlier revision.
	_, count, err = b.Count(ctx, "/test/", "/test/", 2)
	noErr(t, err)
	expEqual(t, int64(2), count)
}

func TestWatch(t *testing.T) {
	b, ctx := setupBackend(t)

	rev1, _ := b.Create(ctx, "/test/a", nil, 0)
	rev2, _ := b.Create(ctx, "/test/a/1", nil, 0)
	b.Update(ctx, "/test/a", nil, rev1, 0)
	b.Delete(ctx, "/test/a", int64(3))
	b.Update(ctx, "/test/a/1", nil, rev2, 0)

	// Watch all events from the beginning.
	wctx, cancel := context.WithCancel(ctx)
	wr := b.Watch(wctx, "/", 1)
	time.Sleep(20 * time.Millisecond)
	cancel()

	var events []*server.Event
	for es := range wr.Events {
		events = append(events, es...)
	}
	expEqual(t, 5, len(events))

	// Watch filtered by prefix.
	wctx, cancel = context.WithCancel(ctx)
	wr = b.Watch(wctx, "/test/a/", 1)
	time.Sleep(20 * time.Millisecond)
	cancel()

	events = nil
	for es := range wr.Events {
		events = append(events, es...)
	}
	expEqual(t, 2, len(events))
}

func TestWatchNewEvents(t *testing.T) {
	b, ctx := setupBackend(t)

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wr := b.Watch(wctx, "/test/", 0)

	// Write after watch starts.
	b.Create(ctx, "/test/a", []byte("hello"), 0)

	select {
	case events := <-wr.Events:
		expEqual(t, 1, len(events))
		expEqual(t, "/test/a", events[0].KV.Key)
		expEqual(t, true, events[0].Create)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for watch event")
	}
}

func TestWatchPrevKV(t *testing.T) {
	b, ctx := setupBackend(t)

	rev, _ := b.Create(ctx, "/test/a", []byte("v1"), 0)

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wr := b.Watch(wctx, "/test/", 0)

	b.Update(ctx, "/test/a", []byte("v2"), rev, 0)

	select {
	case events := <-wr.Events:
		expEqual(t, 1, len(events))
		expEqual(t, "v2", string(events[0].KV.Value))
		expEqual(t, "v1", string(events[0].PrevKV.Value))
		expEqual(t, int64(1), events[0].PrevKV.ModRevision)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for watch event")
	}
}

func TestCompact(t *testing.T) {
	b, ctx := setupBackend(t)

	b.Create(ctx, "/test/a", nil, 0)
	b.Create(ctx, "/test/b", nil, 0)
	b.Create(ctx, "/test/c", nil, 0)

	rev, err := b.Compact(ctx, 2)
	noErr(t, err)
	expEqual(t, int64(3), rev)

	// List at compacted revision should fail.
	_, _, err = b.List(ctx, "/test/", "", 0, 1, false)
	expEqualErr(t, server.ErrCompacted, err)

	// List at current revision should work.
	_, ents, err := b.List(ctx, "/test/", "/test/", 0, 0, false)
	noErr(t, err)
	expEqual(t, 3, len(ents))

	// List at the compact boundary should work.
	_, ents, err = b.List(ctx, "/test/", "/test/", 0, 2, false)
	noErr(t, err)
	expEqual(t, 2, len(ents))
}

func TestCompactTrimsHistory(t *testing.T) {
	b, ctx := setupBackend(t)

	rev1, _ := b.Create(ctx, "/test/a", []byte("v1"), 0)
	b.Create(ctx, "/test/b", []byte("v1"), 0)
	rev3, _, _, _ := b.Update(ctx, "/test/a", []byte("v2"), rev1, 0)
	b.Update(ctx, "/test/a", []byte("v3"), rev3, 0)
	// 4 log entries, /test/a has 3 versions, /test/b has 1.

	if got := len(b.log); got != 4 {
		t.Fatalf("log length before compact: got %d, want 4", got)
	}

	if _, err := b.Compact(ctx, 4); err != nil {
		t.Fatal(err)
	}

	// After compact at rev 4 every log entry is at-or-below the boundary, so
	// the log is fully trimmed. Per-key history in m.keys still holds each
	// key's floor entry so reads at the compact boundary resolve correctly.
	if got := len(b.log); got != 0 {
		t.Fatalf("log length after compact: got %d, want 0", got)
	}
	histA, _ := b.keys.Get("/test/a")
	if got := len(histA); got != 1 {
		t.Fatalf("/test/a history after compact: got %d, want 1", got)
	}
	histB, _ := b.keys.Get("/test/b")
	if got := len(histB); got != 1 {
		t.Fatalf("/test/b history after compact: got %d, want 1", got)
	}

	// Latest values are still readable at the current revision.
	_, kv, err := b.Get(ctx, "/test/a", "", 0, 0, false)
	noErr(t, err)
	expEqual(t, "v3", string(kv.Value))

	// Calling Compact at a rev <= current compactRevision is a no-op.
	if _, err := b.Compact(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if got := len(b.log); got != 0 {
		t.Fatalf("log length after no-op compact: got %d, want 0", got)
	}
}

func TestCompactDropsTombstones(t *testing.T) {
	b, ctx := setupBackend(t)

	rev1, _ := b.Create(ctx, "/test/a", []byte("v1"), 0)
	b.Create(ctx, "/test/b", []byte("v1"), 0)
	b.Delete(ctx, "/test/a", rev1)
	// Log: create a (1), create b (2), delete a (3).

	if _, err := b.Compact(ctx, 3); err != nil {
		t.Fatal(err)
	}

	// /test/a's tombstone at rev 3 is at-or-below the compact boundary,
	// so the entire history should be removed.
	if _, ok := b.keys.Get("/test/a"); ok {
		t.Fatal("expected /test/a to be removed after compacting tombstone")
	}
	// /test/b's floor is its create at rev 2, kept in m.keys.
	histB, ok := b.keys.Get("/test/b")
	if !ok || len(histB) != 1 {
		t.Fatalf("/test/b history after compact: got %v, want 1 entry", histB)
	}
	// Every log entry was at-or-below the boundary, so the log is fully trimmed.
	if got := len(b.log); got != 0 {
		t.Fatalf("log length after compact: got %d, want 0", got)
	}
}

func TestCompactStraddlesBoundary(t *testing.T) {
	b, ctx := setupBackend(t)

	b.Create(ctx, "/test/a", []byte("v1"), 0) // rev 1
	b.Create(ctx, "/test/b", []byte("v1"), 0) // rev 2
	b.Create(ctx, "/test/c", []byte("v1"), 0) // rev 3
	b.Create(ctx, "/test/d", []byte("v1"), 0) // rev 4
	b.Create(ctx, "/test/e", []byte("v1"), 0) // rev 5

	if got := len(b.log); got != 5 {
		t.Fatalf("log length before compact: got %d, want 5", got)
	}

	if _, err := b.Compact(ctx, 3); err != nil {
		t.Fatal(err)
	}

	// Log keeps entries strictly above compactRev: rev 4 and rev 5.
	if got := len(b.log); got != 2 {
		t.Fatalf("log length after compact(3): got %d, want 2", got)
	}
	if got := b.log[0].revision; got != 4 {
		t.Fatalf("log[0].revision: got %d, want 4", got)
	}
	if got := b.log[1].revision; got != 5 {
		t.Fatalf("log[1].revision: got %d, want 5", got)
	}

	// logIndexAfter math is anchored at compactRev+1 = 4.
	if got := b.logIndexAfter(3); got != 0 {
		t.Fatalf("logIndexAfter(3): got %d, want 0", got)
	}
	if got := b.logIndexAfter(4); got != 1 {
		t.Fatalf("logIndexAfter(4): got %d, want 1", got)
	}
	if got := b.logIndexAfter(5); got != 2 {
		t.Fatalf("logIndexAfter(5): got %d, want 2", got)
	}
}

func TestWatchCompacted(t *testing.T) {
	b, ctx := setupBackend(t)

	b.Create(ctx, "/test/a", nil, 0)
	b.Create(ctx, "/test/b", nil, 0)
	b.Compact(ctx, 2)

	wr := b.Watch(ctx, "/test/", 1)
	expEqual(t, int64(2), wr.CompactRevision)

	// Events channel should be closed immediately.
	_, ok := <-wr.Events
	expEqual(t, false, ok)
}

func TestCurrentRevision(t *testing.T) {
	b, ctx := setupBackend(t)

	rev, err := b.CurrentRevision(ctx)
	noErr(t, err)
	expEqual(t, int64(0), rev)

	b.Create(ctx, "/test/a", nil, 0)

	rev, err = b.CurrentRevision(ctx)
	noErr(t, err)
	expEqual(t, int64(1), rev)
}

func TestDbSize(t *testing.T) {
	b, ctx := setupBackend(t)

	size, err := b.DbSize(ctx)
	noErr(t, err)
	expEqual(t, int64(0), size)

	b.Create(ctx, "/test/a", []byte("some data"), 0)

	size, err = b.DbSize(ctx)
	noErr(t, err)
	if size <= 0 {
		t.Fatal("expected positive db size")
	}
}

func TestKeysOnly(t *testing.T) {
	b, ctx := setupBackend(t)

	b.Create(ctx, "/test/a", []byte("value-a"), 0)
	b.Create(ctx, "/test/b", []byte("value-b"), 0)

	_, ents, err := b.List(ctx, "/test/", "/test/", 0, 0, true)
	noErr(t, err)
	expEqual(t, 2, len(ents))
	for _, kv := range ents {
		if kv.Value != nil {
			t.Fatalf("expected nil value for keysOnly, got %q", kv.Value)
		}
	}
}

func TestVersionIncrement(t *testing.T) {
	b, ctx := setupBackend(t)

	rev, _ := b.Create(ctx, "/test/a", []byte("v1"), 0)

	_, kv, err := b.Get(ctx, "/test/a", "", 0, 0, false)
	noErr(t, err)
	expEqual(t, int64(1), kv.Version)

	rev, _, _, _ = b.Update(ctx, "/test/a", []byte("v2"), rev, 0)
	_, kv, err = b.Get(ctx, "/test/a", "", 0, 0, false)
	noErr(t, err)
	expEqual(t, int64(2), kv.Version)

	b.Update(ctx, "/test/a", []byte("v3"), rev, 0)
	_, kv, err = b.Get(ctx, "/test/a", "", 0, 0, false)
	noErr(t, err)
	expEqual(t, int64(3), kv.Version)
}

// TestTTLExpiration creates 10 keys with leases spread across a ~30 second
// window and verifies the TTL goroutine deletes each one after its lease
// elapses. This exercises the workqueue + watch driven expiration path.
func TestTTLExpiration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running TTL test in -short mode")
	}

	b, ctx := setupBackend(t)

	type seeded struct {
		key   string
		lease int64
	}
	leases := []int64{2, 4, 6, 9, 12, 15, 18, 22, 25, 28}
	keys := make([]seeded, 0, len(leases))

	start := time.Now()
	for i, lease := range leases {
		key := fmt.Sprintf("/test/lease-%02d", i)
		_, err := b.Create(ctx, key, []byte("v"), lease)
		noErr(t, err)
		keys = append(keys, seeded{key, lease})
	}

	// All keys should be present immediately after creation.
	_, ents, err := b.List(ctx, "/test/", "/test/", 0, 0, false)
	noErr(t, err)
	expEqual(t, len(leases), len(ents))

	// Each key should be deleted shortly after its lease elapses. We poll up
	// to a small grace period after the deadline to absorb workqueue
	// scheduling latency, and assert that other keys whose leases have not
	// yet elapsed are still present.
	const grace = 3 * time.Second
	for i, k := range keys {
		deadline := start.Add(time.Duration(k.lease)*time.Second + grace)
		for {
			_, kv, err := b.Get(ctx, k.key, "", 0, 0, false)
			noErr(t, err)
			if kv == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("key %s with lease=%ds still present %v after creation",
					k.key, k.lease, time.Since(start))
			}
			time.Sleep(100 * time.Millisecond)
		}

		// Keys with later leases should still be alive — only ones we have
		// already passed the deadline for are allowed to be gone.
		for j := i + 1; j < len(keys); j++ {
			next := keys[j]
			elapsed := time.Since(start)
			if elapsed >= time.Duration(next.lease)*time.Second {
				continue
			}
			_, kv, err := b.Get(ctx, next.key, "", 0, 0, false)
			noErr(t, err)
			if kv == nil {
				t.Fatalf("key %s with lease=%ds removed prematurely at %v",
					next.key, next.lease, elapsed)
			}
		}
	}

	// After the longest lease has expired plus a grace period, nothing
	// should remain under /test/.
	_, ents, err = b.List(ctx, "/test/", "", 0, 0, false)
	noErr(t, err)
	if len(ents) != 0 {
		t.Fatalf("expected /test/ to be empty after all leases expired, got %d keys", len(ents))
	}
}
