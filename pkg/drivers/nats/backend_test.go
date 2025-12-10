package nats

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	kserver "github.com/k3s-io/kine/pkg/server"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func expEqualErr(t *testing.T, want, got error) {
	t.Helper()
	if !errors.Is(want, got) {
		t.Fatalf("expected %v, got %v", want, got)
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
		if prev != "" {
			if prev > ent.Key {
				t.Fatalf("keys not sorted: %s > %s", prev, ent.Key)
			}
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

func setupBackend(t *testing.T) (*server.Server, *nats.Conn, *Backend) {
	ns := test.RunServer(&server.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})

	nc, err := nats.Connect(ns.ClientURL())
	noErr(t, err)

	js, err := jetstream.New(nc)
	noErr(t, err)

	ctx := context.Background()

	bkt, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  "kine",
		History: 10,
	})
	noErr(t, err)

	ekv := NewKeyValue(ctx, bkt, js)

	l := logrus.New()
	l.SetOutput(io.Discard)

	b := Backend{
		l:  l,
		kv: ekv,
		js: js,
	}

	return ns, nc, &b
}

func TestBackend_Create(t *testing.T) {
	ns, nc, b := setupBackend(t)
	defer ns.Shutdown()
	defer nc.Drain()

	ctx := context.Background()

	// Create a key.
	rev, err := b.Create(ctx, "/a", nil, 0)
	noErr(t, err)
	expEqual(t, 1, rev)

	// Attempt to create again.
	_, err = b.Create(ctx, "/a", nil, 0)
	expEqualErr(t, err, kserver.ErrKeyExists)

	rev, err = b.Create(ctx, "/a/b", nil, 0)
	noErr(t, err)
	expEqual(t, 2, rev)

	rev, err = b.Create(ctx, "/a/b/c", nil, 0)
	noErr(t, err)
	expEqual(t, 3, rev)

	rev, err = b.Create(ctx, "/b", nil, 1)
	noErr(t, err)
	expEqual(t, 4, rev)

	time.Sleep(2 * time.Millisecond)

	srev, count, err := b.Count(ctx, "/", "", 0)
	noErr(t, err)
	expEqual(t, 4, srev)
	expEqual(t, 4, count)

	time.Sleep(time.Second)

	srev, count, err = b.Count(ctx, "/", "", 0)
	noErr(t, err)
	expEqual(t, 4, srev)
	expEqual(t, 3, count)

	// Create /b again. Rev is 6 due to the internal delete.
	// on read.
	rev, err = b.Create(ctx, "/b", nil, 0)
	noErr(t, err)
	expEqual(t, 6, rev)

	time.Sleep(2 * time.Millisecond)

	srev, count, err = b.Count(ctx, "/", "", 0)
	noErr(t, err)
	expEqual(t, 6, srev)
	expEqual(t, 4, count)
}

func TestBackend_Get(t *testing.T) {
	ns, nc, b := setupBackend(t)
	defer ns.Shutdown()
	defer nc.Drain()

	ctx := context.Background()

	// Create with lease.
	rev, err := b.Create(ctx, "/a", []byte("b"), 1)
	noErr(t, err)

	time.Sleep(2 * time.Millisecond)

	srev, ent, err := b.Get(ctx, "/a", "", 0, 0, false)
	noErr(t, err)
	expEqual(t, 1, srev)
	expEqual(t, "/a", ent.Key)
	expEqual(t, "b", string(ent.Value))
	expEqual(t, 1, ent.Lease)
	expEqual(t, 1, ent.ModRevision)
	expEqual(t, 1, ent.CreateRevision)

	time.Sleep(time.Second)

	// Latest is gone.
	_, ent, err = b.Get(ctx, "/a", "", 0, 0, false)
	expEqualErr(t, nil, err)

	// Get at a revision will fail also.
	_, ent, err = b.Get(ctx, "/a", "", 0, 1, false)
	expEqualErr(t, nil, err)

	// Get at later revision, does not exist.
	_, _, err = b.Get(ctx, "/a", "", 0, 2, false)
	expEqualErr(t, nil, err)

	// Create it again and update it.
	rev, err = b.Create(ctx, "/a", []byte("c"), 0)
	noErr(t, err)
	expEqual(t, 3, rev)

	_, _, _, err = b.Update(ctx, "/a", []byte("d"), rev, 0)
	noErr(t, err)

	// Get at prior version.
	rev, ent, err = b.Get(ctx, "/a", "", 0, rev, false)
	noErr(t, err)
	expEqual(t, 3, rev)
	expEqual(t, "/a", ent.Key)
	expEqual(t, "c", string(ent.Value))
	expEqual(t, 0, ent.Lease)
	expEqual(t, 3, ent.ModRevision)
	expEqual(t, 3, ent.CreateRevision)
}

func TestBackend_Update(t *testing.T) {
	ns, nc, b := setupBackend(t)
	defer ns.Shutdown()
	defer nc.Drain()

	ctx := context.Background()

	// Create with lease.
	_, _ = b.Create(ctx, "/a", []byte("b"), 1)
	rev, ent, ok, err := b.Update(ctx, "/a", []byte("c"), 1, 0)
	noErr(t, err)
	expEqual(t, 2, rev)
	expEqual(t, true, ok)
	expEqual(t, "/a", ent.Key)
	expEqual(t, "c", string(ent.Value))
	expEqual(t, 0, ent.Lease)
	expEqual(t, 2, ent.ModRevision)
	expEqual(t, 1, ent.CreateRevision)

	rev, ent, ok, err = b.Update(ctx, "/a", []byte("d"), 2, 1)
	noErr(t, err)
	expEqual(t, 3, rev)
	expEqual(t, true, ok)
	expEqual(t, "/a", ent.Key)
	expEqual(t, "d", string(ent.Value))
	expEqual(t, 1, ent.Lease)
	expEqual(t, 3, ent.ModRevision)
	expEqual(t, 1, ent.CreateRevision)

	// Update with wrong revision.
	rev, _, ok, err = b.Update(ctx, "/a", []byte("e"), 2, 1)
	noErr(t, err)
	expEqual(t, 3, rev)
	expEqual(t, false, ok)
}

func TestBackend_Delete(t *testing.T) {
	ns, nc, b := setupBackend(t)
	defer ns.Shutdown()
	defer nc.Drain()

	ctx := context.Background()

	// Create with lease.
	_, _ = b.Create(ctx, "/a", []byte("b"), 1)

	// Note, deleting first performs an update to tombstone
	// the key, followed by a KV delete.
	rev, ent, ok, err := b.Delete(ctx, "/a", 1)
	noErr(t, err)
	expEqual(t, 2, rev)
	expEqual(t, true, ok)
	expEqual(t, "/a", ent.Key)
	expEqual(t, "b", string(ent.Value))
	expEqual(t, 1, ent.Lease)
	expEqual(t, 1, ent.ModRevision)
	expEqual(t, 1, ent.CreateRevision)

	// Create again.
	_, _ = b.Create(ctx, "/a", []byte("b"), 0)

	// Fail to delete since the revision is not the same.
	rev, _, ok, err = b.Delete(ctx, "/a", 1)
	expEqual(t, 4, rev)
	expEqual(t, false, ok)
	expEqualErr(t, nil, err)

	// No revision, will delete the latest.
	rev, _, ok, err = b.Delete(ctx, "/a", 0)
	expEqual(t, 5, rev)
	expEqual(t, true, ok)
	expEqualErr(t, nil, err)
}

func TestBackend_List(t *testing.T) {
	ns, nc, b := setupBackend(t)
	defer ns.Shutdown()
	defer nc.Drain()

	ctx := context.Background()

	// Create a key.
	_, _ = b.Create(ctx, "/a/b/c", nil, 0)
	_, _ = b.Create(ctx, "/a", nil, 0)
	_, _ = b.Create(ctx, "/b", nil, 0)
	_, _ = b.Create(ctx, "/a/b", nil, 0)
	_, _ = b.Create(ctx, "/c", nil, 0)
	_, _ = b.Create(ctx, "/d/a", nil, 0)
	_, _ = b.Create(ctx, "/d/b", nil, 0)

	// Wait for the btree to be updated.
	time.Sleep(time.Millisecond)

	// List the keys.
	rev, ents, err := b.List(ctx, "/", "", 0, 0, false)
	noErr(t, err)
	expEqual(t, 7, rev)
	expEqual(t, 7, len(ents))
	expSortedKeys(t, ents)

	// List the keys with prefix.
	rev, ents, err = b.List(ctx, "/a", "", 0, 0, false)
	noErr(t, err)
	expEqual(t, 7, rev)
	expEqual(t, 3, len(ents))
	expSortedKeys(t, ents)

	// List the keys >= start key.
	rev, ents, err = b.List(ctx, "/", "b", 0, 0, false)
	noErr(t, err)
	expEqual(t, 7, rev)
	expEqual(t, 4, len(ents))
	expSortedKeys(t, ents)

	// List the keys up to a revision.
	rev, ents, err = b.List(ctx, "/", "", 0, 3, false)
	noErr(t, err)
	expEqual(t, 7, rev)
	expEqual(t, 3, len(ents))
	expSortedKeys(t, ents)
	expEqualKeys(t, []string{"/a", "/a/b/c", "/b"}, ents)

	// List the keys with a limit.
	rev, ents, err = b.List(ctx, "/", "", 4, 0, false)
	noErr(t, err)
	expEqual(t, 7, rev)
	expEqual(t, 4, len(ents))
	expSortedKeys(t, ents)
	expEqualKeys(t, []string{"/a", "/a/b", "/a/b/c", "/b"}, ents)

	// List the keys with a limit after some start key.
	rev, ents, err = b.List(ctx, "/", "b", 2, 0, false)
	noErr(t, err)
	expEqual(t, 7, rev)
	expEqual(t, 2, len(ents))
	expSortedKeys(t, ents)
	expEqualKeys(t, []string{"/b", "/c"}, ents)
}

func TestBackend_Watch(t *testing.T) {
	ns, nc, b := setupBackend(t)
	defer ns.Shutdown()
	defer nc.Drain()

	ctx := context.Background()

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rev1, _ := b.Create(ctx, "/a", nil, 0)
	rev2, _ := b.Create(ctx, "/a/1", nil, 0)
	rev1, _, _, _ = b.Update(ctx, "/a", nil, rev1, 0)
	_, _, _, _ = b.Delete(ctx, "/a", rev1)
	_, _, _, _ = b.Update(ctx, "/a/1", nil, rev2, 0)

	wr := b.Watch(cctx, "/a", 0)
	time.Sleep(20 * time.Millisecond)
	cancel()

	var events []*kserver.Event
	for es := range wr.Events {
		events = append(events, es...)
	}

	expEqual(t, 5, len(events))
}
