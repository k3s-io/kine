package nats

import (
	"context"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	kserver "github.com/k3s-io/kine/pkg/server"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

func setupBackend(ctx context.Context, wg *sync.WaitGroup, t *testing.T) (*server.Server, *nats.Conn, *Backend) {
	ns := test.RunServer(&server.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})

	nc, err := nats.Connect(ns.ClientURL())
	noErr(t, err)

	js, err := jetstream.New(nc)
	noErr(t, err)

	bkt, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  "kine",
		History: 10,
	})
	noErr(t, err)

	l := logrus.New()
	l.SetOutput(io.Discard)

	b := Backend{
		l: l,
	}

	ekv := NewKeyValue("local", wg, bkt, js, 10, b.Delete)

	b.kv = ekv

	err = b.Start(ctx)
	noErr(t, err)

	return ns, nc, &b
}

func TestBackend_Create(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	defer func() {
		cancel()
		wg.Wait()
	}()

	ns, nc, b := setupBackend(ctx, wg, t)
	defer ns.Shutdown()
	defer nc.Drain()
	defer os.RemoveAll(ns.StoreDir())

	prefix := func(key string) string {
		return "/test" + key
	}

	baseRev, err := b.CurrentRevision(ctx)
	noErr(t, err)

	// Create a key.
	rev, err := b.Create(ctx, prefix("/a"), nil, 0)
	noErr(t, err)
	expEqual(t, baseRev+1, rev)

	// Attempt to create again.
	_, err = b.Create(ctx, prefix("/a"), nil, 0)
	expEqualErr(t, err, kserver.ErrKeyExists)

	rev, err = b.Create(ctx, prefix("/a/b"), nil, 0)
	noErr(t, err)
	expEqual(t, baseRev+2, rev)

	rev, err = b.Create(ctx, prefix("/a/b/c"), nil, 0)
	noErr(t, err)
	expEqual(t, baseRev+3, rev)

	// Create with lease.
	rev, err = b.Create(ctx, prefix("/b"), nil, 1)
	noErr(t, err)
	expEqual(t, baseRev+4, rev)

	srev, count, err := b.Count(ctx, prefix("/"), "", 0)
	noErr(t, err)
	expEqual(t, baseRev+4, srev)
	expEqual(t, 4, count)

	time.Sleep(time.Second * 2)

	// Delete from expiry should increment steam sequence by 2.
	// PUT tombstone, then DEL key
	currRev, err := b.CurrentRevision(ctx)
	noErr(t, err)
	expEqual(t, baseRev+6, currRev)

	srev, count, err = b.Count(ctx, prefix("/"), "", 0)
	noErr(t, err)
	expEqual(t, baseRev+6, srev)
	expEqual(t, 3, count)

	rev, err = b.Create(ctx, prefix("/b"), nil, 0)
	noErr(t, err)
	expEqual(t, baseRev+7, rev)

	srev, count, err = b.Count(ctx, prefix("/"), "", 0)
	noErr(t, err)
	expEqual(t, baseRev+7, srev)
	expEqual(t, 4, count)
}

func TestBackend_Get(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	defer func() {
		cancel()
		wg.Wait()
	}()

	ns, nc, b := setupBackend(ctx, wg, t)
	defer ns.Shutdown()
	defer nc.Drain()
	defer os.RemoveAll(ns.StoreDir())

	prefix := func(key string) string {
		return "/test" + key
	}

	baseRev, err := b.CurrentRevision(ctx)
	noErr(t, err)

	// Create with lease.
	_, err = b.Create(ctx, prefix("/a"), []byte("b"), 1)
	noErr(t, err)

	srev, ent, err := b.Get(ctx, prefix("/a"), "", 0, 0, false)
	noErr(t, err)
	expEqual(t, baseRev+1, srev)
	expEqual(t, prefix("/a"), ent.Key)
	expEqual(t, "b", string(ent.Value))
	expEqual(t, 1, ent.Lease)
	expEqual(t, baseRev+1, ent.ModRevision)
	expEqual(t, baseRev+1, ent.CreateRevision)

	time.Sleep(time.Second * 2)

	// Latest is gone.
	_, _, err = b.Get(ctx, prefix("/a"), "", 0, 0, false)
	expEqualErr(t, nil, err)

	// Get at a revision will fail also.
	_, _, err = b.Get(ctx, prefix("/a"), "", 0, 1, false)
	expEqualErr(t, nil, err)

	// Get at later revision, does not exist.
	_, _, err = b.Get(ctx, prefix("/a"), "", 0, 20, false)
	expEqualErr(t, kserver.ErrFutureRev, err)

	// Create it again and update it.
	rev, err := b.Create(ctx, prefix("/a"), []byte("c"), 0)
	noErr(t, err)
	expEqual(t, baseRev+4, rev)

	_, _, _, err = b.Update(ctx, prefix("/a"), []byte("d"), rev, 0)
	noErr(t, err)

	// Get at prior version.
	rev, ent, err = b.Get(ctx, prefix("/a"), "", 0, rev, false)
	noErr(t, err)
	expEqual(t, baseRev+4, rev) // Should be the requested older revision
	expEqual(t, prefix("/a"), ent.Key)
	expEqual(t, "c", string(ent.Value))
	expEqual(t, 0, ent.Lease)
	expEqual(t, rev, ent.ModRevision)
	expEqual(t, rev, ent.CreateRevision)

	rev, ent, err = b.Get(ctx, prefix("/a"), "", 0, 0, false)
	noErr(t, err)
	expEqual(t, baseRev+5, rev) // Should be the latest revision
	expEqual(t, "d", string(ent.Value))
}

func TestBackend_Update(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	defer func() {
		cancel()
		wg.Wait()
	}()

	ns, nc, b := setupBackend(ctx, wg, t)
	defer ns.Shutdown()
	defer nc.Drain()
	defer os.RemoveAll(ns.StoreDir())

	prefix := func(key string) string {
		return "/test" + key
	}

	baseRev, err := b.CurrentRevision(ctx)
	noErr(t, err)

	// Create with lease.
	rev, err := b.Create(ctx, prefix("/a"), []byte("b"), 1)
	noErr(t, err)
	expEqual(t, baseRev+1, rev)

	rev, ent, ok, err := b.Update(ctx, prefix("/a"), []byte("c"), rev, 0)
	noErr(t, err)
	expEqual(t, baseRev+2, rev)
	expEqual(t, true, ok)
	expEqual(t, prefix("/a"), ent.Key)
	expEqual(t, "c", string(ent.Value))
	expEqual(t, 0, ent.Lease)
	expEqual(t, baseRev+2, ent.ModRevision)
	expEqual(t, baseRev+1, ent.CreateRevision)

	rev, ent, ok, err = b.Update(ctx, prefix("/a"), []byte("d"), rev, 1)
	noErr(t, err)
	expEqual(t, baseRev+3, rev)
	expEqual(t, true, ok)
	expEqual(t, prefix("/a"), ent.Key)
	expEqual(t, "d", string(ent.Value))
	expEqual(t, 1, ent.Lease)
	expEqual(t, baseRev+3, ent.ModRevision)
	expEqual(t, baseRev+1, ent.CreateRevision)

	// Update with wrong revision.
	rev, _, ok, err = b.Update(ctx, "/a", []byte("e"), 2, 1)
	noErr(t, err)
	expEqual(t, baseRev+3, rev)
	expEqual(t, false, ok)
}

func TestBackend_Delete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	defer func() {
		cancel()
		wg.Wait()
	}()

	ns, nc, b := setupBackend(ctx, wg, t)
	defer ns.Shutdown()
	defer nc.Drain()
	defer os.RemoveAll(ns.StoreDir())

	prefix := func(key string) string {
		return "/test" + key
	}

	baseRev, err := b.CurrentRevision(ctx)
	noErr(t, err)

	// Create with lease.
	rev, err := b.Create(ctx, prefix("/a"), []byte("b"), 1)
	noErr(t, err)
	expEqual(t, baseRev+1, rev)

	// Note, deleting first performs an update to tombstone
	// the key, followed by a KV delete.
	rev, ent, ok, err := b.Delete(ctx, prefix("/a"), baseRev+1)
	noErr(t, err)
	expEqual(t, baseRev+2, rev)
	expEqual(t, true, ok)
	expEqual(t, prefix("/a"), ent.Key)
	expEqual(t, "b", string(ent.Value))
	expEqual(t, 1, ent.Lease)
	expEqual(t, baseRev+1, ent.ModRevision)
	expEqual(t, baseRev+1, ent.CreateRevision)

	// Create again.
	rev, err = b.Create(ctx, prefix("/a"), []byte("b"), 0)
	noErr(t, err)
	expEqual(t, baseRev+4, rev)

	// Fail to delete since the revision is not the same.
	rev, _, ok, err = b.Delete(ctx, prefix("/a"), baseRev+1)
	noErr(t, err)
	expEqual(t, baseRev+4, rev)
	expEqual(t, false, ok)

	// No revision, will delete the latest.
	rev, _, ok, err = b.Delete(ctx, prefix("/a"), 0)
	noErr(t, err)
	expEqual(t, baseRev+5, rev)
	expEqual(t, true, ok)
}

func TestBackend_List(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	defer func() {
		cancel()
		wg.Wait()
	}()

	ns, nc, b := setupBackend(ctx, wg, t)
	defer ns.Shutdown()
	defer nc.Drain()
	defer os.RemoveAll(ns.StoreDir())

	prefix := func(key string) string {
		return "/test" + key
	}

	baseRev, err := b.CurrentRevision(ctx)
	noErr(t, err)

	// Create a key.
	_, _ = b.Create(ctx, prefix("/a/b/c"), nil, 0)
	_, _ = b.Create(ctx, prefix("/a"), nil, 0)
	_, _ = b.Create(ctx, prefix("/b"), nil, 0)
	_, _ = b.Create(ctx, prefix("/a/b"), nil, 0)
	_, _ = b.Create(ctx, prefix("/c"), nil, 0)
	_, _ = b.Create(ctx, prefix("/d/a"), nil, 0)
	_, _ = b.Create(ctx, prefix("/d/b"), nil, 0)

	// List the keys.
	rev, ents, err := b.List(ctx, prefix("/"), "", 0, 0, false)
	noErr(t, err)
	expEqual(t, baseRev+7, rev)
	expEqual(t, 7, len(ents))
	expSortedKeys(t, ents)

	// List the keys with prefix.
	rev, ents, err = b.List(ctx, prefix("/a"), prefix("/a"), 0, 0, false)
	noErr(t, err)
	expEqual(t, baseRev+7, rev)
	expEqual(t, 3, len(ents))
	expSortedKeys(t, ents)

	// List the keys >= start key.
	rev, ents, err = b.List(ctx, prefix("/"), prefix("/b"), 0, 0, false)
	noErr(t, err)
	expEqual(t, baseRev+7, rev)
	expEqual(t, 4, len(ents))
	expSortedKeys(t, ents)

	// List the keys up to a revision.
	rev, ents, err = b.List(ctx, prefix("/"), "", 0, baseRev+3, false)
	noErr(t, err)
	expEqual(t, baseRev+3, rev)
	expEqual(t, 3, len(ents))
	expSortedKeys(t, ents)
	expEqualKeys(t, []string{prefix("/a"), prefix("/a/b/c"), prefix("/b")}, ents)

	// List the keys with a limit.
	rev, ents, err = b.List(ctx, prefix("/"), "", 4, 0, false)
	noErr(t, err)
	expEqual(t, baseRev+7, rev)
	expEqual(t, 7, len(ents)) // expect full list
	expSortedKeys(t, ents)
	expEqualKeys(t, []string{prefix("/a"), prefix("/a/b"), prefix("/a/b/c"), prefix("/b"), prefix("/c"), prefix("/d/a"), prefix("/d/b")}, ents)

	// List the keys with a limit after some start key.
	rev, ents, err = b.List(ctx, prefix("/"), prefix("/b"), 2, 0, false)
	noErr(t, err)
	expEqual(t, baseRev+7, rev)
	expEqual(t, 4, len(ents))
	expSortedKeys(t, ents)
	expEqualKeys(t, []string{prefix("/b"), prefix("/c"), prefix("/d/a"), prefix("/d/b")}, ents)

	// List the keys after some start key with slash prefix
	rev, ents, err = b.List(ctx, prefix("/"), prefix("/c"), 0, 0, false)
	noErr(t, err)
	expEqual(t, baseRev+7, rev)
	expEqual(t, 3, len(ents))
	expSortedKeys(t, ents)
	expEqualKeys(t, []string{prefix("/c"), prefix("/d/a"), prefix("/d/b")}, ents)
}

func TestBackend_Watch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	defer func() {
		cancel()
		wg.Wait()
	}()

	ns, nc, b := setupBackend(ctx, wg, t)
	defer ns.Shutdown()
	defer nc.Drain()
	defer os.RemoveAll(ns.StoreDir())

	prefix := func(key string) string {
		return "/test" + key
	}

	baseRev, err := b.CurrentRevision(ctx)
	noErr(t, err)

	cctx, cancel := context.WithCancel(ctx)

	rev1, _ := b.Create(ctx, prefix("/a"), nil, 0)
	rev2, _ := b.Create(ctx, prefix("/a/1"), nil, 0)
	rev1, _, _, _ = b.Update(ctx, prefix("/a"), nil, rev1, 0)
	_, _, _, _ = b.Delete(ctx, prefix("/a"), rev1)
	_, _, _, _ = b.Update(ctx, prefix("/a/1"), nil, rev2, 0)

	wr := b.Watch(cctx, "/", baseRev+1)
	time.Sleep(20 * time.Millisecond)
	cancel()

	var events []*kserver.Event
	for es := range wr.Events {
		events = append(events, es...)
	}

	expEqual(t, 5, len(events))

	cctx, cancel = context.WithCancel(ctx)
	wr = b.Watch(cctx, prefix("/a/"), baseRev)
	time.Sleep(20 * time.Millisecond)
	cancel()

	events = make([]*kserver.Event, 0)

	for es := range wr.Events {
		events = append(events, es...)
	}

	expEqual(t, 2, len(events))
}
