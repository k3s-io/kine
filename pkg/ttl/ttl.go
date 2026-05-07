// Package ttl runs lease-driven key expiration on top of any server.Backend.
//
// A single goroutine seeds a delaying workqueue from an initial paginated
// List of leased keys, then watches for new lease events from the next
// revision onward. A handler goroutine consumes the queue and deletes each
// key once its lease elapses. Both pkg/drivers/memory and pkg/logstructured
// share this implementation; the only contract is the List/Watch/Delete
// subset of server.Backend.
package ttl

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/client-go/util/workqueue"

	"github.com/k3s-io/kine/pkg/server"
)

const (
	// retryInterval is how long we wait before requeueing a key whose Delete
	// failed for a transient reason.
	retryInterval = 250 * time.Millisecond
	// listPageSize is the page size used when seeding the workqueue from the
	// initial list of leased keys. Backends that ignore the limit parameter
	// (e.g. the in-memory backend) will return everything in one call; the
	// pagination loop terminates correctly either way.
	listPageSize = 1000
)

type entry struct {
	key         string
	modRevision int64
	expiredAt   time.Time
}

// Run consumes leased keys from b and deletes each one when its TTL elapses.
// It blocks until ctx is canceled or the watch channel closes. Callers
// typically launch this with `go ttl.Run(ctx, backend)`.
func Run(ctx context.Context, b server.Backend) {
	queue := workqueue.NewTypedDelayingQueue[string]()
	var mu sync.RWMutex
	store := make(map[string]*entry)

	go func() {
		for handle(ctx, b, &mu, queue, store) {
		}
	}()

	rev, err := seed(ctx, b, &mu, queue, store)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			logrus.Errorf("TTL initial list failed: %v", err)
		}
		queue.ShutDown()
		return
	}

	// Watch from rev+1: seed already covered everything up to rev, so we
	// only need events strictly after.
	wr := b.Watch(ctx, "/", rev+1)
	if wr.CompactRevision != 0 {
		logrus.Errorf("TTL event watch failed: %v", server.ErrCompacted)
		queue.ShutDown()
		return
	}

	for {
		select {
		case <-ctx.Done():
			queue.ShutDown()
			return
		case events, ok := <-wr.Events:
			if !ok {
				queue.ShutDown()
				return
			}
			for _, event := range events {
				if event.Delete || event.KV == nil || event.KV.Lease <= 0 {
					continue
				}
				kv := event.KV
				stored := load(&mu, store, kv.Key)
				if stored == nil || kv.ModRevision > stored.modRevision {
					expires := save(&mu, store, kv)
					logrus.Tracef("TTL set key=%v modRev=%v ttl=%v", kv.Key, kv.ModRevision, expires)
					queue.AddAfter(kv.Key, expires)
				}
			}
		}
	}
}

// seed pages through every leased key at the current revision and adds it
// to the workqueue. Pagination is anchored at the revision returned by the
// first List so subsequent pages are stable.
func seed(ctx context.Context, b server.Backend, mu *sync.RWMutex, queue workqueue.TypedDelayingInterface[string], store map[string]*entry) (int64, error) {
	rev, kvs, err := b.List(ctx, "/", "/", listPageSize, 0, true)
	if err != nil {
		return rev, err
	}
	for len(kvs) > 0 {
		for _, kv := range kvs {
			if kv.Lease > 0 {
				expires := save(mu, store, kv)
				logrus.Tracef("TTL seed key=%v modRev=%v ttl=%v", kv.Key, kv.ModRevision, expires)
				queue.AddAfter(kv.Key, expires)
			}
		}
		if int64(len(kvs)) < listPageSize {
			break
		}
		_, kvs, err = b.List(ctx, "/", kvs[len(kvs)-1].Key, listPageSize, rev, true)
		if err != nil {
			return rev, err
		}
	}
	return rev, nil
}

// handle processes one queue item. Returns false when the queue has shut
// down, true otherwise so the caller can keep looping.
func handle(ctx context.Context, b server.Backend, mu *sync.RWMutex, queue workqueue.TypedDelayingInterface[string], store map[string]*entry) bool {
	key, shutdown := queue.Get()
	if shutdown {
		logrus.Info("TTL events work queue has shut down")
		return false
	}
	defer queue.Done(key)

	e := load(mu, store, key)
	if e == nil {
		logrus.Errorf("TTL event not found for key=%v", key)
		return true
	}

	if expires := time.Until(e.expiredAt); expires > 0 {
		logrus.Tracef("TTL has not expired for key=%v, ttl=%v, requeuing", key, expires)
		queue.AddAfter(key, expires)
		return true
	}

	logrus.Tracef("TTL delete key=%v modRev=%v", e.key, e.modRevision)
	if _, _, _, err := b.Delete(ctx, e.key, e.modRevision); err != nil && !errors.Is(err, context.Canceled) {
		logrus.Errorf("TTL delete trigger failed for key=%v: %v, requeuing", e.key, err)
		queue.AddAfter(e.key, retryInterval)
		return true
	}

	mu.Lock()
	defer mu.Unlock()
	delete(store, e.key)
	return true
}

func load(mu *sync.RWMutex, store map[string]*entry, key string) *entry {
	mu.RLock()
	defer mu.RUnlock()
	return store[key]
}

func save(mu *sync.RWMutex, store map[string]*entry, kv *server.KeyValue) time.Duration {
	mu.Lock()
	defer mu.Unlock()
	expires := time.Duration(kv.Lease) * time.Second
	store[kv.Key] = &entry{
		key:         kv.Key,
		modRevision: kv.ModRevision,
		expiredAt:   time.Now().Add(expires),
	}
	return expires
}
