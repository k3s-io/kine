package nats

import (
	"context"
	"encoding/json"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

// TODO: version this data structure to simplify and optimize for size.
type natsData struct {
	// v1 fields
	KV           *server.KeyValue `json:"KV"`
	PrevRevision int64            `json:"PrevRevision"`
	Create       bool             `json:"Create"`
	Delete       bool             `json:"Delete"`

	CreateTime time.Time `json:"-"`
}

func (d *natsData) Encode() ([]byte, error) {
	buf, err := json.Marshal(d)
	return buf, err
}

func (d *natsData) Decode(e jetstream.KeyValueEntry) error {
	if e == nil || e.Value() == nil {
		return nil
	}

	err := json.Unmarshal(e.Value(), d)
	if err != nil {
		return err
	}
	d.KV.ModRevision = int64(e.Revision())
	if d.KV.CreateRevision == 0 {
		d.KV.CreateRevision = d.KV.ModRevision
	}
	d.CreateTime = e.Created()
	return nil
}

var (
	// Ensure Backend implements server.Backend.
	_ server.Backend = (&Backend{})
)

type Backend struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	kv     *KeyValue
	l      *logrus.Logger
	cancel context.CancelFunc
}

func (b *Backend) Close() error {
	b.cancel()
	return b.nc.Drain()
}

// isExpiredKey checks if the key is expired based on the create time and lease.
func (b *Backend) isExpiredKey(value *natsData) bool {
	if value.KV.Lease == 0 {
		return false
	}

	return time.Now().After(value.CreateTime.Add(time.Second * time.Duration(value.KV.Lease)))
}

// get returns the key-value entry for the given key and revision, if specified.
// This takes into account entries that have been marked as deleted or expired.
func (b *Backend) get(ctx context.Context, key string, revision int64, allowDeletes bool) (int64, *natsData, error) {
	var (
		entry jetstream.KeyValueEntry
		err   error
	)

	// Get latest revision if not specified.
	if revision <= 0 {
		entry, err = b.kv.Get(ctx, key)
	} else {
		entry, err = b.kv.GetRevision(ctx, key, uint64(revision))
	}
	if err != nil {
		return 0, nil, err
	}

	rev := int64(entry.Revision())

	var val natsData
	err = val.Decode(entry)
	if err != nil {
		return 0, nil, err
	}

	if val.Delete && !allowDeletes {
		return 0, nil, jetstream.ErrKeyNotFound
	}

	if b.isExpiredKey(&val) {
		err := b.kv.Delete(ctx, val.KV.Key, jetstream.LastRevision(uint64(rev)))
		if err != nil {
			b.l.Warnf("Failed to delete expired key %s: %v", val.KV.Key, err)
		}
		// Return a zero indicating the key was deleted.
		return 0, nil, jetstream.ErrKeyNotFound
	}

	return rev, &val, nil
}

// Start starts the backend.
// See https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
func (b *Backend) Start(ctx context.Context) error {
	if _, err := b.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0); err != nil {
		if err != server.ErrKeyExists {
			b.l.Errorf("Failed to create health check key: %v", err)
		}
	}
	return nil
}

// DbSize get the kineBucket size from JetStream.
func (b *Backend) DbSize(ctx context.Context) (int64, error) {
	return b.kv.BucketSize(ctx)
}

// CurrentRevision returns the current revision of the database.
func (b *Backend) CurrentRevision(ctx context.Context) (int64, error) {
	return b.kv.BucketRevision(), nil
}

// Count returns an exact count of the number of matching keys and the current revision of the database.
func (b *Backend) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	count, err := b.kv.Count(ctx, prefix, startKey, revision)
	if err != nil {
		return 0, 0, err
	}

	storeRev := b.kv.BucketRevision()
	return storeRev, count, nil
}

// Get returns the store's current revision, the associated server.KeyValue or an error.
func (b *Backend) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (int64, *server.KeyValue, error) {
	storeRev := b.kv.BucketRevision()
	// Get the kv entry and return the revision.
	rev, nv, err := b.get(ctx, key, revision, false)
	if err == nil {
		if nv == nil {
			return storeRev, nil, nil
		}
		return rev, nv.KV, nil
	}
	if err == jetstream.ErrKeyNotFound {
		return storeRev, nil, nil
	}

	return rev, nil, err
}

// Create attempts to create the key-value entry and returns the revision number.
func (b *Backend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	// Check if key exists already. If the entry exists even if marked as expired or deleted,
	// the revision will be returned to apply an update.
	rev, pnv, err := b.get(ctx, key, 0, true)
	// If an error other than key not found, return.
	if err != nil && err != jetstream.ErrKeyNotFound {
		return 0, err
	}

	nv := natsData{
		Delete:       false,
		Create:       true,
		PrevRevision: 0,
		KV: &server.KeyValue{
			Key:            key,
			CreateRevision: 0,
			ModRevision:    0,
			Value:          value,
			Lease:          lease,
		},
	}

	if pnv != nil {
		if !pnv.Delete {
			return 0, server.ErrKeyExists
		}
		nv.PrevRevision = pnv.KV.ModRevision
	}

	data, err := nv.Encode()
	if err != nil {
		return 0, err
	}

	if pnv != nil {
		seq, err := b.kv.Update(ctx, key, data, uint64(rev))
		if err != nil {
			if jsWrongLastSeqErr.Is(err) {
				b.l.Warnf("create conflict: key=%s, rev=%d, err=%s", key, rev, err)
				return 0, server.ErrKeyExists
			}
			return 0, err
		}

		return int64(seq), nil
	}

	// An update with a zero revision will create the key.
	seq, err := b.kv.Create(ctx, key, data)
	if err != nil {
		if jsWrongLastSeqErr.Is(err) {
			b.l.Warnf("create conflict: key=%s, rev=0, err=%s", key, err)
			return 0, server.ErrKeyExists
		}
		return 0, err
	}

	return int64(seq), nil
}

func (b *Backend) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	// Get the key, allow deletes.
	rev, value, err := b.get(ctx, key, 0, true)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return rev, nil, true, nil
		}
		return rev, nil, false, err
	}
	if value == nil {
		return rev, nil, true, nil
	}
	if value.Delete {
		return rev, value.KV, true, nil
	}
	if revision != 0 && value.KV.ModRevision != revision {
		return rev, value.KV, false, nil
	}

	nv := natsData{
		Delete:       true,
		PrevRevision: rev,
		KV:           value.KV,
	}
	data, err := nv.Encode()
	if err != nil {
		return rev, nil, false, err
	}

	// Update with a tombstone.
	drev, err := b.kv.Update(ctx, key, data, uint64(rev))
	if err != nil {
		if jsWrongLastSeqErr.Is(err) {
			b.l.Warnf("delete conflict: key=%s, rev=%d, err=%s", key, rev, err)
			return 0, nil, false, nil
		}
		return rev, value.KV, false, nil
	}

	err = b.kv.Delete(ctx, key, jetstream.LastRevision(drev))
	if err != nil {
		if jsWrongLastSeqErr.Is(err) {
			b.l.Warnf("delete conflict: key=%s, rev=%d, err=%s", key, drev, err)
			return 0, nil, false, nil
		}
		return rev, value.KV, false, nil
	}

	return int64(drev), value.KV, true, nil
}

func (b *Backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	// Get the latest revision of the key.
	rev, pnd, err := b.get(ctx, key, 0, false)
	// TODO: correct semantics for these various errors?
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return rev, nil, false, nil
		}
		return rev, nil, false, err
	}

	// Return nothing?
	if pnd == nil {
		return 0, nil, false, nil
	}

	// Incorrect revision, return the current value.
	if pnd.KV.ModRevision != revision {
		return rev, pnd.KV, false, nil
	}

	nd := natsData{
		Delete:       false,
		Create:       false,
		PrevRevision: pnd.KV.ModRevision,
		KV: &server.KeyValue{
			Key:            key,
			CreateRevision: pnd.KV.CreateRevision,
			Value:          value,
			Lease:          lease,
		},
	}

	if pnd.KV.CreateRevision == 0 {
		nd.KV.CreateRevision = rev
	}

	data, err := nd.Encode()
	if err != nil {
		return 0, nil, false, err
	}

	seq, err := b.kv.Update(ctx, key, data, uint64(revision))
	if err != nil {
		// This may occur if a concurrent writer created the key.
		if jsWrongLastSeqErr.Is(err) {
			b.l.Warnf("update conflict: key=%s, rev=%d, err=%s", key, revision, err)
			return 0, nil, false, nil
		}
		return 0, nil, false, err
	}

	nd.KV.ModRevision = int64(seq)

	return int64(seq), nd.KV, true, nil
}

// List returns a range of keys starting with the prefix.
// This would translated to one or more tokens, e.g. `a.b.c`.
// The startKey would be the next set of tokens that follow the prefix
// that are alphanumerically equal to or greater than the startKey.
// If limit is provided, the maximum set of matches is limited.
// If revision is provided, this indicates the maximum revision to return.
func (b *Backend) List(ctx context.Context, prefix, startKey string, limit, maxRevision int64) (int64, []*server.KeyValue, error) {
	matches, err := b.kv.List(ctx, prefix, startKey, limit, maxRevision)
	if err != nil {
		return 0, nil, err
	}

	kvs := make([]*server.KeyValue, 0, len(matches))
	for _, e := range matches {
		var nd natsData
		err = nd.Decode(e)
		if err != nil {
			return 0, nil, err
		}
		kvs = append(kvs, nd.KV)
	}

	storeRev := b.kv.BucketRevision()
	return storeRev, kvs, nil
}

func (b *Backend) Watch(ctx context.Context, prefix string, startRevision int64) server.WatchResult {
	events := make(chan []*server.Event, 32)

	rev := startRevision
	if rev == 0 {
		rev = b.kv.BucketRevision()
	}

	go func() {
		defer close(events)

		var w jetstream.KeyWatcher
		for {
			var err error
			w, err = b.kv.Watch(ctx, prefix, startRevision)
			if err == nil {
				break
			}
			b.l.Warnf("watch init: prefix=%s, err=%s", prefix, err)
			time.Sleep(time.Second)
		}

		for {
			select {
			case <-ctx.Done():
				err := ctx.Err()
				if err != nil && err != context.Canceled {
					b.l.Warnf("watch ctx: prefix=%s, err=%s", prefix, err)
				}
				return

			case e := <-w.Updates():
				if e.Operation() != jetstream.KeyValuePut {
					continue
				}

				key := e.Key()

				var nd natsData
				err := nd.Decode(e)
				if err != nil {
					b.l.Warnf("watch decode: key=%s, err=%s", key, err)
					continue
				}

				event := server.Event{
					Create: nd.Create,
					Delete: nd.Delete,
					KV:     nd.KV,
					PrevKV: &server.KeyValue{},
				}

				if nd.PrevRevision > 0 {
					_, pnd, err := b.get(ctx, key, nd.PrevRevision, false)
					if err == nil {
						event.PrevKV = pnd.KV
					}
				}

				events <- []*server.Event{&event}
			}
		}
	}()

	return server.WatchResult{
		Events:          events,
		CurrentRevision: rev,
	}
}

// Compact is a no-op / not implemented. Revision history is managed by the jetstream bucket.
func (b *Backend) Compact(ctx context.Context, revision int64) (int64, error) {
	return revision, nil
}
