package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

const (
	compactRevAPI     = "compact_rev_key_apiserver"
	waitForSeqTimeout = 5 * time.Second
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

// Ensure Backend implements server.Backend.
var _ server.Backend = (&Backend{})

type Backend struct {
	kv               *KeyValue
	l                *logrus.Logger
	compactInterval  time.Duration
	compactMinRetain int64
	ctx              context.Context
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
func (b *Backend) get(ctx context.Context, key string, revision int64, allowDeletes, checkRevision bool) (int64, *natsData, error) {
	entry, err := b.kv.GetRevision(ctx, key, revision, checkRevision)
	if err != nil {
		return 0, nil, err
	}

	rev := int64(entry.Revision())

	var val natsData
	err = val.Decode(entry)
	if err != nil {
		return 0, nil, err
	}

	if val.Create && val.KV != nil {
		val.KV.CreateRevision = rev
		val.KV.ModRevision = rev
	}

	if (val.Delete && !allowDeletes) || b.isExpiredKey(&val) {
		return 0, nil, jetstream.ErrKeyNotFound
	}

	return rev, &val, nil
}

// Start starts the backend.
// See https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
func (b *Backend) Start(ctx context.Context) error {
	b.ctx = ctx

	// Wait for btree watcher to finish initial replay before accepting operations
	// This prevents reads from seeing inconsistent state during startup
	b.l.Infof("Waiting for btree replay to complete...")
	if err := b.kv.waitReady(ctx); err != nil {
		return fmt.Errorf("failed to initialize btree: %w", err)
	}

	b.l.Infof("Creating health key...")

	rev, err := b.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0)
	if err == nil {
		// Start automatic compaction if enabled
		if b.compactInterval > 0 {
			go b.compactor()
		} else {
			b.l.Infof("Automatic compaction disabled (interval: %v)", b.compactInterval)
		}
		return nil
	}

	// Already exists, perform an update to increment the revision.
	if err == server.ErrKeyExists {
		_, _, _, err = b.Update(ctx, "/registry/health", []byte(`{"health":"true"}`), rev, 0)
		if err == nil {
			// Start automatic compaction if enabled
			if b.compactInterval > 0 {
				go b.compactor()
			} else {
				b.l.Infof("Automatic compaction disabled (interval: %v)", b.compactInterval)
			}
		}
		return err
	}

	return err
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

	var rev int64
	if revision > 0 {
		rev = revision
	} else {
		rev = b.kv.BucketRevision()
	}

	return rev, count, nil
}

// Get returns the store's current revision, the associated server.KeyValue or an error.
// Like SQL backends, this is implemented as a List operation that returns the first result.
// The rangeEnd parameter allows for range queries (prefix matching).
func (b *Backend) Get(ctx context.Context, key, rangeEnd string, limit, revision int64, keysOnly bool) (int64, *server.KeyValue, error) {
	// SQL backends implement Get as List with the first result returned
	// This handles both exact key lookups and range queries uniformly
	rev, kvs, err := b.List(ctx, key, rangeEnd, limit, revision, keysOnly)
	if err != nil {
		return rev, nil, err
	}

	if len(kvs) == 0 {
		return rev, nil, nil
	}

	return rev, kvs[0], nil
}

// Create attempts to create the key-value entry and returns the revision number.
func (b *Backend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	// Check if key exists already. If the entry exists even if marked as expired or deleted,
	// the revision will be returned to apply an update.
	rev, pnv, err := b.get(ctx, key, 0, true, true)

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

	var seq uint64
	if pnv != nil {
		seq, err = b.kv.Update(ctx, key, data, uint64(rev))
		if err != nil {
			if jsWrongLastSeqErr.Is(err) {
				b.l.Debugf("create conflict: key=%s, rev=%d, err=%s", key, rev, err)
				return 0, server.ErrKeyExists
			}
			return 0, err
		}
	} else {
		seq, err = b.kv.Create(ctx, key, data)
		if err != nil {
			if jsWrongLastSeqErr.Is(err) {
				b.l.Debugf("create conflict: key=%s, rev=0, err=%s", key, err)
				return 0, server.ErrKeyExists
			}
			return 0, err
		}
	}

	// Wait for btree watcher to process this sequence for read-after-write consistency
	if err := b.kv.waitForSequence(ctx, seq, waitForSeqTimeout); err != nil {
		b.l.Warnf("create: btree watcher lag: key=%s, seq=%d, err=%v", key, seq, err)
		// Continue anyway - data is in NATS, btree is lagging
	}

	return int64(seq), nil
}

func (b *Backend) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	// Get the key, allow deletes.
	rev, pnv, err := b.get(ctx, key, 0, true, true)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return rev, nil, true, nil
		}
		return rev, nil, false, err
	}

	if pnv == nil {
		return rev, nil, true, nil
	}

	if pnv.Delete {
		return rev, pnv.KV, true, nil
	}

	if revision != 0 && pnv.KV.ModRevision != revision {
		return rev, pnv.KV, false, nil
	}

	nv := natsData{
		Delete:       true,
		PrevRevision: rev,
		KV:           pnv.KV,
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

			rev, pnd, err := b.get(ctx, key, 0, false, true)

			var kv *server.KeyValue
			if pnd != nil {
				kv = pnd.KV
			}

			return rev, kv, false, err
		}
		return rev, pnv.KV, false, nil
	}

	// Wait for btree watcher to process the tombstone
	if err := b.kv.waitForSequence(ctx, drev, waitForSeqTimeout); err != nil {
		b.l.Warnf("delete: btree watcher lag (tombstone): key=%s, seq=%d, err=%v", key, drev, err)
		// Continue anyway - data is in NATS, just btree is lagging
	}

	err = b.kv.Delete(ctx, key, jetstream.LastRevision(drev))
	if err != nil {
		if jsWrongLastSeqErr.Is(err) {
			b.l.Debugf("delete conflict: key=%s, rev=%d, err=%s", key, drev, err)
			return 0, nil, false, nil
		}
		return rev, pnv.KV, false, nil
	}

	return int64(drev), pnv.KV, true, nil
}

func (b *Backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	// Get the latest revision of the key.
	rev, pnv, err := b.get(ctx, key, 0, false, true)
	// TODO: correct semantics for these various errors?
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return rev, nil, false, nil
		}
		return rev, nil, false, err
	}

	// Return nothing?
	if pnv == nil {
		b.l.Warnf("update: key=%s, rev=%d, pnv=nil", key, revision)
		return 0, nil, false, nil
	}

	// Incorrect revision, return the current value.
	if pnv.KV.ModRevision != revision {
		return rev, pnv.KV, false, nil
	}

	nv := natsData{
		Delete:       false,
		Create:       false,
		PrevRevision: pnv.KV.ModRevision,
		KV: &server.KeyValue{
			Key:            key,
			CreateRevision: pnv.KV.CreateRevision,
			Value:          value,
			Lease:          lease,
		},
	}

	if pnv.KV.CreateRevision == 0 {
		nv.KV.CreateRevision = rev
	}

	data, err := nv.Encode()
	if err != nil {
		return 0, nil, false, err
	}

	seq, err := b.kv.Update(ctx, key, data, uint64(revision))
	if err != nil {
		// This may occur if a concurrent writer created the key.
		if jsWrongLastSeqErr.Is(err) {
			b.l.Warnf("update conflict: key=%s, rev=%d, err=%s", key, revision, err)

			rev, pnd, err := b.get(ctx, key, 0, false, true)

			var kv *server.KeyValue
			if pnd != nil {
				kv = pnd.KV
			}

			return rev, kv, false, err
		}
		return 0, nil, false, err
	}

	// Wait for btree watcher to process this sequence for read-after-write consistency
	if err := b.kv.waitForSequence(ctx, seq, waitForSeqTimeout); err != nil {
		b.l.Warnf("update: btree watcher lag: key=%s, seq=%d, err=%v", key, seq, err)
		// Continue anyway - data is in NATS, just btree is lagging
	}

	nv.KV.ModRevision = int64(seq)

	return int64(seq), nv.KV, true, nil
}

// List returns a range of keys starting with the prefix.
// This would translated to one or more tokens, e.g. `a.b.c`.
// The startKey would be the next set of tokens that follow the prefix
// that are alphanumerically equal to or greater than the startKey.
// If limit is provided, the maximum set of matches is limited.
// If revision is provided, this indicates the maximum revision to return.
func (b *Backend) List(ctx context.Context, prefix, startKey string, limit, maxRevision int64, keysOnly bool) (int64, []*server.KeyValue, error) {
	matches, err := b.kv.List(ctx, prefix, startKey, limit, maxRevision, keysOnly)
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

	var rev int64
	if maxRevision > 0 {
		rev = maxRevision
	} else {
		rev = b.kv.BucketRevision()
	}

	return rev, kvs, nil
}

func (b *Backend) Watch(ctx context.Context, prefix string, startRevision int64) server.WatchResult {
	events := make(chan []*server.Event, 32)

	go func() {
		defer close(events)
		lastRevision := startRevision

		// Loop to re-establish the watch if it fails.
		var w KeyWatcher
	outer:
		for {
			var err error
			w, err = b.kv.Watch(ctx, prefix, lastRevision)
			if err == nil {
				break
			} else if errors.Is(err, context.Canceled) || errors.Is(err, nats.ErrConnectionClosed) {
				return
			}

			b.l.Warnf("watch init: prefix=%s, err=%s", prefix, err)
			time.Sleep(time.Second)
		}
		defer w.Stop()

		for {
			select {
			case <-ctx.Done():
				err := ctx.Err()
				if err == nil || errors.Is(err, context.Canceled) {
					return
				}
				b.l.Debugf("watch ctx: prefix=%s, err=%s", prefix, err)
				w.Stop()
				goto outer

			case err := <-w.Err():
				b.l.Debugf("watch error: prefix=%s, err=%s", prefix, err)
				w.Stop()
				goto outer

			case e := <-w.Updates():
				lastRevision = int64(e.Revision())

				if e.Operation() != jetstream.KeyValuePut {
					continue
				}

				key := e.Key()

				var nd natsData
				err := nd.Decode(e)
				if err != nil {
					b.l.Debugf("watch decode: key=%s, err=%s", key, err)
					continue
				}

				event := server.Event{
					Create: nd.Create,
					Delete: nd.Delete,
					KV:     nd.KV,
					PrevKV: &server.KeyValue{
						ModRevision: nd.PrevRevision,
					},
				}

				if nd.PrevRevision > 0 {
					_, pnd, err := b.get(ctx, key, nd.PrevRevision, false, false)
					if err == nil && pnd != nil {
						event.PrevKV = pnd.KV
					}
				}

				events <- []*server.Event{&event}
			}
		}
	}()

	rev := startRevision
	if rev == 0 {
		rev = b.kv.BucketRevision()
	}

	return server.WatchResult{
		Events:          events,
		CurrentRevision: rev,
	}
}

// Compact is a no-op / not implemented. Revision history is managed by the jetstream bucket.
func (b *Backend) Compact(ctx context.Context, revision int64) (int64, error) {
	currRev := b.kv.BucketRevision()

	k, err := b.kv.getRevision(ctx, compactRevAPI, 0)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			v := server.EncodeVersion(1, []byte(strconv.FormatInt(0, 10)))
			if _, err := b.Create(ctx, compactRevAPI, v, 0); err != nil {
				return 0, err
			}
			_, err = b.kv.getRevision(ctx, compactRevAPI, 0)
			if err != nil {
				return 0, err
			}
		} else {
			return 0, err
		}
	}

	_, nd, err := b.get(ctx, compactRevAPI, int64(k.Revision()), false, true)
	// _, nd, err := b.get(ctx, compactRevAPI, 0, false, false)
	if err != nil {
		return 0, err
	}

	compactVers, compactRev := decodeCompactValue(nd.KV.Value)

	// Calculate target compact revision: currentRev - minRetain
	targetCompactRev := currRev - b.compactMinRetain

	if revision > 0 && revision < targetCompactRev {
		targetCompactRev = revision
	}

	if targetCompactRev < 0 {
		targetCompactRev = 0
	}

	if targetCompactRev > compactRev {
		compactValue := server.EncodeVersion(compactVers+1, []byte(strconv.FormatInt(targetCompactRev, 10)))

		b.l.Debugf("Compacting to version: %s", string(compactValue))

		_, _, _, err := b.Update(ctx, compactRevAPI, compactValue, int64(nd.KV.ModRevision), 0)
		if err != nil {
			return currRev, err
		}

		b.kv.compactRev.Store(targetCompactRev)
	} else {
		b.l.Debugf("No compaction performed: targetCompactRev: %d, oldCompact: %d", targetCompactRev, compactRev)
	}

	return currRev, nil
}

// compactor runs periodic automatic compaction in the background.
// This advances the compact revision point,
// causing queries for old revisions to return ErrCompacted.
// Required for passing conformance testing.
func (b *Backend) compactor() {
	t := time.NewTicker(b.compactInterval)
	defer t.Stop()

	b.l.Infof("Starting automatic compaction (interval: %v, minRetain: %d)", b.compactInterval, b.compactMinRetain)

	w, err := b.kv.nkv.Watch(b.ctx, compactRevAPI)
	if err != nil {
		b.l.Errorf("Failed to configure watch for compact revision: %v", err)
	}
	defer w.Stop()

	_, err = b.Compact(b.ctx, b.kv.BucketRevision())
	if err != nil {
		b.l.Errorf("Automatic compaction failed: %v", err)
	}

	for {
		select {
		case <-b.ctx.Done():
			b.l.Infof("Stopping automatic compaction")
			return
		case <-t.C:
			_, err := b.Compact(b.ctx, b.kv.BucketRevision())
			if err != nil {
				b.l.Errorf("Automatic compaction failed: %v", err)
			}
		case e := <-w.Updates():
			if e == nil {
				b.l.Warnf("compact revision update empty")
				continue
			}

			if e.Operation() != jetstream.KeyValuePut {
				continue
			}

			var nd natsData
			err = nd.Decode(e)
			if err != nil {
				b.l.Errorf("Failed to decode compact revision update: %v", err)
				continue
			}

			if kv := nd.KV; kv != nil {
				_, rev := decodeCompactValue(kv.Value)
				old := b.kv.compactRev.Load()
				b.kv.compactRev.CompareAndSwap(old, rev)
			}
		}
	}
}

func decodeCompactValue(value []byte) (int64, int64) {
	vers, revB := server.DecodeVersion(value)
	rev, _ := strconv.ParseInt(string(revB), 10, 64)
	return vers, rev
}
