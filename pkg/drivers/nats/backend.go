package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
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

// get returns the key-value entry for the given key and revision, if specified.
// This takes into account entries that have been marked as deleted or expired.
func (b *Backend) get(ctx context.Context, key string, revision int64, allowDeletes, checkRevision bool) (int64, *natsData, error) {
	entry, err := b.kv.GetRevision(ctx, key, revision, checkRevision)
	if err != nil {
		return b.kv.BucketRevision(), nil, err
	}

	rev := int64(entry.Revision())

	var nd natsData
	err = nd.Decode(entry)
	if err != nil {
		return b.kv.BucketRevision(), nil, err
	}

	if nd.Create && nd.KV != nil {
		nd.KV.CreateRevision = rev
		nd.KV.ModRevision = rev
	}

	if nd.Delete && !allowDeletes {
		return b.kv.BucketRevision(), nil, nil
	}

	return rev, &nd, nil
}

// Start starts the backend.
// See https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
func (b *Backend) Start(ctx context.Context) error {
	b.ctx = ctx

	b.kv.Start(b.ctx)
	b.kv.ew.Start(b.ctx)

	// Wait for btree watcher to finish initial replay before accepting operations
	// This prevents reads from seeing inconsistent state during startup
	b.l.Infof("Waiting for btree replay to complete...")
	if err := b.kv.waitReady(ctx); err != nil {
		return fmt.Errorf("failed to initialize btree: %w", err)
	}

	b.l.Infof("Creating health key...")

	rev, err := b.Create(ctx, "/registry/health", nil, 0)
	if err != nil && err != server.ErrKeyExists {
		b.l.Errorf("failed to create health key: %v", err)
		return err
	}

	// Perform an update to increment the revision.
	_, _, _, err = b.Update(ctx, "/registry/health", []byte(`{"health":"true"}`), rev, 0)
	if err != nil {
		return err
	}

	if b.compactInterval > 0 {
		go b.compactor()
	} else {
		b.l.Infof("Automatic compaction disabled (interval: %v)", b.compactInterval)
	}

	go b.compactWatcher()

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
		return b.kv.BucketRevision(), 0, err
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
// Mirrors etcd and other drivers by being a list call with a single return
func (b *Backend) Get(ctx context.Context, key, rangeEnd string, limit, revision int64, keysOnly bool) (int64, *server.KeyValue, error) {
	if strings.HasSuffix(key, "/") && rangeEnd == "" {
		key = key[:len(key)-1]
	}

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
	rev, pnd, err := b.get(ctx, key, 0, true, true)

	// If an error other than key not found, return.
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return b.kv.BucketRevision(), err
	}

	nd := natsData{
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

	if pnd != nil {
		if !pnd.Delete {
			return rev, server.ErrKeyExists
		}
		nd.PrevRevision = pnd.KV.ModRevision
	}

	data, err := nd.Encode()
	if err != nil {
		return b.kv.BucketRevision(), err
	}

	var seq uint64
	if pnd != nil {
		seq, err = b.kv.Update(ctx, key, data, uint64(rev))
		if err != nil {
			if jsWrongLastSeqErr.Is(err) {
				b.l.Debugf("update conflict: key=%s, rev=%d, err=%s (bad last sequence)", key, rev, err)
				return b.kv.BucketRevision(), server.ErrKeyExists
			}
			return b.kv.BucketRevision(), err
		}
	} else {
		seq, err = b.kv.Create(ctx, key, data)
		if err != nil {
			if jsWrongLastSeqErr.Is(err) {
				b.l.Debugf("create conflict: key=%s, rev=0, err=%s", key, err)
				return b.kv.BucketRevision(), server.ErrKeyExists
			}
			return b.kv.BucketRevision(), err
		}
	}

	if lease > 0 {
		b.kv.ew.Add(key, int64(seq), time.Now().Add(time.Second*time.Duration(lease)))
	}

	return int64(seq), nil
}

func (b *Backend) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	// Get the key, allow tombstones.
	rev, pnd, err := b.get(ctx, key, 0, false, true)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return rev, nil, false, nil
		}
		return rev, nil, false, err
	}

	if pnd == nil {
		return rev, nil, true, nil
	}

	if pnd.Delete {
		return rev, nil, false, nil
	}

	if revision != 0 && pnd.KV.ModRevision != revision {
		return rev, pnd.KV, false, nil
	}

	nd := natsData{
		Delete:       true,
		PrevRevision: rev,
		KV:           pnd.KV,
	}

	data, err := nd.Encode()
	if err != nil {
		return rev, nil, false, err
	}

	// Update with a tombstone.
	drev, err := b.kv.Update(ctx, key, data, uint64(rev))
	if err != nil {
		if jsWrongLastSeqErr.Is(err) {
			b.l.Debugf("delete conflict: key=%s, rev=%d, err=%s", key, rev, err)

			rev, pnd, err = b.get(ctx, key, 0, false, true)

			if errors.Is(err, jetstream.ErrKeyNotFound) {
				return rev, nil, false, nil
			}

			var kv *server.KeyValue
			if pnd != nil {
				kv = pnd.KV
			}

			return rev, kv, false, err
		}
		return rev, pnd.KV, false, nil
	}

	err = b.kv.Delete(ctx, key, jetstream.LastRevision(drev))
	if err != nil {
		if jsWrongLastSeqErr.Is(err) {
			b.l.Debugf("delete conflict: key=%s, rev=%d, err=%s", key, drev, err)
			return b.kv.BucketRevision(), nil, false, nil
		}
		return rev, pnd.KV, false, nil
	}

	return int64(drev), pnd.KV, true, nil
}

func (b *Backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	// Response and error flow modeled off of LogStructured Update function

	// Get the latest revision of the key.
	rev, pnd, err := b.get(ctx, key, 0, false, true)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return b.kv.BucketRevision(), nil, false, nil
		}
		return b.kv.BucketRevision(), nil, false, err
	}

	if pnd == nil {
		return b.kv.BucketRevision(), nil, false, nil
	}

	// Incorrect revision, return the current value.
	if pnd.KV.ModRevision != revision {
		b.l.Debugf("update revision conflict: key=%s, rev=%d, expected=%d", key, revision, pnd.KV.ModRevision)
		return rev, pnd.KV, false, nil
	}

	nv := natsData{
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
		nv.KV.CreateRevision = rev
	}

	data, err := nv.Encode()
	if err != nil {
		return b.kv.BucketRevision(), nil, false, err
	}

	seq, err := b.kv.Update(ctx, key, data, uint64(revision))
	if err != nil {
		// This may occur if a concurrent writer created the key.
		if jsWrongLastSeqErr.Is(err) {
			b.l.Debugf("update conflict: key=%s, rev=%d, err=%s", key, revision, err)

			rev, pnd, err := b.get(ctx, key, 0, false, true)

			if errors.Is(err, jetstream.ErrKeyNotFound) {
				return b.kv.BucketRevision(), nil, false, nil
			}

			var kv *server.KeyValue
			if pnd != nil {
				kv = pnd.KV
			}

			return rev, kv, false, err
		}

		return b.kv.BucketRevision(), nil, false, err
	}

	nv.KV.ModRevision = int64(seq)

	if lease > 0 {
		b.kv.ew.Add(key, int64(seq), time.Now().Add(time.Second*time.Duration(lease)))
	}

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
		return b.kv.BucketRevision(), nil, err
	}

	kvs := make([]*server.KeyValue, 0, len(matches))
	for _, e := range matches {
		var nd natsData
		err = nd.Decode(e)
		if err != nil {
			return b.kv.BucketRevision(), nil, err
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

	if startRevision > 0 && startRevision <= b.kv.compactRev.Load() {
		return server.WatchResult{
			Events:          events,
			CurrentRevision: b.kv.BucketRevision(),
			CompactRevision: b.kv.compactRev.Load(),
		}
	}

	go func() {
		defer close(events)

		// Loop to re-establish the watch if it fails.
		var w KeyWatcher
	outer:
		for {
			var err error
			w, err = b.kv.Watch(ctx, prefix, startRevision)
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
				err = w.Stop()
				if err != nil {
					b.l.Debugf("watch stop: prefix=%s, err=%s", prefix, err)
				}
				goto outer

			case err := <-w.Err():
				b.l.Debugf("watch error: prefix=%s, err=%s", prefix, err)
				err = w.Stop()
				if err != nil {
					b.l.Debugf("watch stop: prefix=%s, err=%s", prefix, err)
				}
				goto outer

			case e := <-w.Updates():
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
	b.l.Debugf("compact: compacting to revision: %d", revision)
	currRev := b.kv.BucketRevision()

	k, err := b.kv.getRevision(ctx, compactRevAPI, 0)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			v := server.EncodeVersion(1, []byte(strconv.FormatInt(0, 10)))
			if _, err := b.Create(ctx, compactRevAPI, v, 0); err != nil {
				return b.kv.BucketRevision(), err
			}
			k, err = b.kv.getRevision(ctx, compactRevAPI, 0)
			if err != nil {
				return b.kv.BucketRevision(), err
			}
		} else {
			return b.kv.BucketRevision(), err
		}
	}

	_, nd, err := b.get(ctx, compactRevAPI, int64(k.Revision()), false, true)
	if err != nil {
		return b.kv.BucketRevision(), err
	}

	compactVers, compactRev := decodeCompactValue(nd.KV.Value)

	// Calculate target compact revision: currentRev - minRetain
	targetCompactRev := currRev - b.compactMinRetain

	if revision > 1 {
		targetCompactRev = revision
	}

	if targetCompactRev < 0 {
		targetCompactRev = 0
	}

	// Don't just compact after a compact
	if targetCompactRev > compactRev+1 {
		compactValue := server.EncodeVersion(compactVers+1, []byte(strconv.FormatInt(targetCompactRev, 10)))

		_, _, _, err := b.Update(ctx, compactRevAPI, compactValue, int64(nd.KV.ModRevision), 0)
		if err != nil {
			return currRev, err
		}
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

	b.l.Infof("Starting automatic compaction (interval: %v, minimum retention: %d)", b.compactInterval, b.compactMinRetain)

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
		}
	}
}

func (b *Backend) compactWatcher() {
	b.l.Infof("Starting compaction watcher")

	w, err := b.kv.Watch(b.ctx, compactRevAPI, 0)
	if err != nil {
		b.l.Errorf("Failed to configure watch for compact revision: %v", err)
	}
	defer w.Stop()

	for {
		select {
		case <-b.ctx.Done():
			b.l.Infof("Stopping compaction watcher")
			return
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

				if rev > 0 {
					swapped := b.kv.compactRev.CompareAndSwap(old, rev)
					b.l.Debugf("compact revision updated: old=%d, new=%d, swapped=%v", old, rev, swapped)
				}
			}
		}
	}
}

func decodeCompactValue(value []byte) (int64, int64) {
	vers, revB := server.DecodeVersion(value)
	rev, _ := strconv.ParseInt(string(revB), 10, 64)
	return vers, rev
}

func (b *Backend) Health(ctx context.Context) error {
	_, err := b.kv.nkv.Status(ctx)

	return err
}
