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

	var nd natsData
	err = nd.Decode(entry)
	if err != nil {
		return 0, nil, err
	}

	if nd.Create && nd.KV != nil {
		nd.KV.CreateRevision = rev
		nd.KV.ModRevision = rev
	}

	if nd.Delete && !allowDeletes {
		return 0, nil, jetstream.ErrKeyNotFound
	}

	return rev, &nd, nil
}

// Start starts the backend.
// See https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
func (b *Backend) Start(ctx context.Context) error {
	b.ctx = ctx

	b.kv.Start(context.Background())
	b.kv.ew.Start(ctx)

	// Wait for btree watcher to finish initial replay before accepting operations
	// This prevents reads from seeing inconsistent state during startup
	b.l.Infof("Waiting for btree replay to complete...")
	if err := b.kv.waitReady(ctx); err != nil {
		return fmt.Errorf("failed to initialize btree: %w", err)
	}

	b.l.Infof("Creating health key...")

	/**
	b.l.Infof("Creating health key...")

	rev, err := b.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0)
	if err == nil {
		// Start automatic compaction if enabled
		if b.compactInterval > 0 {
			go b.compactor()
		} else {
			b.l.Infof("Automatic compaction disabled (interval: %v)", b.compactInterval)
		}
		go b.compactWatcher()
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
		go b.compactWatcher()
		return err
	}
	**/

	if b.compactInterval > 0 {
		go b.compactor()
	} else {
		b.l.Infof("Automatic compaction disabled (interval: %v)", b.compactInterval)
	}

	go b.compactWatcher()

	// Initialize operational keys
	rev, _ := b.Create(ctx, "/registry/health", []byte(""), 0)
	b.Update(ctx, "/registry/health", []byte(`{"health":"true"}`), rev, 0)
	// b.Create(ctx, compactRevAPI, []byte("0|0"), 0)

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

	b.l.Infof("Get: key=%s, rangeEnd=%s, limit=%d, rev=%d, keysOnly=%v", key, rangeEnd, limit, revision, keysOnly)

	rev, kvs, err := b.List(ctx, key, rangeEnd, limit, revision, keysOnly)
	if err != nil {
		return rev, nil, err
	}

	b.l.Infof("Get return: key=%s, rev=%d, count=%d", key, rev, len(kvs))

	if len(kvs) == 0 {
		return rev, nil, nil
	}

	return rev, kvs[0], nil
}

// Create attempts to create the key-value entry and returns the revision number.
func (b *Backend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	logrus.Infof("Create: key=%s, value=%d, lease=%d", key, len(value), lease)
	// Check if key exists already. If the entry exists even if marked as expired or deleted,
	// the revision will be returned to apply an update.
	rev, pnd, err := b.get(ctx, key, 0, true, true)

	// If an error other than key not found, return.
	if err != nil && err != jetstream.ErrKeyNotFound {
		return 0, err
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
		return 0, err
	}

	var seq uint64
	if pnd != nil {
		seq, err = b.kv.Update(ctx, key, data, uint64(rev))
		if err != nil {
			if jsWrongLastSeqErr.Is(err) {
				b.l.Debugf("update conflict: key=%s, rev=%d, err=%s (bad last sequence)", key, rev, err)
				return 0, server.ErrKeyExists
			}
			return 0, err
		}
	} else {
		seq, err = b.kv.Create(ctx, key, data)
		if err != nil {
			if jsWrongLastSeqErr.Is(err) {
				b.l.Warnf("create conflict: key=%s, rev=0, err=%s", key, err)
				return 0, server.ErrKeyExists
			}
			return 0, err
		}
	}

	if lease > 0 {
		b.kv.ew.Add(key, int64(seq), time.Now().Add(time.Second*time.Duration(lease)))
	}

	return int64(seq), nil
}

func (b *Backend) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	logrus.Infof("Delete: key=%s, revision=%d", key, revision)
	// Get the key, allow deletes.
	rev, pnd, err := b.get(ctx, key, 0, false, true)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
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

	logrus.Infof("Delete: updating with tombstone: key=%s, data=%s", key, string(data))
	// Update with a tombstone.
	drev, err := b.kv.Update(ctx, key, data, uint64(rev))
	if err != nil {
		if jsWrongLastSeqErr.Is(err) {
			b.l.Warnf("delete conflict: key=%s, rev=%d, err=%s", key, rev, err)

			rev, pnd, err = b.get(ctx, key, 0, false, true)

			var kv *server.KeyValue
			if pnd != nil {
				kv = pnd.KV
			}

			return rev, kv, false, err
		}
		return rev, pnd.KV, false, nil
	}

	logrus.Infof("Delete: deleting with revision: key=%s, revision=%d", key, drev)
	err = b.kv.Delete(ctx, key, jetstream.LastRevision(drev))
	if err != nil {
		if jsWrongLastSeqErr.Is(err) {
			b.l.Debugf("delete conflict: key=%s, rev=%d, err=%s", key, drev, err)
			return 0, nil, false, nil
		}
		return rev, pnd.KV, false, nil
	}

	return int64(drev), pnd.KV, true, nil
}

func (b *Backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	// Response and error flow modeled off of LogStructured Update function

	logrus.Debugf("Update: key=%s, revision=%d, lease=%d", key, revision, lease)

	// Get the latest revision of the key.
	rev, pnd, err := b.get(ctx, key, 0, false, true)
	if err != nil {
		logrus.Debugf("Update: key=%s, revision=%d, lease=%d, err=%v", key, revision, lease, err)
		if err == jetstream.ErrKeyNotFound {
			return 0, nil, false, nil
		}
		return 0, nil, false, err
	}

	logrus.Debugf("Update: key=%s, revision=%d, lease=%d, rev=%d, pnd=%v", key, revision, lease, rev, pnd)

	if pnd == nil {
		logrus.Debugf("Update: key=%s, revision=%d, lease=%d, pnd=nil", key, revision, lease)
		return 0, nil, false, nil
	}

	// Incorrect revision, return the current value.
	if pnd.KV.ModRevision != revision {
		b.l.Warnf("update revision conflict: key=%s, rev=%d, expected=%d", key, revision, pnd.KV.ModRevision)
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
	currentRev := b.kv.BucketRevision()
	b.l.Infof("List: prefix=%s, startKey=%s, limit=%d, maxRevision=%d, keysOnly=%v, currentRev=%d", prefix, startKey, limit, maxRevision, keysOnly, currentRev)

	matches, err := b.kv.List(ctx, prefix, startKey, limit, maxRevision, keysOnly)
	if err != nil {
		return b.kv.BucketRevision(), nil, err
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

	compactRev := b.kv.compactRev.Load()
	currentRev := b.kv.BucketRevision()

	b.l.Infof("Watch: prefix=%s, compactRev=%d, currentRev=%d, startRevision=%d", prefix, compactRev, currentRev, startRevision)

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
				w.Stop()
				goto outer

			case err := <-w.Err():
				b.l.Debugf("watch error: prefix=%s, err=%s", prefix, err)
				w.Stop()
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

				logrus.Infof("watch event: seq=%d, key=%s, create=%v, delete=%v, prevRevision=%v", e.Revision(), key, nd.Create, nd.Delete, nd.PrevRevision)

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

	/**
	s, err := b.kv.js.Stream(ctx, fmt.Sprintf("KV_%s", b.kv.nkv.Bucket()))
	if err != nil {
		return 0, err
	}

	err = s.Purge(ctx,
		jetstream.WithPurgeKeep(1),
		jetstream.WithPurgeSequence(uint64(revision)),
	)
	if err != nil {
		return 0, err
	}
		**/

	k, err := b.kv.getRevision(ctx, compactRevAPI, 0)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			v := server.EncodeVersion(1, []byte(strconv.FormatInt(0, 10)))
			if _, err := b.Create(ctx, compactRevAPI, v, 0); err != nil {
				return 0, err
			}
			k, err = b.kv.getRevision(ctx, compactRevAPI, 0)
			if err != nil {
				return 0, err
			}
		} else {
			return 0, err
		}
	}

	_, nd, err := b.get(ctx, compactRevAPI, int64(k.Revision()), false, true)
	if err != nil {
		return 0, err
	}

	compactVers, compactRev := decodeCompactValue(nd.KV.Value)

	b.l.Infof("Compact Current: compactVers=%d, compactRev=%d", compactVers, compactRev)

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

		b.l.Debugf("compact: compacting to version: %s from: %d", string(compactValue), compactRev)

		_, _, _, err := b.Update(ctx, compactRevAPI, compactValue, int64(nd.KV.ModRevision), 0)
		if err != nil {
			return currRev, err
		}
		b.l.Debugf("compact: currentRev: %d, compactRev: %d, targetCompactRev: %d, compactVers: %d", currRev, compactRev, targetCompactRev, compactVers)
	} else {
		b.l.Debugf("compact: no compaction performed: targetCompactRev: %d, oldCompact: %d", targetCompactRev, compactRev)
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
			b.l.Infof("Stopping automatic compaction")
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

				// The stream sequence should be at least two
				// sequences higher than the compact revision due to the PUT on the compact key,
				// otherwise this is a compact on a compact.
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
