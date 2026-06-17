package t4

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	kserver "github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/ttl"
	"github.com/sirupsen/logrus"
	"github.com/t4db/t4"
)

// backend implements kine's server.Backend using a *t4.Node.
type backend struct {
	node *t4.Node
}

// Start blocks until the t4 node is ready to serve writes, retrying
// with backoff for up to 60 seconds. This guards against kine's health-check
// write firing before a large S3 checkpoint restore has finished.
//
// On first start it also seeds the health key with a Create+Update so the
// initial currentRevision matches what the memory and nats drivers expose
// (rev=2 after bootstrap). The k8s apiserver storage tests assume this
// shape.
func (b *backend) Start(ctx context.Context) error {
	const (
		retryInterval = 500 * time.Millisecond
		retryTimeout  = 60 * time.Second
	)
	deadline := time.Now().Add(retryTimeout)
	for {
		_, err := b.node.Create(ctx, kserver.HealthKey, []byte(kserver.HealthVal), 0)
		if err == nil || errors.Is(err, t4.ErrKeyExists) {
			if err = b.refreshHealthKey(ctx); err == nil {
				go ttl.Run(ctx, b)
				return nil
			}
		}
		if errors.Is(err, t4.ErrClosed) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("t4: backend not ready after %s: %w", retryTimeout, err)
		}
		logrus.Warnf("t4: backend not yet ready (%v), retrying...", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryInterval):
		}
	}
}

func (b *backend) refreshHealthKey(ctx context.Context) error {
	kv, err := b.node.LinearizableGet(ctx, kserver.HealthKey)
	if err != nil {
		return err
	}
	if kv == nil {
		return errors.New("health key disappeared")
	}
	_, _, _, err = b.node.Update(ctx, kserver.HealthKey, []byte(kserver.HealthVal), kv.Revision, 0)
	return err
}

func (b *backend) CurrentRevision(_ context.Context) (int64, error) {
	return b.node.CurrentRevision(), nil
}

func (b *backend) Get(ctx context.Context, key string, revision int64, keysOnly bool) (int64, *kserver.KeyValue, error) {
	curRev := b.node.CurrentRevision()
	if revision > 0 && revision > curRev {
		return curRev, nil, kserver.ErrFutureRev
	}
	if revision > 0 && revision < b.node.CompactRevision() {
		return curRev, nil, kserver.ErrCompacted
	}

	kv, err := b.node.LinearizableGet(ctx, key, readOpts(revision)...)
	if err != nil {
		return curRev, nil, translateErr(err)
	}
	return curRev, toServerKV(kv, keysOnly), nil
}

// readOpts builds the ReadOption slice for a historical read. revision <= 0
// returns no options (HEAD read).
func readOpts(revision int64) []t4.ReadOption {
	if revision <= 0 {
		return nil
	}
	return []t4.ReadOption{t4.WithRevision(revision)}
}

// translateErr maps t4-internal errors to the kine/etcd sentinel errors that
// apiserver understands and retries appropriately.
func translateErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, t4.ErrNoLeader):
		return kserver.ErrNoLeader
	case errors.Is(err, t4.ErrCompacted):
		return kserver.ErrCompacted
	case errors.Is(err, t4.ErrFutureRevision):
		return kserver.ErrFutureRev
	default:
		return err
	}
}

func (b *backend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	rev, err := b.node.Create(ctx, key, value, lease)
	if errors.Is(err, t4.ErrKeyExists) {
		return 0, kserver.ErrKeyExists
	}
	return rev, translateErr(err)
}

func (b *backend) Delete(ctx context.Context, key string, revision int64) (int64, *kserver.KeyValue, bool, error) {
	newRev, oldKV, deleted, err := b.node.DeleteIfRevision(ctx, key, revision)
	if err != nil {
		return 0, nil, false, translateErr(err)
	}
	return newRev, toServerKV(oldKV, false), deleted, nil
}

func (b *backend) List(ctx context.Context, key, end string, limit, revision int64, keysOnly bool) (int64, []*kserver.KeyValue, error) {
	curRev := b.node.CurrentRevision()
	if revision > 0 && revision > curRev {
		return curRev, nil, kserver.ErrFutureRev
	}
	if revision > 0 && revision < b.node.CompactRevision() {
		return curRev, nil, kserver.ErrCompacted
	}
	prefix, startKey := translateRange(key, end)
	opts := readOpts(revision)
	if startKey != "" {
		opts = append(opts, t4.WithFromKey(startKey))
	}
	if limit > 0 {
		opts = append(opts, t4.WithLimit(limit))
	}
	kvs, err := b.node.LinearizableList(ctx, prefix, opts...)
	if err != nil {
		return curRev, nil, translateErr(err)
	}
	out := make([]*kserver.KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, toServerKV(kv, keysOnly))
	}
	return curRev, out, nil
}

func (b *backend) Count(ctx context.Context, key, end string, revision int64) (int64, int64, error) {
	curRev := b.node.CurrentRevision()
	if revision > 0 && revision > curRev {
		return curRev, 0, kserver.ErrFutureRev
	}
	if revision > 0 && revision < b.node.CompactRevision() {
		return curRev, 0, kserver.ErrCompacted
	}
	prefix, startKey := translateRange(key, end)
	opts := readOpts(revision)
	if key != "" {
		opts = append(opts, t4.WithFromKey(startKey))
	}
	count, err := b.node.LinearizableCount(ctx, prefix, opts...)
	if err != nil {
		return curRev, 0, translateErr(err)
	}
	return curRev, count, nil
}

func (b *backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *kserver.KeyValue, bool, error) {
	newRev, oldKV, updated, err := b.node.Update(ctx, key, value, revision, lease)
	if err != nil {
		return 0, nil, false, translateErr(err)
	}
	return newRev, toServerKV(oldKV, false), updated, nil
}

func (b *backend) Watch(ctx context.Context, key, end string, revision int64) kserver.WatchResult {
	curRev := b.node.CurrentRevision()
	compactRev := b.node.CompactRevision()

	errCh := make(chan error, 1)
	eventCh := make(chan []*kserver.Event, 64)

	if revision > 0 && revision <= compactRev {
		errCh <- kserver.ErrCompacted
		close(errCh)
		close(eventCh)
		return kserver.WatchResult{CurrentRevision: curRev, CompactRevision: compactRev, Events: eventCh, Errorc: errCh}
	}

	prefix, _ := translateRange(key, end)
	go func() {
		defer close(eventCh)
		defer close(errCh)
		// PrevKV is required: toServerEvent classifies an event as Create if
		// ev.PrevKV == nil. Without it, every update is reported as a Create
		// and apiserver's watchCache rejects them as duplicate/out-of-order.
		ch, err := b.node.Watch(ctx, prefix, revision, t4.WithPrevKV())
		if err != nil {
			errCh <- translateErr(err)
			return
		}
		// Coalesce events that are already buffered into a single slice send.
		// kine's server-side already batches, but per-send overhead matters
		// under churn — and a small batch saves a chan op per event.
		// Soft cap aligned with the upstream t4.Node.Watch buffer; the drain
		// loop only takes immediately available events.
		const maxBatch = 64
		for ev := range ch {
			batch := []*kserver.Event{toServerEvent(&ev)}
		drain:
			for len(batch) < maxBatch {
				select {
				case ev2, ok := <-ch:
					if !ok {
						break drain
					}
					batch = append(batch, toServerEvent(&ev2))
				default:
					break drain
				}
			}
			select {
			case eventCh <- batch:
			case <-ctx.Done():
				return
			}
		}
	}()

	return kserver.WatchResult{CurrentRevision: curRev, CompactRevision: 0, Events: eventCh, Errorc: errCh}
}

func (b *backend) DbSize(_ context.Context) (int64, error) {
	dbDir := filepath.Join(b.node.Config().DataDir, "db")
	var total int64
	err := filepath.Walk(dbDir, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func (b *backend) Compact(ctx context.Context, revision int64) (int64, error) {
	if err := b.node.Compact(ctx, revision); err != nil {
		return 0, translateErr(err)
	}
	return revision, nil
}

func (b *backend) WaitForSyncTo(revision int64) {
	_ = b.node.WaitForRevision(context.Background(), revision)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func toServerKV(kv *t4.KeyValue, keysOnly bool) *kserver.KeyValue {
	if kv == nil {
		return nil
	}
	skv := &kserver.KeyValue{
		Key:            kv.Key,
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.Revision,
		Lease:          kv.Lease,
	}
	if !keysOnly {
		skv.Value = kv.Value
	}
	return skv
}

func toServerEvent(ev *t4.Event) *kserver.Event {
	se := &kserver.Event{
		Delete: ev.Type == t4.EventDelete,
		Create: ev.Type == t4.EventPut && ev.PrevKV == nil,
		KV:     toServerKV(ev.KV, false),
	}
	if ev.PrevKV != nil {
		se.PrevKV = toServerKV(ev.PrevKV, false)
	}
	return se
}

func translateRange(key, end string) (prefix, startKey string) {
	if end != "" && len(key) > len(end) {
		return key[:len(end)], key
	}
	return key, ""
}
