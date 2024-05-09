package nats

import (
	"context"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

var (
	_ server.Backend = &BackendLogger{}
)

type BackendLogger struct {
	logger    *logrus.Logger
	backend   server.Backend
	threshold time.Duration
}

func (b *BackendLogger) logMethod(dur time.Duration, str string, args ...any) {
	if dur > b.threshold {
		b.logger.Warnf(str, args...)
	} else {
		b.logger.Tracef(str, args...)
	}
}

func (b *BackendLogger) Start(ctx context.Context) error {
	return b.backend.Start(ctx)
}

// Get returns the store's current revision, the associated server.KeyValue or an error.
func (b *BackendLogger) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (revRet int64, kvRet *server.KeyValue, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		size := 0
		if kvRet != nil {
			size = len(kvRet.Value)
		}
		fStr := "GET %s, rev=%d => revRet=%d, kv=%v, size=%d, err=%v, duration=%s"
		b.logMethod(dur, fStr, key, revision, revRet, kvRet != nil, size, errRet, dur)
	}()

	return b.backend.Get(ctx, key, rangeEnd, limit, revision)
}

// Create attempts to create the key-value entry and returns the revision number.
func (b *BackendLogger) Create(ctx context.Context, key string, value []byte, lease int64) (revRet int64, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "CREATE %s, size=%d, lease=%d => rev=%d, err=%v, duration=%s"
		b.logMethod(dur, fStr, key, len(value), lease, revRet, errRet, dur)
	}()

	return b.backend.Create(ctx, key, value, lease)
}

func (b *BackendLogger) Delete(ctx context.Context, key string, revision int64) (revRet int64, kvRet *server.KeyValue, deletedRet bool, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "DELETE %s, rev=%d => rev=%d, kv=%v, deleted=%v, err=%v, duration=%s"
		b.logMethod(dur, fStr, key, revision, revRet, kvRet != nil, deletedRet, errRet, dur)
	}()

	return b.backend.Delete(ctx, key, revision)
}

func (b *BackendLogger) List(ctx context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "LIST %s, start=%s, limit=%d, rev=%d => rev=%d, kvs=%d, err=%v, duration=%s"
		b.logMethod(dur, fStr, prefix, startKey, limit, revision, revRet, len(kvRet), errRet, dur)
	}()

	return b.backend.List(ctx, prefix, startKey, limit, revision)
}

// Count returns an exact count of the number of matching keys and the current revision of the database
func (b *BackendLogger) Count(ctx context.Context, prefix, startKey string, revision int64) (revRet int64, count int64, err error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		fStr := "COUNT %s, start=%s, rev=%d => rev=%d, count=%d, err=%v, duration=%s"
		b.logMethod(dur, fStr, prefix, startKey, revision, revRet, count, err, dur)
	}()

	return b.backend.Count(ctx, prefix, startKey, revision)
}

func (b *BackendLogger) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *server.KeyValue, updateRet bool, errRet error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		kvRev := int64(0)
		if kvRet != nil {
			kvRev = kvRet.ModRevision
		}
		fStr := "UPDATE %s, value=%d, rev=%d, lease=%v => rev=%d, kvrev=%d, updated=%v, err=%v, duration=%s"
		b.logMethod(dur, fStr, key, len(value), revision, lease, revRet, kvRev, updateRet, errRet, dur)
	}()

	return b.backend.Update(ctx, key, value, revision, lease)
}

func (b *BackendLogger) Watch(ctx context.Context, prefix string, revision int64) server.WatchResult {
	return b.backend.Watch(ctx, prefix, revision)
}

// DbSize get the kineBucket size from JetStream.
func (b *BackendLogger) DbSize(ctx context.Context) (int64, error) {
	return b.backend.DbSize(ctx)
}

// CurrentRevision returns the current revision of the database.
func (b *BackendLogger) CurrentRevision(ctx context.Context) (int64, error) {
	return b.backend.CurrentRevision(ctx)
}

// Compact is a no-op / not implemented. Revision history is managed by the jetstream bucket.
func (b *BackendLogger) Compact(ctx context.Context, revision int64) (int64, error) {
	return revision, nil
}
