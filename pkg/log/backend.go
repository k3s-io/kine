package log

import (
	"context"
	"time"

	"github.com/rancher/kine/pkg/backend"
	"github.com/sirupsen/logrus"
)

type Log interface {
	CurrentRevision(ctx context.Context) (int64, error)
	List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeletes bool) (int64, []*backend.Event, error)
	After(ctx context.Context, prefix string, revision int64) (int64, []*backend.Event, error)
	Watch(ctx context.Context, prefix string) <-chan []*backend.Event
	Count(ctx context.Context, prefix string) (int64, int64, error)
	Append(ctx context.Context, event *backend.Event) (int64, error)
}

type Backend struct {
	log Log
}

func New(log Log) *Backend {
	return &Backend{
		log: log,
	}
}

func (b *Backend) Get(ctx context.Context, key string, revision int64) (revRet int64, kvRet *backend.KeyValue, errRet error) {
	defer func() {
		b.adjustRevision(ctx, &revRet)
		logrus.Debugf("GET %s, rev=%d => rev=%d, kv=%v, err=%v", key, revision, revRet, kvRet != nil, errRet)
	}()

	rev, event, err := b.get(ctx, key, revision, false)
	if event == nil {
		return rev, nil, err
	}
	return rev, event.KV, err
}

func (b *Backend) get(ctx context.Context, key string, revision int64, includeDeletes bool) (int64, *backend.Event, error) {
	rev, events, err := b.log.List(ctx, key, "", 1, revision, includeDeletes)
	if err != nil {
		return 0, nil, err
	}
	if revision != 0 {
		rev = revision
	}
	if len(events) == 0 {
		return rev, nil, nil
	}
	return rev, events[0], nil
}

func (b *Backend) adjustRevision(ctx context.Context, rev *int64) {
	if *rev != 0 {
		return
	}

	if newRev, err := b.log.CurrentRevision(ctx); err == nil {
		*rev = newRev
	}
}

func (b *Backend) Create(ctx context.Context, key string, value []byte, lease int64) (revRet int64, errRet error) {
	defer func() {
		b.adjustRevision(ctx, &revRet)
		logrus.Debugf("CREATE %s, size=%d, lease=%d => rev=%d, err=%v", key, len(value), lease, revRet, errRet)
	}()

	rev, prevEvent, err := b.get(ctx, key, 0, true)
	if err != nil {
		return 0, err
	}
	createEvent := &backend.Event{
		Create: true,
		KV: &backend.KeyValue{
			Key:   key,
			Value: value,
			Lease: lease,
		},
		PrevKV: &backend.KeyValue{
			ModRevision: rev,
		},
	}
	if prevEvent != nil {
		if !prevEvent.Delete {
			return 0, backend.ErrKeyExists
		}
		createEvent.PrevKV = prevEvent.KV
	}

	return b.log.Append(ctx, createEvent)
}

func (b *Backend) Delete(ctx context.Context, key string, revision int64) (revRet int64, kvRet *backend.KeyValue, deletedRet bool, errRet error) {
	defer func() {
		b.adjustRevision(ctx, &revRet)
		logrus.Debugf("DELETE %s, rev=%d => rev=%d, kv=%v, deleted=%v, err=%v", key, revision, revRet, kvRet != nil, deletedRet, errRet)
	}()

	rev, event, err := b.get(ctx, key, revision, true)
	if err != nil {
		return 0, nil, false, err
	}

	if event == nil || event.Delete {
		return rev, nil, true, nil
	}

	if revision != 0 && event.KV.ModRevision != revision {
		return rev, event.KV, false, nil
	}

	deleteEvent := &backend.Event{
		Delete: true,
		KV:     event.KV,
		PrevKV: event.KV,
	}

	rev, err = b.log.Append(ctx, deleteEvent)
	return rev, event.KV, true, err
}

func (b *Backend) List(ctx context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*backend.KeyValue, errRet error) {
	defer func() {
		b.adjustRevision(ctx, &revRet)
		logrus.Debugf("LIST %s, start=%s, limit=%d, rev=%d => rev=%d, kvs=%d, err=%v", prefix, startKey, limit, revision, revRet, len(kvRet), errRet)
	}()

	rev, events, err := b.log.List(ctx, prefix, startKey, limit, revision, false)
	if err != nil {
		return 0, nil, err
	}
	if revision != 0 {
		rev = revision
	}

	kvs := make([]*backend.KeyValue, 0, len(events))
	for _, event := range events {
		kvs = append(kvs, event.KV)
	}
	return rev, kvs, nil
}

func (b *Backend) Count(ctx context.Context, prefix string) (revRet int64, count int64, err error) {
	defer func() {
		b.adjustRevision(ctx, &revRet)
		logrus.Debugf("COUNT %s => rev=%d, count=%d, err=%v", prefix, revRet, count, err)
	}()
	return b.log.Count(ctx, prefix)
}

func (b *Backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *backend.KeyValue, updateRet bool, errRet error) {
	defer func() {
		b.adjustRevision(ctx, &revRet)
		logrus.Debugf("UPDATE %s, value=%d, rev=%d, lease=%v => rev=%d, kv=%v, updated=%v, err=%v", key, len(value), revision, lease, revRet, kvRet != nil, updateRet, errRet)
	}()

	rev, event, err := b.get(ctx, key, revision, false)
	if err != nil {
		return 0, nil, false, err
	}

	if event == nil {
		return 0, nil, false, nil
	}

	if event.KV.ModRevision != revision {
		return rev, event.KV, false, nil
	}

	updateEvent := &backend.Event{
		KV: &backend.KeyValue{
			Key:            key,
			CreateRevision: event.KV.CreateRevision,
			Value:          value,
			Lease:          lease,
		},
		PrevKV: event.KV,
	}

	rev, err = b.log.Append(ctx, updateEvent)
	if err != nil {
		rev, event, err := b.get(ctx, key, revision, false)
		return rev, event.KV, false, err
	}

	updateEvent.KV.ModRevision = rev
	return rev, updateEvent.KV, true, err
}

func (b *Backend) Start(ctx context.Context) {
	go b.ttl(ctx)
}

func (b *Backend) ttl(ctx context.Context) {
	// very naive TTL support
	for events := range b.log.Watch(ctx, "/") {
		for _, event := range events {
			if event.KV.Lease <= 0 {
				continue
			}
			go func() {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Duration(event.KV.Lease) * time.Second):
				}
				b.Delete(ctx, event.KV.Key, event.KV.ModRevision)
			}()
		}
	}
}

func (b *Backend) Watch(ctx context.Context, prefix string, revision int64) <-chan []*backend.Event {
	logrus.Debugf("WATCH %s, revision=%d", prefix, revision)

	// starting watching right away so we don't miss anything
	ctx, cancel := context.WithCancel(ctx)
	readChan := b.log.Watch(ctx, prefix)

	result := make(chan []*backend.Event)
	lastRevision := int64(0)

	rev, kvs, err := b.log.After(ctx, prefix, revision)
	lastRevision = rev
	if err != nil {
		logrus.Error("failed to list %s for revision %s", prefix, revision)
		cancel()
	}
	if len(kvs) > 0 {
		result <- kvs
	}

	go func() {
		// always ensure we fully read the channel
		for i := range readChan {
			i = filter(i, lastRevision)
			result <- filter(i, lastRevision)
		}
		close(result)
	}()

	return result
}

func filter(events []*backend.Event, rev int64) []*backend.Event {
	for len(events) > 0 && events[0].KV.ModRevision <= rev {
		events = events[1:]
	}

	return events
}
