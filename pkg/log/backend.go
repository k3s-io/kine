package log

import (
	"context"

	"github.com/rancher/kine/pkg/backend"
)

type Log interface {
	List(ctx context.Context, prefix, startKey string, limit, revision int64) (int64, []*backend.Event, error)
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

func (b *Backend) Get(ctx context.Context, key string, revision int64) (int64, *backend.KeyValue, error) {
	rev, event, err := b.get(ctx, key, revision)
	if event == nil {
		return rev, nil, err
	}
	return rev, event.KV, err
}

func (b *Backend) get(ctx context.Context, key string, revision int64) (int64, *backend.Event, error) {
	rev, events, err := b.log.List(ctx, key, "", 1, revision)
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

func (b *Backend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	rev, prevEvent, err := b.get(ctx, key, 0)
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

func (b *Backend) Delete(ctx context.Context, key string, revision int64) (int64, *backend.KeyValue, bool, error) {
	rev, event, err := b.get(ctx, key, revision)
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
		PrevKV: event.KV,
	}

	rev, err = b.log.Append(ctx, deleteEvent)
	return rev, event.KV, true, err
}

func (b *Backend) List(ctx context.Context, prefix, startKey string, limit, revision int64) (int64, []*backend.KeyValue, error) {
	rev, events, err := b.log.List(ctx, prefix, startKey, limit, revision)
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

func (b *Backend) Count(ctx context.Context, prefix string) (int64, int64, error) {
	return b.log.Count(ctx, prefix)
}

func (b *Backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *backend.KeyValue, bool, error) {
	rev, event, err := b.get(ctx, key, revision)
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
			Key:   key,
			Value: value,
			Lease: lease,
		},
		PrevKV: event.KV,
	}

	rev, err = b.log.Append(ctx, updateEvent)
	updateEvent.KV.ModRevision = rev
	return rev, updateEvent.KV, true, err
}

func (b *Backend) Watch(ctx context.Context, prefix string) <-chan []*backend.Event {
	return b.log.Watch(ctx, prefix)
}
