package logstructured

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
	retryInterval = 250 * time.Millisecond
)

type Log interface {
	Start(ctx context.Context) error
	CompactRevision(ctx context.Context) (int64, error)
	CurrentRevision(ctx context.Context) (int64, error)
	List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeletes bool) (int64, []*server.Event, error)
	Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error)
	After(ctx context.Context, prefix string, revision, limit int64) (int64, []*server.Event, error)
	Watch(ctx context.Context, prefix string) <-chan []*server.Event
	Append(ctx context.Context, event *server.Event) (int64, error)
	DbSize(ctx context.Context) (int64, error)
	Compact(ctx context.Context, revision int64) (int64, error)
}

type ttlEventKV struct {
	key         string
	modRevision int64
	expiredAt   time.Time
}

type LogStructured struct {
	log Log
}

func New(log Log) *LogStructured {
	return &LogStructured{
		log: log,
	}
}

func (l *LogStructured) Start(ctx context.Context) error {
	if err := l.log.Start(ctx); err != nil {
		return err
	}
	// See https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	if _, err := l.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0); err != nil {
		if err != server.ErrKeyExists {
			logrus.Errorf("Failed to create health check key: %v", err)
		}
	}
	go l.ttl(ctx)
	return nil
}

func (l *LogStructured) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (revRet int64, kvRet *server.KeyValue, errRet error) {
	defer func() {
		l.adjustRevision(ctx, &revRet)
		logrus.Tracef("GET %s, rev=%d => rev=%d, kv=%v, err=%v", key, revision, revRet, kvRet != nil, errRet)
	}()

	rev, event, err := l.get(ctx, key, rangeEnd, limit, revision, false)
	if event == nil {
		return rev, nil, err
	}
	return rev, event.KV, err
}

func (l *LogStructured) get(ctx context.Context, key, rangeEnd string, limit, revision int64, includeDeletes bool) (int64, *server.Event, error) {
	rev, events, err := l.log.List(ctx, key, rangeEnd, limit, revision, includeDeletes)
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

func (l *LogStructured) adjustRevision(ctx context.Context, rev *int64) {
	if *rev != 0 {
		return
	}

	if newRev, err := l.log.CurrentRevision(ctx); err == nil {
		*rev = newRev
	}
}

func (l *LogStructured) Create(ctx context.Context, key string, value []byte, lease int64) (revRet int64, errRet error) {
	defer func() {
		l.adjustRevision(ctx, &revRet)
		logrus.Tracef("CREATE %s, size=%d, lease=%d => rev=%d, err=%v", key, len(value), lease, revRet, errRet)
	}()

	rev, prevEvent, err := l.get(ctx, key, "", 1, 0, true)
	if err != nil {
		return 0, err
	}
	createEvent := &server.Event{
		Create: true,
		KV: &server.KeyValue{
			Key:   key,
			Value: value,
			Lease: lease,
		},
		PrevKV: &server.KeyValue{
			ModRevision: rev,
		},
	}
	if prevEvent != nil {
		if !prevEvent.Delete {
			return 0, server.ErrKeyExists
		}
		createEvent.PrevKV = prevEvent.KV
	}

	revRet, errRet = l.log.Append(ctx, createEvent)
	return
}

func (l *LogStructured) Delete(ctx context.Context, key string, revision int64) (revRet int64, kvRet *server.KeyValue, deletedRet bool, errRet error) {
	defer func() {
		l.adjustRevision(ctx, &revRet)
		logrus.Tracef("DELETE %s, rev=%d => rev=%d, kv=%v, deleted=%v, err=%v", key, revision, revRet, kvRet != nil, deletedRet, errRet)
	}()

	rev, event, err := l.get(ctx, key, "", 1, 0, true)
	if err != nil {
		return 0, nil, false, err
	}

	if event == nil {
		return rev, nil, true, nil
	}

	if event.Delete {
		return rev, event.KV, true, nil
	}

	if revision != 0 && event.KV.ModRevision != revision {
		return rev, event.KV, false, nil
	}

	deleteEvent := &server.Event{
		Delete: true,
		KV:     event.KV,
		PrevKV: event.KV,
	}

	rev, err = l.log.Append(ctx, deleteEvent)
	if err != nil {
		// If error on Append we assume it's a UNIQUE constraint error, so we fetch the latest (if we can)
		// and return that the delete failed
		latestRev, latestEvent, latestErr := l.get(ctx, key, "", 1, 0, true)
		if latestErr != nil || latestEvent == nil {
			return rev, event.KV, false, nil
		}
		return latestRev, latestEvent.KV, false, nil
	}
	return rev, event.KV, true, err
}

func (l *LogStructured) List(ctx context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	defer func() {
		logrus.Tracef("LIST %s, start=%s, limit=%d, rev=%d => rev=%d, kvs=%d, err=%v", prefix, startKey, limit, revision, revRet, len(kvRet), errRet)
	}()

	rev, events, err := l.log.List(ctx, prefix, startKey, limit, revision, false)
	if err != nil {
		return rev, nil, err
	}
	if revision == 0 && len(events) == 0 {
		// if no revision is requested and no events are returned, then
		// get the current revision and relist.  Relist is required because
		// between now and getting the current revision something could have
		// been created.
		currentRev, err := l.log.CurrentRevision(ctx)
		if err != nil {
			return currentRev, nil, err
		}
		return l.List(ctx, prefix, startKey, limit, currentRev)
	} else if revision != 0 {
		rev = revision
	}

	kvs := make([]*server.KeyValue, 0, len(events))
	for _, event := range events {
		kvs = append(kvs, event.KV)
	}
	return rev, kvs, nil
}

func (l *LogStructured) Count(ctx context.Context, prefix, startKey string, revision int64) (revRet int64, count int64, err error) {
	defer func() {
		logrus.Tracef("COUNT %s, rev=%d => rev=%d, count=%d, err=%v", prefix, revision, revRet, count, err)
	}()
	rev, count, err := l.log.Count(ctx, prefix, startKey, revision)
	if err != nil {
		return 0, 0, err
	}

	if count == 0 {
		// if count is zero, then so is revision, so now get the current revision and re-count at that revision
		currentRev, err := l.log.CurrentRevision(ctx)
		if err != nil {
			return 0, 0, err
		}
		rev, rows, err := l.List(ctx, prefix, prefix, 1000, currentRev)
		return rev, int64(len(rows)), err
	}
	return rev, count, nil
}

func (l *LogStructured) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *server.KeyValue, updateRet bool, errRet error) {
	defer func() {
		l.adjustRevision(ctx, &revRet)
		kvRev := int64(0)
		if kvRet != nil {
			kvRev = kvRet.ModRevision
		}
		logrus.Tracef("UPDATE %s, value=%d, rev=%d, lease=%v => rev=%d, kvrev=%d, updated=%v, err=%v", key, len(value), revision, lease, revRet, kvRev, updateRet, errRet)
	}()

	rev, event, err := l.get(ctx, key, "", 1, 0, false)
	if err != nil {
		return 0, nil, false, err
	}

	if event == nil {
		return 0, nil, false, nil
	}

	if event.KV.ModRevision != revision {
		return rev, event.KV, false, nil
	}

	updateEvent := &server.Event{
		KV: &server.KeyValue{
			Key:            key,
			CreateRevision: event.KV.CreateRevision,
			Value:          value,
			Lease:          lease,
		},
		PrevKV: event.KV,
	}

	rev, err = l.log.Append(ctx, updateEvent)
	if err != nil {
		rev, event, err := l.get(ctx, key, "", 1, 0, false)
		if event == nil {
			return rev, nil, false, err
		}
		return rev, event.KV, false, err
	}

	updateEvent.KV.ModRevision = rev
	return rev, updateEvent.KV, true, err
}

func (l *LogStructured) ttl(ctx context.Context) {
	queue := workqueue.NewDelayingQueue()
	rwMutex := &sync.RWMutex{}
	ttlEventKVMap := make(map[string]*ttlEventKV)
	go func() {
		for l.handleTTLEvents(ctx, rwMutex, queue, ttlEventKVMap) {
		}
	}()

	for {
		select {
		case <-ctx.Done():
			queue.ShutDown()
			return
		default:
		}

		for event := range l.ttlEvents(ctx) {
			if event.Delete {
				continue
			}

			eventKV := loadTTLEventKV(rwMutex, ttlEventKVMap, event.KV.Key)
			if eventKV == nil {
				expires := storeTTLEventKV(rwMutex, ttlEventKVMap, event.KV)
				logrus.Tracef("TTL add event key=%v, modRev=%v, ttl=%v", event.KV.Key, event.KV.ModRevision, expires)
				queue.AddAfter(event.KV.Key, expires)
			} else {
				if event.KV.ModRevision > eventKV.modRevision {
					expires := storeTTLEventKV(rwMutex, ttlEventKVMap, event.KV)
					logrus.Tracef("TTL update event key=%v, modRev=%v, ttl=%v", event.KV.Key, event.KV.ModRevision, expires)
					queue.AddAfter(event.KV.Key, expires)
				}
			}
		}
	}
}

func (l *LogStructured) handleTTLEvents(ctx context.Context, rwMutex *sync.RWMutex, queue workqueue.DelayingInterface, store map[string]*ttlEventKV) bool {
	key, shutdown := queue.Get()
	if shutdown {
		logrus.Info("TTL events work queue has shut down")
		return false
	}
	defer queue.Done(key)

	eventKV := loadTTLEventKV(rwMutex, store, key.(string))
	if eventKV == nil {
		logrus.Errorf("TTL event not found for key=%v", key)
		return true
	}

	if expires := time.Until(eventKV.expiredAt); expires > 0 {
		logrus.Tracef("TTL has not expired for key=%v, ttl=%v, requeuing", key, expires)
		queue.AddAfter(key, expires)
		return true
	}

	logrus.Tracef("TTL delete key=%v, modRev=%v", eventKV.key, eventKV.modRevision)
	if _, _, _, err := l.Delete(ctx, eventKV.key, eventKV.modRevision); err != nil {
		logrus.Errorf("TTL delete trigger failed for key=%v: %v, requeuing", eventKV.key, err)
		queue.AddAfter(eventKV.key, retryInterval)
		return true
	}

	rwMutex.Lock()
	defer rwMutex.Unlock()
	delete(store, eventKV.key)

	return true
}

// ttlEvents starts a goroutine to do a ListWatch on the root prefix. First it lists
// all non-deleted keys with a page size of 1000, then it starts watching at the
// revision returned by the initial list. Any keys that have a Lease associated with
// them are sent into the result channel for deferred handling of TTL expiration.
func (l *LogStructured) ttlEvents(ctx context.Context) chan *server.Event {
	result := make(chan *server.Event)

	go func() {
		defer close(result)

		rev, events, err := l.log.List(ctx, "/", "", 1000, 0, false)
		for len(events) > 0 {
			if err != nil {
				logrus.Errorf("TTL event list failed: %v", err)
				return
			}

			for _, event := range events {
				if event.KV.Lease > 0 {
					result <- event
				}
			}

			_, events, err = l.log.List(ctx, "/", events[len(events)-1].KV.Key, 1000, rev, false)
		}

		wr := l.Watch(ctx, "/", rev)
		if wr.CompactRevision != 0 {
			logrus.Errorf("TTL event watch failed: %v", server.ErrCompacted)
			return
		}
		for events := range wr.Events {
			for _, event := range events {
				if event.KV.Lease > 0 {
					result <- event
				}
			}
		}
		logrus.Info("TTL events watch channel closed")
	}()

	return result
}

func loadTTLEventKV(rwMutex *sync.RWMutex, store map[string]*ttlEventKV, key string) *ttlEventKV {
	rwMutex.RLock()
	defer rwMutex.RUnlock()
	return store[key]
}

func storeTTLEventKV(rwMutex *sync.RWMutex, store map[string]*ttlEventKV, eventKV *server.KeyValue) time.Duration {
	rwMutex.Lock()
	defer rwMutex.Unlock()
	expires := time.Duration(eventKV.Lease) * time.Second
	store[eventKV.Key] = &ttlEventKV{
		key:         eventKV.Key,
		modRevision: eventKV.ModRevision,
		expiredAt:   time.Now().Add(expires),
	}
	return expires
}

func (l *LogStructured) Watch(ctx context.Context, prefix string, revision int64) server.WatchResult {
	logrus.Tracef("WATCH %s, revision=%d", prefix, revision)

	// starting watching right away so we don't miss anything
	ctx, cancel := context.WithCancel(ctx)
	readChan := l.log.Watch(ctx, prefix)

	// include the current revision in list
	if revision > 0 {
		revision--
	}

	result := make(chan []*server.Event, 100)
	errc := make(chan error, 1)
	wr := server.WatchResult{Events: result, Errorc: errc}

	rev, kvs, err := l.log.After(ctx, prefix, revision, 0)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			logrus.Errorf("Failed to list %s for revision %d: %v", prefix, revision, err)
			if err == server.ErrCompacted {
				compact, _ := l.log.CompactRevision(ctx)
				wr.CompactRevision = compact
				wr.CurrentRevision = rev
			} else {
				errc <- server.ErrGRPCUnhealthy
			}
		}
		cancel()
	}

	logrus.Tracef("WATCH LIST key=%s rev=%d => rev=%d kvs=%d", prefix, revision, rev, len(kvs))

	go func() {
		lastRevision := revision
		if len(kvs) > 0 {
			lastRevision = rev
		}

		if len(kvs) > 0 {
			result <- kvs
		}

		// always ensure we fully read the channel
		for i := range readChan {
			result <- filter(i, lastRevision)
		}
		close(result)
		cancel()
	}()

	return wr
}

func filter(events []*server.Event, rev int64) []*server.Event {
	for len(events) > 0 && events[0].KV.ModRevision <= rev {
		events = events[1:]
	}

	return events
}

func (l *LogStructured) DbSize(ctx context.Context) (int64, error) {
	return l.log.DbSize(ctx)
}

func (l *LogStructured) CurrentRevision(ctx context.Context) (int64, error) {
	return l.log.CurrentRevision(ctx)
}

func (l *LogStructured) Compact(ctx context.Context, revision int64) (int64, error) {
	return l.log.Compact(ctx, revision)
}
