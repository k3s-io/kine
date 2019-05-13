package bridge

import (
	"context"
	"sync"

	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/rancher/kine/pkg/backend"
)

func (s *KVServerBridge) Watch(ws etcdserverpb.Watch_WatchServer) error {
	w := watcher{
		server:  ws,
		backend: s.limited.backend,
		watches: map[int64]func(){},
	}
	defer w.Close()

	for {
		msg, err := ws.Recv()
		if err != nil {
			return err
		}

		if msg.GetCreateRequest() != nil {
			w.Start(msg.GetCreateRequest())
		} else if msg.GetCancelRequest() != nil {
			w.Cancel(msg.GetCancelRequest().WatchId)
		}
	}
}

type watcher struct {
	sync.Mutex

	wg      sync.WaitGroup
	backend backend.Backend
	server  etcdserverpb.Watch_WatchServer
	ids     int64
	watches map[int64]func()
}

func (w *watcher) Start(r *etcdserverpb.WatchCreateRequest) {
	w.Lock()
	defer w.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	w.ids++

	id := w.ids
	w.watches[id] = cancel
	w.wg.Add(1)

	key := string(r.Key)
	end := string(r.RangeEnd)
	if len(end) > 0 && end[len(end)-1] == '0' {
		key = string(append(r.RangeEnd[:len(r.RangeEnd)-1], r.RangeEnd[len(r.RangeEnd)-1]-1)) + "/"
	}

	go func() {
		defer w.wg.Done()
		for e := range w.backend.Watch(ctx, key) {
			err := w.server.Send(&etcdserverpb.WatchResponse{
				WatchId: id,
				Events:  toEvents(e...),
			})
			if err != nil {
				cancel()
			}
		}
	}()
}

func toEvents(events ...*backend.Event) []*mvccpb.Event {
	ret := make([]*mvccpb.Event, 0, len(events))
	for _, e := range events {
		ret = append(ret, toEvent(e))
	}
	return ret
}

func toEvent(event *backend.Event) *mvccpb.Event {
	e := &mvccpb.Event{
		Kv:     toKV(event.KV),
		PrevKv: toKV(event.PrevKV),
	}
	if event.Delete {
		e.Type = mvccpb.DELETE
	} else {
		e.Type = mvccpb.PUT
	}

	return e
}

func (w *watcher) Cancel(watchID int64) {
	w.Lock()
	defer w.Unlock()
	if cancel, ok := w.watches[watchID]; ok {
		cancel()
		delete(w.watches, watchID)
	}
}

func (w *watcher) Close() {
	w.Lock()
	defer w.Unlock()
	for _, v := range w.watches {
		v()
	}
	w.wg.Wait()
}
