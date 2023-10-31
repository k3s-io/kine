package server

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var watchID int64

// explicit interface check
var _ etcdserverpb.WatchServer = (*KVServerBridge)(nil)

func (s *KVServerBridge) Watch(ws etcdserverpb.Watch_WatchServer) error {
	w := watcher{
		server:  ws,
		backend: s.limited.backend,
		watches: map[int64]func(){},
	}
	defer w.Close()

	logrus.Tracef("WATCH SERVER CREATE")

	for {
		msg, err := ws.Recv()
		if err != nil {
			return err
		}

		if cr := msg.GetCreateRequest(); cr != nil {
			w.Start(ws.Context(), cr)
		}
		if cr := msg.GetCancelRequest(); cr != nil {
			logrus.Tracef("WATCH CANCEL REQ id=%d", cr.WatchId)
			w.Cancel(cr.WatchId, 0, 0, nil)
		}
	}
}

type watcher struct {
	sync.Mutex

	wg      sync.WaitGroup
	backend Backend
	server  etcdserverpb.Watch_WatchServer
	watches map[int64]func()
}

func (w *watcher) Start(ctx context.Context, r *etcdserverpb.WatchCreateRequest) {
	w.Lock()
	defer w.Unlock()

	ctx, cancel := context.WithCancel(ctx)

	id := atomic.AddInt64(&watchID, 1)
	w.watches[id] = cancel
	w.wg.Add(1)

	key := string(r.Key)

	logrus.Tracef("WATCH START id=%d, count=%d, key=%s, revision=%d", id, len(w.watches), key, r.StartRevision)

	go func() {
		defer w.wg.Done()
		if err := w.server.Send(&etcdserverpb.WatchResponse{
			Header:  &etcdserverpb.ResponseHeader{},
			Created: true,
			WatchId: id,
		}); err != nil {
			w.Cancel(id, 0, 0, err)
			return
		}

		wr := w.backend.Watch(ctx, key, r.StartRevision)

		// If the watch result has a non-zero CompactRevision, then the watch request failed due to
		// the requested start revision having been compacted.  Pass the current and and compact
		// revision to the client via the cancel response, along with the correct error message.
		if wr.CompactRevision != 0 {
			w.Cancel(id, wr.CurrentRevision, wr.CompactRevision, ErrCompacted)
			return
		}

		outer := true
		for outer {
			// Block on initial read from channel
			reads := 1
			events := <-wr.Events

			// Collect additional queued events from the channel
			inner := true
			for inner {
				select {
				case e, ok := <-wr.Events:
					reads++
					events = append(events, e...)
					if !ok {
						// channel was closed, break out of both loops
						inner = false
						outer = false
					}
				default:
					inner = false
				}
			}

			// Send collected events in a single response
			if len(events) > 0 {
				if logrus.IsLevelEnabled(logrus.TraceLevel) {
					for _, event := range events {
						logrus.Tracef("WATCH READ id=%d, key=%s, revision=%d", id, event.KV.Key, event.KV.ModRevision)
					}
				}
				wr := &etcdserverpb.WatchResponse{
					Header:  txnHeader(events[len(events)-1].KV.ModRevision),
					WatchId: id,
					Events:  toEvents(events...),
				}
				logrus.Tracef("WATCH SEND id=%d, events=%d, size=%d reads=%d", id, len(wr.Events), wr.Size(), reads)
				if err := w.server.Send(wr); err != nil {
					w.Cancel(id, 0, 0, err)
				}
			}
		}
		w.Cancel(id, 0, 0, nil)
		logrus.Tracef("WATCH CLOSE id=%d, key=%s", id, key)
	}()
}

func toEvents(events ...*Event) []*mvccpb.Event {
	ret := make([]*mvccpb.Event, 0, len(events))
	for _, e := range events {
		ret = append(ret, toEvent(e))
	}
	return ret
}

func toEvent(event *Event) *mvccpb.Event {
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

func (w *watcher) Cancel(watchID, revision, compactRev int64, err error) {
	w.Lock()
	if cancel, ok := w.watches[watchID]; ok {
		cancel()
		delete(w.watches, watchID)
	}
	w.Unlock()

	reason := ""
	if err != nil {
		reason = err.Error()
	}
	logrus.Tracef("WATCH CANCEL id=%d, reason=%s, compactRev=%d", watchID, reason, compactRev)

	serr := w.server.Send(&etcdserverpb.WatchResponse{
		Header:          txnHeader(revision),
		Canceled:        err != nil,
		CancelReason:    reason,
		WatchId:         watchID,
		CompactRevision: compactRev,
	})
	if serr != nil && err != nil && !clientv3.IsConnCanceled(serr) {
		logrus.Errorf("WATCH Failed to send cancel response for watchID %d: %v", watchID, serr)
	}
}

func (w *watcher) Close() {
	logrus.Tracef("WATCH SERVER CLOSE")
	w.Lock()
	for _, v := range w.watches {
		v()
	}
	w.Unlock()
	w.wg.Wait()
}
