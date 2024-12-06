package server

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/apimachinery/pkg/util/wait"
)

var watchID int64

// explicit interface check
var _ etcdserverpb.WatchServer = (*KVServerBridge)(nil)

// getProgressReportInterval returns the configured progress report interval, with some jitter
func (s *KVServerBridge) getProgressReportInterval() time.Duration {
	// add rand(1/10*notifyInterval) as jitter so that kine will not
	// send progress notifications to watchers at the same time even when watchers
	// are created at the same time.
	jitter := time.Duration(rand.Int63n(int64(s.limited.notifyInterval) / 10))
	return s.limited.notifyInterval + jitter
}

func (s *KVServerBridge) Watch(ws etcdserverpb.Watch_WatchServer) error {
	w := watcher{
		server:   ws,
		backend:  s.limited.backend,
		watches:  map[int64]func(){},
		progress: map[int64]chan<- int64{},
	}
	defer w.Close()

	logrus.Tracef("WATCH SERVER CREATE")

	go wait.PollInfiniteWithContext(ws.Context(), s.getProgressReportInterval(), w.ProgressIfSynced)

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
		if pr := msg.GetProgressRequest(); pr != nil {
			w.Progress(ws.Context())
		}
	}
}

type watcher struct {
	sync.RWMutex

	wg       sync.WaitGroup
	backend  Backend
	server   etcdserverpb.Watch_WatchServer
	watches  map[int64]func()
	progress map[int64]chan<- int64
}

func (w *watcher) Start(ctx context.Context, r *etcdserverpb.WatchCreateRequest) {
	if r.WatchId != clientv3.AutoWatchID {
		logrus.Warnf("WATCH START id=%d ignoring request with client-provided id", r.WatchId)
		return
	}

	w.Lock()
	defer w.Unlock()

	ctx, cancel := context.WithCancel(ctx)

	id := atomic.AddInt64(&watchID, 1)
	w.watches[id] = cancel
	w.wg.Add(1)

	key := string(r.Key)
	startRevision := r.StartRevision

	var progressCh chan int64
	if r.ProgressNotify {
		progressCh = make(chan int64)
		w.progress[id] = progressCh
	}

	logrus.Tracef("WATCH START id=%d, key=%s, revision=%d, progressNotify=%v, watchCount=%d", id, key, startRevision, r.ProgressNotify, len(w.watches))

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

		wr := w.backend.Watch(ctx, key, startRevision)

		// If the watch result has a non-zero CompactRevision, then the watch request failed due to
		// the requested start revision having been compacted.  Pass the current and and compact
		// revision to the client via the cancel response, along with the correct error message.
		if wr.CompactRevision != 0 {
			w.Cancel(id, wr.CurrentRevision, wr.CompactRevision, ErrCompacted)
			return
		}

		trace := logrus.IsLevelEnabled(logrus.TraceLevel)
		outer := true
		for outer {
			var reads int
			var events []*Event
			var revision int64

			// Block on initial read from events or progress channel
			select {
			case events = <-wr.Events:
				// got events; read additional queued events from the channel and add to batch
				reads++
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
			case revision = <-progressCh:
				// have been requested to send progress with no events
			}

			// get max revision from collected events
			if len(events) > 0 {
				revision = events[len(events)-1].KV.ModRevision
				if trace {
					for _, event := range events {
						logrus.Tracef("WATCH READ id=%d, key=%s, revision=%d", id, event.KV.Key, event.KV.ModRevision)
					}
				}
			}

			// send response. note that there are no events if this is a progress response.
			if revision >= startRevision {
				wr := &etcdserverpb.WatchResponse{
					Header:  txnHeader(revision),
					WatchId: id,
					Events:  toEvents(events...),
				}
				logrus.Tracef("WATCH SEND id=%d, key=%s, revision=%d, events=%d, size=%d, reads=%d", id, key, revision, len(wr.Events), wr.Size(), reads)
				if err := w.server.Send(wr); err != nil {
					w.Cancel(id, 0, 0, err)
				}
			}
		}

		select {
		case err := <-wr.Errorc:
			w.Cancel(id, 0, 0, err)
		default:
			w.Cancel(id, 0, 0, nil)
		}
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
	if progressCh, ok := w.progress[watchID]; ok {
		close(progressCh)
		delete(w.progress, watchID)
	}
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
		Canceled:        true,
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
	for id, progressCh := range w.progress {
		close(progressCh)
		delete(w.progress, id)
	}
	for id, cancel := range w.watches {
		cancel()
		delete(w.watches, id)
	}
	w.Unlock()
	w.wg.Wait()
}

// Progress sends a progress report if all watchers are synced.
// Ref: https://github.com/etcd-io/etcd/blob/v3.5.11/server/mvcc/watchable_store.go#L500-L504
func (w *watcher) Progress(ctx context.Context) {
	w.RLock()
	defer w.RUnlock()

	logrus.Tracef("WATCH REQUEST PROGRESS")

	// All synced watchers will be blocked in the outer loop and able to receive on the progress channel.
	// If any cannot be sent to, then it is not synced and has pending events to be sent.
	// Send revision 0, as we don't actually want the watchers to send a progress response if they do receive.
	for id, progressCh := range w.progress {
		select {
		case progressCh <- 0:
		default:
			logrus.Tracef("WATCH SEND PROGRESS FAILED NOT SYNCED id=%d ", id)
			return
		}
	}

	// If all watchers are synced, send a broadcast progress notification with the latest revision.
	id := int64(clientv3.InvalidWatchID)
	rev, err := w.backend.CurrentRevision(ctx)
	if err != nil {
		logrus.Errorf("Failed to get current revision for ProgressNotify: %v", err)
		return
	}

	logrus.Tracef("WATCH SEND PROGRESS id=%d, revision=%d", id, rev)
	go w.server.Send(&etcdserverpb.WatchResponse{Header: txnHeader(rev), WatchId: id})
}

// ProgressIfSynced sends a progress report on any channels that are synced and blocked on the outer loop
func (w *watcher) ProgressIfSynced(ctx context.Context) (bool, error) {
	logrus.Tracef("WATCH PROGRESS TICK")
	revision, err := w.backend.CurrentRevision(ctx)
	if err != nil {
		logrus.Errorf("Failed to get current revision for ProgressNotify: %v", err)
		return false, nil
	}

	w.RLock()
	defer w.RUnlock()

	// Send revision to all synced channels
	for _, progressCh := range w.progress {
		select {
		case progressCh <- revision:
		default:
		}
	}
	return false, nil
}
