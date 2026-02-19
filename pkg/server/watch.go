package server

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k3s-io/kine/pkg/util"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// progressResponsePeriod determines how often broadcast watch progress responses will be sent
	progressResponsePeriod = 100 * time.Millisecond
)

var serverID int64
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
	id := atomic.AddInt64(&serverID, 1)
	w := watcher{
		id:       id,
		server:   ws,
		backend:  s.limited.backend,
		watches:  map[int64]func(){},
		progress: map[int64]chan<- int64{},
	}
	defer w.Close()

	logrus.Tracef("WATCH SERVER CREATE server=%d", w.id)

	go util.UntilWithContext(ws.Context(), s.getProgressReportInterval(), w.ProgressIfSynced, false)
	go util.UntilWithContext(ws.Context(), progressResponsePeriod, w.ProgressAll, false)

	for {
		msg, err := ws.Recv()
		if err != nil {
			return err
		}

		if cr := msg.GetCreateRequest(); cr != nil {
			w.Create(ws.Context(), cr)
		}
		if cr := msg.GetCancelRequest(); cr != nil {
			logrus.Tracef("WATCH CANCEL REQ server=%d, id=%d", w.id, cr.WatchId)
			w.Cancel(cr.WatchId, 0, 0, nil)
		}
		if pr := msg.GetProgressRequest(); pr != nil {
			w.Progress(ws.Context())
		}
	}
}

type watcher struct {
	sync.RWMutex

	id       int64
	wg       sync.WaitGroup
	backend  Backend
	server   etcdserverpb.Watch_WatchServer
	watches  map[int64]func()
	progress map[int64]chan<- int64
	notify   atomic.Bool
}

func (w *watcher) Create(ctx context.Context, r *etcdserverpb.WatchCreateRequest) {
	if r.WatchId != clientv3.AutoWatchID {
		logrus.Warnf("WATCH CREATE server=%d, id=%d rejecting request with client-provided id", w.id, r.WatchId)
		w.CancelEarly(ctx, ErrInvalidWatch)
		return
	}

	if r.StartRevision < 0 {
		logrus.Warnf("WATCH CREATE server=%d rejecting request with negative StartRevision=%d", w.id, r.StartRevision)
		w.CancelEarly(ctx, ErrCompacted)
		return
	}

	w.Lock()
	defer w.Unlock()

	ctx, cancel := context.WithCancel(ctx)

	id := atomic.AddInt64(&watchID, 1)
	w.watches[id] = cancel

	key := string(r.Key)
	startRevision := r.StartRevision

	// redirect apiserver watches to the substitute compact revision key
	// response is fixed up in toKV()
	if key == compactRevKey {
		key = compactRevAPI
	}

	var progressCh chan int64
	if r.ProgressNotify {
		progressCh = make(chan int64)
		w.progress[id] = progressCh
	}

	logrus.Tracef("WATCH CREATE server=%d, id=%d, key=%s, revision=%d, progressNotify=%v, watchCount=%d", w.id, id, key, startRevision, r.ProgressNotify, len(w.watches))

	w.wg.Add(1)
	go w.watch(ctx, key, id, startRevision, progressCh)
}

func (w *watcher) watch(ctx context.Context, key string, id, startRevision int64, progressCh chan int64) {
	defer w.wg.Done()
	trace := logrus.IsLevelEnabled(logrus.TraceLevel)

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
			// get max revision from collected events
			if len(events) > 0 {
				revision = events[len(events)-1].KV.ModRevision
			}
		case revision = <-progressCh:
			// have been requested to send progress with no events
		}

		// send response. note that there are no events if this is a progress response -
		// but revision 0 is also sent on the progress channel to check if this
		// reader has synced or not, so we must not send with revision 0.
		if revision != 0 && (len(events) == 0 || revision >= startRevision) {
			wr := &etcdserverpb.WatchResponse{
				Header:  txnHeader(revision),
				WatchId: id,
				Events:  toEvents(events...),
			}
			if trace {
				keys := make([]string, len(wr.Events))
				for i, event := range wr.Events {
					keys[i] = string(event.Kv.Key)
				}
				logrus.Tracef("WATCH SEND server=%d, id=%d, key=%s, revision=%d, events=%d, size=%d, reads=%d, keys=%s", w.id, id, key, revision, len(wr.Events), wr.Size(), reads, keys)
			}
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
	logrus.Tracef("WATCH CLOSE server=%d, id=%d, key=%s", w.id, id, key)
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

func (w *watcher) removeWatch(watchID int64) bool {
	w.Lock()
	defer w.Unlock()
	if progressCh, ok := w.progress[watchID]; ok {
		close(progressCh)
		delete(w.progress, watchID)
	}
	if cancel, ok := w.watches[watchID]; ok {
		cancel()
		delete(w.watches, watchID)
		return true
	}
	return false
}

func (w *watcher) CancelEarly(ctx context.Context, err error) {
	rev, err := w.backend.CurrentRevision(ctx)
	if err != nil {
		logrus.Warnf("Failed to get current revision for early watch cancel: %v", err)
		return
	}

	err = w.server.Send(&etcdserverpb.WatchResponse{
		Header:       txnHeader(rev),
		WatchId:      clientv3.InvalidWatchID,
		Canceled:     true,
		Created:      true,
		CancelReason: err.Error(),
	})

	if err != nil && !clientv3.IsConnCanceled(err) {
		logrus.Errorf("WATCH Failed to send early cancel response for server=%d: %v", w.id, err)
	}
}

func (w *watcher) Cancel(watchID, revision, compactRev int64, err error) {
	// do not send WatchResponse for unknown watch ID
	if !w.removeWatch(watchID) {
		return
	}

	reason := ""
	if err != nil {
		reason = err.Error()
	}
	logrus.Tracef("WATCH CANCEL server=%d, id=%d, reason=%s, compactRev=%d", w.id, watchID, reason, compactRev)

	serr := w.server.Send(&etcdserverpb.WatchResponse{
		Header:          txnHeader(revision),
		Canceled:        true,
		CancelReason:    reason,
		WatchId:         watchID,
		CompactRevision: compactRev,
	})
	if serr != nil && err != nil && !clientv3.IsConnCanceled(serr) {
		logrus.Errorf("WATCH Failed to send cancel response for server=%d, id=%d: %v", w.id, watchID, serr)
	}
}

func (w *watcher) Close() {
	logrus.Tracef("WATCH SERVER CLOSE server=%d", w.id)
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

// Progress requests a progress report if all watchers are synced.
// The apiserver may spam progress requests every 100ms while waiting for caches to sync.
// This handler just sets a flag indicating that notification has been requested;
// a polling goroutine periodically checks the flag and sends a notification if all
// watchers are synced.
// Ref: https://github.com/etcd-io/etcd/blob/v3.5.27/server/mvcc/watchable_store.go#L519-L523
// Ref: https://github.com/kubernetes/kubernetes/blob/v1.35.1/staging/src/k8s.io/apiserver/pkg/storage/cacher/progress/watch_progress.go#L34-L36
func (w *watcher) Progress(ctx context.Context) {
	logrus.Tracef("WATCH REQUEST PROGRESS server=%d", w.id)
	w.RLock()
	defer w.RUnlock()
	w.notify.Store(true)
}

// ProgressAll sends a broadcast watch progress notification if all
// watches on this server are synced.
func (w *watcher) ProgressAll(ctx context.Context) {
	if !w.notify.Load() {
		return
	}

	// If all watchers are synced, send a broadcast progress notification with the latest revision.
	id := int64(clientv3.InvalidWatchID)
	rev, err := w.backend.CurrentRevision(ctx)
	if err != nil {
		logrus.Errorf("Failed to get current revision for ProgressNotify: %v", err)
		return
	}

	// ensure that poll loop is caught up with current revision before checking for sync
	w.backend.WaitForSyncTo(rev)

	w.RLock()
	defer w.RUnlock()
	w.notify.Store(false)

	// All synced watchers will be blocked in the outer loop and able to receive on the progress channel.
	// If any cannot be sent to, then it is not synced and has pending events to be sent.
	// Send revision 0, as we don't actually want the watchers to send a progress response if they do receive.
	for id, progressCh := range w.progress {
		select {
		case progressCh <- 0:
		default:
			logrus.Tracef("WATCH SEND PROGRESS FAILED READER NOT SYNCED server=%d, id=%d", w.id, id)
			return
		}
	}

	logrus.Tracef("WATCH SEND PROGRESS server=%d, id=%d, revision=%d", w.id, id, rev)
	go w.server.Send(&etcdserverpb.WatchResponse{Header: txnHeader(rev), WatchId: id})
}

// ProgressIfSynced sends a progress report on any channels that are synced.
func (w *watcher) ProgressIfSynced(ctx context.Context) {
	logrus.Tracef("WATCH PROGRESS TICK server=%d", w.id)

	revision, err := w.backend.CurrentRevision(ctx)
	if err != nil {
		logrus.Errorf("Failed to get current revision for ProgressNotify: %v", err)
		return
	}

	w.backend.WaitForSyncTo(revision)

	w.RLock()
	defer w.RUnlock()

	// Send revision to all synced channels
	for _, progressCh := range w.progress {
		select {
		case progressCh <- revision:
		default:
		}
	}
}
