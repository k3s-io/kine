package server

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/apimachinery/pkg/util/wait"
)

var serverID int64
var watchID int64
var invalidWatchID int64 = clientv3.InvalidWatchID

// explicit interface check
var _ etcdserverpb.WatchServer = (*KVServerBridge)(nil)

func (s *KVServerBridge) Watch(ws etcdserverpb.Watch_WatchServer) error {
	id := atomic.AddInt64(&serverID, 1)
	w := watcher{
		id:       id,
		backend:  s.limited.backend,
		watches:  map[int64]func(){},
		progress: map[int64]chan<- int64{},
		server: &server{
			id:       id,
			ws:       ws,
			maxRev:   map[int64]*revAt{},
			interval: s.limited.notifyInterval,
		},
	}
	defer w.Close()

	logrus.Tracef("WATCH SERVER CREATE server=%d", w.id)

	go wait.PollUntilContextCancel(ws.Context(), s.limited.notifyInterval, false, func(ctx context.Context) (bool, error) {
		w.ProgressIfSynced(ctx)
		return false, nil
	})

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

// server wraps the raw WatchServer with a mutex preventing
// concurrent calls to Send. WatchServer.Send calls StreamServer.SendMsg
// which is not safe to call concurrently from different goroutines.
// server also ensures that the current revision never goes backwards,
// which could otherwise happen due to races between the event watch loop
// and watch progress notifications.
type server struct {
	sync.RWMutex

	id       int64
	ws       etcdserverpb.Watch_WatchServer
	maxRev   map[int64]*revAt
	interval time.Duration
}

type revAt struct {
	r int64
	t time.Time
}

func (s *server) Send(wr *etcdserverpb.WatchResponse) error {
	s.Lock()
	defer s.Unlock()
	now := time.Now()
	if wr != nil && wr.Header != nil {
		if wr.WatchId != invalidWatchID && wr.Created {
			// Watch created, start tracking
			s.maxRev[wr.WatchId] = &revAt{r: wr.Header.Revision, t: now}
		} else if wr.WatchId != invalidWatchID && wr.Canceled {
			// Watch deleted, stop tracking
			delete(s.maxRev, wr.WatchId)
		} else {
			hasEvents := len(wr.Events) > 0
			// Track max revisions
			for id, rev := range s.maxRev {
				if wr.WatchId == invalidWatchID || wr.WatchId == id {
					if wr.Header.Revision > rev.r {
						// Record new max revision
						rev.r = wr.Header.Revision
					} else if hasEvents {
						// Only progress notifications should ever re-send an already-seen revision; if we try to
						// send events with an old revision the apiserver watch cache will ignore the event and
						// become permanently desynced. Even if we return an error here and close the watch, the
						// watcher will resume AFTER the already-seen revision, and remain desynced.
						logrus.Fatalf("WATCH SEND EVENTS FOR PAST REVISION server=%d id=%d, events=%d, revision=%d, maxRevision=%d", s.id, wr.WatchId, len(wr.Events), wr.Header.Revision, rev.r)
					}
				}
			}
			// Track last send time
			if wr.WatchId == invalidWatchID {
				for _, rev := range s.maxRev {
					rev.t = now
				}
			} else if rev, ok := s.maxRev[wr.WatchId]; ok {
				if !hasEvents && now.Sub(rev.t) < s.interval {
					// watch will call Send even if all events have been filtered out, so that this function
					// can track max seen revisions to determine if a watch is synced or not. This does mean
					// that there is no way to tell the difference between a directed notification, and all
					// events having been filtered out due to not matching the key. Handle this by
					// surpressing send of directed progress reports if the progress report interval has not
					// elapsed since the last send.
					return nil
				}
				rev.t = now
			}
		}
	}
	if logrus.IsLevelEnabled(logrus.TraceLevel) {
		keys := make([]string, len(wr.Events))
		for i, event := range wr.Events {
			keys[i] = fmt.Sprintf("%s@%d", event.Kv.Key, event.Kv.ModRevision)
		}
		logrus.Tracef("WATCH SEND server=%d id=%d, revision=%d, events=%d, size=%d, keys=%s", s.id, wr.WatchId, wr.Header.Revision, len(wr.Events), wr.Size(), keys)
	}
	return s.ws.Send(wr)
}

// maxRevision returns the max revision sent to the selected watch, or any
// watch if passed invalidWatchID. If no watches are present or the provided
// ID is invalid, this returns 0.
func (s *server) maxRevision(watchId int64) int64 {
	s.RLock()
	defer s.RUnlock()
	var maxRev int64
	if watchId == invalidWatchID {
		for _, rev := range s.maxRev {
			if maxRev < rev.r {
				maxRev = rev.r
			}
		}
		return maxRev
	}
	if rev, ok := s.maxRev[watchId]; ok {
		maxRev = rev.r
	}
	return maxRev
}

// minRevision return the lowest revision sent to any watch. If no watches are
// present, this returns MaxInt64.
func (s *server) minRevision() int64 {
	s.RLock()
	defer s.RUnlock()
	var minRev int64 = math.MaxInt64
	for _, rev := range s.maxRev {
		if minRev > rev.r {
			minRev = rev.r
		}
	}
	return minRev
}

// watcher holds state for a single Watch StreamServer
// Each StreamServer may have multiple watches over different keys; each watch has a globally unique ID
// Individual watches may (at time of creation) opt in to receving periodic progress notifications,
// which send a message with a header containing the current revision, but no events.
type watcher struct {
	sync.RWMutex

	id       int64
	wg       sync.WaitGroup
	backend  Backend
	server   *server
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

	// redirect apiserver watches to the substitute compact revision key
	// response is fixed up in toKV()
	if bytes.Equal(r.Key, compactRevKey) {
		r.Key = compactRevAPI
	}

	key := string(r.Key)
	end := string(r.RangeEnd)
	startRevision := r.StartRevision
	if key == "\x00" && end == "\x00" {
		key = ""
		end = ""
	}

	var progressCh chan int64
	if r.ProgressNotify {
		progressCh = make(chan int64)
		w.progress[id] = progressCh
	}

	logrus.Tracef("WATCH CREATE server=%d, id=%d, key=%s, end=%s, revision=%d, progressNotify=%v, watchCount=%d", w.id, id, key, end, startRevision, r.ProgressNotify, len(w.watches))

	w.wg.Add(1)
	go w.watch(ctx, key, end, id, startRevision, progressCh)
}

func (w *watcher) watch(ctx context.Context, key, end string, id, startRevision int64, progressCh chan int64) {
	defer w.wg.Done()

	if err := w.server.Send(&etcdserverpb.WatchResponse{
		Header:  &etcdserverpb.ResponseHeader{},
		Created: true,
		WatchId: id,
	}); err != nil {
		w.Cancel(id, 0, 0, err)
		return
	}

	wr := w.backend.Watch(ctx, startRevision)

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
		var events Events
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
			if i := len(events) - 1; i >= 0 && events[i] != nil {
				revision = events[i].KV.ModRevision
			}
		case progressRev := <-progressCh:
			// have been requested to send progress with no events;
			revision = progressRev
		}

		// send response - note that there are no events if this is a progress response
		if revision >= startRevision {
			wr := &etcdserverpb.WatchResponse{
				Header:  txnHeader(revision),
				WatchId: id,
				Events:  toEvents(key, end, events),
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

func toEvents(key, end string, events Events) []*mvccpb.Event {
	ret := make([]*mvccpb.Event, 0, len(events))
	for _, e := range events {
		if e.InRange(key, end) {
			ret = append(ret, toEvent(e))
		}
	}
	return ret
}

func toEvent(event *Event) *mvccpb.Event {
	e := &mvccpb.Event{Kv: toKV(event.KV)}
	if !event.Create {
		e.PrevKv = toKV(event.PrevKV)
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

func (w *watcher) CancelEarly(ctx context.Context, earlyErr error) {
	rev, err := w.backend.CurrentRevision(ctx)
	if err != nil {
		logrus.Warnf("Failed to get current revision for early watch cancel: %v", err)
		return
	}

	err = w.server.Send(&etcdserverpb.WatchResponse{
		Header:       txnHeader(rev),
		WatchId:      invalidWatchID,
		Canceled:     true,
		Created:      true,
		CancelReason: earlyErr.Error(),
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
// This handler sets a flag indicating that notification has been requested and
// starts a goroutine to check the flag and send a notification if all watchers
// are synced.
// Ref: https://github.com/etcd-io/etcd/blob/v3.5.27/server/mvcc/watchable_store.go#L519-L523
// Ref: https://github.com/kubernetes/kubernetes/blob/v1.35.1/staging/src/k8s.io/apiserver/pkg/storage/cacher/progress/watch_progress.go#L34-L36
func (w *watcher) Progress(ctx context.Context) {
	logrus.Tracef("WATCH REQUEST PROGRESS server=%d", w.id)
	if w.notify.CompareAndSwap(false, true) {
		go w.ProgressAll(ctx)
	}
}

// ProgressAll sends a broadcast watch progress notification if all
// watches on this server are synced.
func (w *watcher) ProgressAll(ctx context.Context) {
	if !w.notify.Load() {
		return
	}

	// If all watchers are synced, send a broadcast progress notification with the latest revision.
	rev, err := w.backend.CurrentRevision(ctx)
	if err != nil {
		logrus.Errorf("Failed to get current revision for ProgressNotify: %v", err)
		return
	}

	w.RLock()
	defer w.RUnlock()
	w.notify.Store(false)

	if minRev := w.server.minRevision(); minRev < rev {
		logrus.Tracef("WATCH SEND PROGRESS FAILED ALL READERS NOT SYNCED server=%d, minRev=%d, revision=%d", w.id, minRev, rev)
		return
	}

	logrus.Tracef("WATCH SEND PROGRESS server=%d, revision=%d", w.id, rev)
	go w.server.Send(&etcdserverpb.WatchResponse{Header: txnHeader(rev), WatchId: invalidWatchID})
}

// ProgressIfSynced sends a progress report on any channels that are synced.
func (w *watcher) ProgressIfSynced(ctx context.Context) {
	logrus.Tracef("WATCH PROGRESS TICK server=%d", w.id)
	if w.notify.Load() {
		// Do not send direct progress to individual to watches if broadcast progress has been requested.
		// This avoids sending unxpected double-progress - one broadcast in response to the request, another direct from this timer.
		return
	}

	rev, err := w.backend.CurrentRevision(ctx)
	if err != nil {
		logrus.Errorf("Failed to get current revision for ProgressNotify: %v", err)
		return
	}

	w.RLock()
	defer w.RUnlock()

	// Send revision to all synced channels
	for id, progressCh := range w.progress {
		if w.server.maxRevision(id) == rev {
			logrus.Tracef("WATCH PROGRESS TICK server=%d, id=%d, revision=%d synced", w.id, id, rev)
			select {
			case progressCh <- rev:
			default:
			}
		}
	}
}
