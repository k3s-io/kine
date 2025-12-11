package nats

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

type DeleteFn func(ctx context.Context, key string, seq int64) (int64, *server.KeyValue, bool, error)

type ExpireWatcher struct {
	heap   *ExpireHeap
	fn     DeleteFn
	wakeCh chan struct{}
	ctx    context.Context
}

func NewExpireWatcher(fn DeleteFn) *ExpireWatcher {
	return &ExpireWatcher{
		heap:   NewExpireHeap(),
		fn:     fn,
		wakeCh: make(chan struct{}, 1),
	}
}

func (w *ExpireWatcher) Add(key string, seq int64, expires time.Time) {
	if w == nil {
		return
	}

	w.heap.Add(&ExpireEntry{
		key:     key,
		seq:     seq,
		expires: expires,
	})
	w.signal()
}

func (w *ExpireWatcher) RemoveKey(key string) []*ExpireEntry {
	if w == nil {
		return nil
	}

	removed := w.heap.RemoveByKey(key)
	if len(removed) > 0 {
		w.signal()
	}
	return removed
}

func (w *ExpireWatcher) Start(ctx context.Context) {
	if w == nil {
		return
	}

	w.ctx = ctx

	var timer *time.Timer
	var timerC <-chan time.Time

	stopTimer := func() {
		if timer == nil {
			return
		}
		timer.Stop()
		timer = nil
		timerC = nil
	}

	go func() {
		for {
			next := w.heap.Peek()
			if next == nil {
				stopTimer()
			} else {
				wait := time.Until(next.expires)
				if wait <= 0 {
					w.processExpired()
					continue
				}

				if timer == nil {
					timer = time.NewTimer(wait)
					timerC = timer.C
				} else {
					timer.Reset(wait)
				}
			}

			select {
			case <-ctx.Done():
				stopTimer()
				return
			case <-w.wakeCh:
				continue
			case <-timerC:
				w.processExpired()
			}
		}
	}()

	logrus.Infof("started expire watcher")
}

func (w *ExpireWatcher) processExpired() {
	now := time.Now()

	for {
		entry := w.heap.Next(now)

		if entry == nil || entry.expires.After(now) {
			return
		}

		if w.fn != nil {
			_, _, _, err := w.fn(w.ctx, entry.key, entry.seq)
			if err != nil {
				logrus.Errorf("error deleting expired key: %s, err=%s", entry.key, err)
			}
		}
	}
}

func (w *ExpireWatcher) signal() {
	if w == nil || w.wakeCh == nil {
		return
	}

	select {
	case w.wakeCh <- struct{}{}:
	default:
	}
}

type ExpireEntry struct {
	key     string
	seq     int64
	expires time.Time
}

type ExpireHeap struct {
	entries []*ExpireEntry
	mutex   sync.RWMutex
}

func NewExpireHeap() *ExpireHeap {
	h := &ExpireHeap{
		entries: make([]*ExpireEntry, 0),
	}
	heap.Init(h)
	return h
}

func (h *ExpireHeap) Len() int {
	return len(h.entries)
}

func (h *ExpireHeap) Less(i, j int) bool {
	return h.entries[i].expires.Before(h.entries[j].expires)
}

func (h *ExpireHeap) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
}

func (h *ExpireHeap) Push(x any) {
	entry, ok := x.(*ExpireEntry)
	if !ok {
		return
	}
	h.entries = append(h.entries, entry)
}

func (h *ExpireHeap) Pop() any {
	old := h.entries
	n := len(old)
	entry := old[n-1]
	h.entries = old[0 : n-1]
	return entry
}

func (h *ExpireHeap) Add(entry *ExpireEntry) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	heap.Push(h, entry)
}

func (h *ExpireHeap) Remove() *ExpireEntry {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.Len() == 0 {
		return nil
	}

	entry, ok := heap.Pop(h).(*ExpireEntry)
	if !ok {
		return nil
	}

	return entry
}

// Peek returns the next entry to expire, without removing it from the heap.
func (h *ExpireHeap) Peek() *ExpireEntry {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	if h.Len() == 0 {
		return nil
	}

	return h.entries[0]
}

func (h *ExpireHeap) Next(expired time.Time) *ExpireEntry {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.Len() == 0 {
		return nil
	}

	next := h.entries[0]
	if next.expires.After(expired) {
		return nil
	}

	entry, ok := heap.Pop(h).(*ExpireEntry)
	if !ok {
		return nil
	}

	return entry
}

func (h *ExpireHeap) RemoveByKey(key string) []*ExpireEntry {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	var removed []*ExpireEntry
	var remaining []*ExpireEntry

	for _, entry := range h.entries {
		if entry.key == key {
			removed = append(removed, entry)
		} else {
			remaining = append(remaining, entry)
		}
	}

	if len(removed) > 0 {
		h.entries = remaining
		heap.Init(h)
	}

	return removed
}

func (h *ExpireHeap) Size() int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.Len()
}

func (h *ExpireHeap) IsEmpty() bool {
	return h.Size() == 0
}
