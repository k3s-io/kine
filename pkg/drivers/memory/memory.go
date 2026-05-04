package memory

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/ttl"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/btree"
)

func init() {
	drivers.Register("memory", New)
}

func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	logrus.Info("using in-memory backend")
	return false, &Memory{
		keys:     btree.NewMap[string, []*entry](0),
		notifyCh: make(chan struct{}),
	}, nil
}

type entry struct {
	revision       int64
	key            string
	value          []byte
	createRevision int64
	version        int64
	prevRevision   int64
	lease          int64
	created        bool
	deleted        bool
	prev           *entry
}

func (e *entry) toKeyValue() *server.KeyValue {
	return &server.KeyValue{
		Key:            e.key,
		Value:          e.value,
		CreateRevision: e.createRevision,
		ModRevision:    e.revision,
		Version:        e.version,
		Lease:          e.lease,
	}
}

type Memory struct {
	mu              sync.RWMutex
	currentRevision atomic.Int64
	compactRevision int64

	log  []*entry
	keys *btree.Map[string, []*entry]

	notifyMu sync.Mutex
	notifyCh chan struct{}
}

// explicit interface check
var _ server.Backend = &Memory{}

func (m *Memory) Start(ctx context.Context) error {
	// Seed the same startup entries that SQL-backed and NATS backends do:
	//   1. compact_rev_key — written by SQLLog.compactStart; gives the backend
	//      a non-zero starting revision (apiserver rejects rev=0).
	//   2. /registry/health — written by LogStructured.Start; the apiserver
	//      uses it as a liveness probe.
	m.mu.Lock()
	rev := m.nextRevision()
	m.appendEntry(&entry{
		revision:       rev,
		key:            "compact_rev_key",
		createRevision: rev,
		version:        1,
		created:        true,
	})
	rev = m.nextRevision()
	m.appendEntry(&entry{
		revision:       rev,
		key:            "/registry/health",
		value:          []byte(`{"health":"true"}`),
		createRevision: rev,
		version:        1,
		created:        true,
	})
	m.mu.Unlock()

	go ttl.Run(ctx, m)
	return nil
}

func (m *Memory) CurrentRevision(ctx context.Context) (int64, error) {
	return m.currentRevision.Load(), nil
}

func (m *Memory) DbSize(ctx context.Context) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var size int64
	for _, e := range m.log {
		size += int64(len(e.key) + len(e.value) + 64)
	}
	return size, nil
}

func (m *Memory) WaitForSyncTo(revision int64) {}

// nextRevision allocates and returns the next revision. Increment is
// atomic; callers still need m.mu (write) when pairing the new revision
// with an appendEntry, so the log stays in revision order.
func (m *Memory) nextRevision() int64 {
	return m.currentRevision.Add(1)
}

// appendEntry adds e to the log and to its key's history, wiring up
// e.prev to point at the previous entry for the same key (if any).
// Caller must hold m.mu (write).
func (m *Memory) appendEntry(e *entry) {
	m.log = append(m.log, e)
	hist, _ := m.keys.Get(e.key)
	if len(hist) > 0 {
		e.prev = hist[len(hist)-1]
	}
	m.keys.Set(e.key, append(hist, e))
}

// broadcast wakes every goroutine currently blocked on getNotifyCh.
// Implemented as close-and-replace rather than sync.Cond.Broadcast so that
// waiters can select on ctx.Done() alongside the wake — sync.Cond.Wait()
// can't be combined with context cancellation without a helper goroutine.
func (m *Memory) broadcast() {
	m.notifyMu.Lock()
	close(m.notifyCh)
	m.notifyCh = make(chan struct{})
	m.notifyMu.Unlock()
}

// getNotifyCh returns a channel that will be closed on the next broadcast.
// Watchers should grab it BEFORE scanning the log so they don't miss a
// broadcast that fires while they're iterating.
func (m *Memory) getNotifyCh() <-chan struct{} {
	m.notifyMu.Lock()
	ch := m.notifyCh
	m.notifyMu.Unlock()
	return ch
}

// latest returns the most recent entry for key, or nil if none exists.
// The returned entry may be a tombstone (deleted == true).
// Caller must hold m.mu (read or write).
func (m *Memory) latest(key string) *entry {
	hist, ok := m.keys.Get(key)
	if !ok || len(hist) == 0 {
		return nil
	}
	return hist[len(hist)-1]
}

// atRevision returns the entry for key with the highest revision <= revision,
// or nil if no such entry exists.
// Caller must hold m.mu (read or write).
func (m *Memory) atRevision(key string, revision int64) *entry {
	hist, ok := m.keys.Get(key)
	if !ok {
		return nil
	}
	var result *entry
	for _, e := range hist {
		if e.revision <= revision {
			result = e
		} else {
			break
		}
	}
	return result
}

// logIndexAfter returns the index of the first log entry with revision > rev.
// The log is dense and ordered, with m.log[0].revision == m.compactRevision+1,
// so the index is just rev - m.compactRevision (clamped). For rev values at or
// below m.compactRevision the result is 0; for rev at or above the highest
// stored revision the result is len(m.log).
// Caller must hold m.mu (read or write).
func (m *Memory) logIndexAfter(rev int64) int {
	i := rev - m.compactRevision
	if i < 0 {
		i = 0
	}
	if i > int64(len(m.log)) {
		i = int64(len(m.log))
	}
	return int(i)
}

// Get returns the current revision and the KeyValue for the given key.
func (m *Memory) Get(ctx context.Context, key, rangeEnd string, limit, revision int64, keysOnly bool) (int64, *server.KeyValue, error) {
	if strings.HasSuffix(key, "/") && rangeEnd == "" {
		key = key[:len(key)-1]
	}
	rev, kvs, err := m.List(ctx, key, rangeEnd, limit, revision, keysOnly)
	if err != nil {
		return rev, nil, err
	}
	if len(kvs) == 0 {
		return rev, nil, nil
	}
	return rev, kvs[0], nil
}

func (m *Memory) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	latest := m.latest(key)
	if latest != nil && !latest.deleted {
		return m.currentRevision.Load(), server.ErrKeyExists
	}

	rev := m.nextRevision()
	var prevRev int64
	if latest != nil {
		prevRev = latest.revision
	}

	m.appendEntry(&entry{
		revision:       rev,
		key:            key,
		value:          value,
		createRevision: rev,
		version:        1,
		prevRevision:   prevRev,
		lease:          lease,
		created:        true,
	})
	m.broadcast()
	return rev, nil
}

func (m *Memory) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	latest := m.latest(key)
	if latest == nil || latest.deleted {
		return m.currentRevision.Load(), nil, false, nil
	}
	if latest.revision != revision {
		return m.currentRevision.Load(), latest.toKeyValue(), false, nil
	}

	rev := m.nextRevision()
	e := &entry{
		revision:       rev,
		key:            key,
		value:          value,
		createRevision: latest.createRevision,
		version:        latest.version + 1,
		prevRevision:   latest.revision,
		lease:          lease,
	}
	m.appendEntry(e)
	m.broadcast()
	return rev, e.toKeyValue(), true, nil
}

func (m *Memory) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	latest := m.latest(key)
	if latest == nil || latest.deleted {
		return m.currentRevision.Load(), nil, false, nil
	}
	if revision != 0 && latest.revision != revision {
		return m.currentRevision.Load(), latest.toKeyValue(), false, nil
	}

	rev := m.nextRevision()
	m.appendEntry(&entry{
		revision:       rev,
		key:            key,
		value:          latest.value,
		createRevision: latest.createRevision,
		version:        latest.version,
		prevRevision:   latest.revision,
		lease:          latest.lease,
		deleted:        true,
	})
	m.broadcast()
	return rev, latest.toKeyValue(), true, nil
}

func (m *Memory) List(ctx context.Context, prefix, startKey string, limit, revision int64, keysOnly bool) (int64, []*server.KeyValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rev := m.currentRevision.Load()
	if revision > 0 {
		if revision > rev {
			return rev, nil, server.ErrFutureRev
		}
		if revision < m.compactRevision {
			return rev, nil, server.ErrCompacted
		}
		rev = revision
	}

	seek, exact := seekKey(prefix, startKey)
	iter := m.keys.Iter()
	if !iter.Seek(seek) {
		return rev, nil, nil
	}

	var kvs []*server.KeyValue
	for {
		k := iter.Key()
		if exact && k != seek {
			break
		}
		if !strings.HasPrefix(k, prefix) {
			break
		}

		var e *entry
		if revision > 0 {
			e = m.atRevision(k, revision)
		} else {
			e = m.latest(k)
		}

		if e != nil && !e.deleted {
			kv := e.toKeyValue()
			if keysOnly {
				kv.Value = nil
			}
			kvs = append(kvs, kv)
		}

		if !iter.Next() {
			break
		}
	}
	return rev, kvs, nil
}

func (m *Memory) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	rev, kvs, err := m.List(ctx, prefix, startKey, 0, revision, true)
	if err != nil {
		return rev, 0, err
	}
	return rev, int64(len(kvs)), nil
}

func (m *Memory) Watch(ctx context.Context, prefix string, startRevision int64) server.WatchResult {
	m.mu.RLock()
	compactRev := m.compactRevision
	m.mu.RUnlock()
	rev := m.currentRevision.Load()

	events := make(chan []*server.Event, 100)

	if startRevision > 0 && startRevision <= compactRev {
		close(events)
		return server.WatchResult{
			CurrentRevision: rev,
			CompactRevision: compactRev,
			Events:          events,
		}
	}

	go func() {
		defer close(events)

		// lastSeen is the highest revision already delivered to the caller.
		// Subsequent passes start from the first log entry whose revision is
		// greater than lastSeen, looked up via logIndexAfter — direct array
		// indexing by revision no longer holds once compaction trims m.log.
		lastSeen := startRevision - 1
		if lastSeen < 0 {
			lastSeen = rev
		}

		for {
			notifyCh := m.getNotifyCh()

			m.mu.RLock()
			var batch []*server.Event
			for i := m.logIndexAfter(lastSeen); i < len(m.log); i++ {
				e := m.log[i]
				if !strings.HasPrefix(e.key, prefix) {
					lastSeen = e.revision
					continue
				}

				event := &server.Event{
					Create: e.created,
					Delete: e.deleted,
					KV:     e.toKeyValue(),
					PrevKV: &server.KeyValue{ModRevision: e.prevRevision},
				}
				if e.prev != nil {
					event.PrevKV = e.prev.toKeyValue()
				}

				batch = append(batch, event)
				lastSeen = e.revision
			}
			m.mu.RUnlock()

			if len(batch) > 0 {
				select {
				case events <- batch:
				case <-ctx.Done():
					return
				}
			}

			select {
			case <-notifyCh:
			case <-ctx.Done():
				return
			}
		}
	}()

	watchRev := startRevision
	if watchRev <= 0 {
		watchRev = rev
	}

	return server.WatchResult{
		CurrentRevision: watchRev,
		Events:          events,
	}
}

// Compact discards events below the given revision. The log slice is trimmed
// to entries with revision > the compact target — events at or below the
// boundary are no longer accessible to watchers. Per-key history in m.keys is
// trimmed to its floor (the latest entry with revision <= the target) so that
// reads at the compact boundary still resolve to the correct value; if that
// floor is a tombstone the entire key history is removed. Entries with
// revision > the compact target are always retained.
func (m *Memory) Compact(ctx context.Context, revision int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rev := m.currentRevision.Load()
	if revision > rev {
		return rev, nil
	}
	if revision <= m.compactRevision {
		return rev, nil
	}

	var toRemove []string

	m.keys.Scan(func(key string, hist []*entry) bool {
		floorIdx := -1
		for i, e := range hist {
			if e.revision <= revision {
				floorIdx = i
			} else {
				break
			}
		}

		var newHist []*entry
		switch {
		case floorIdx < 0:
			// No entry at or below the compact boundary; keep everything.
			newHist = hist
		case hist[floorIdx].deleted:
			// Floor is a tombstone — drop it and everything before; readers
			// at the compact boundary correctly see the key as absent.
			newHist = hist[floorIdx+1:]
		default:
			newHist = hist[floorIdx:]
		}

		if len(newHist) == 0 {
			toRemove = append(toRemove, key)
		} else if len(newHist) != len(hist) {
			// The new first entry's predecessor is being dropped; clear the
			// pointer so the dropped entry can be garbage collected.
			newHist[0].prev = nil
			m.keys.Set(key, newHist)
		}
		return true
	})

	for _, k := range toRemove {
		m.keys.Delete(k)
	}

	// Trim the log. Because the log is dense and ordered by revision, the cut
	// point is just (revision - oldCompactRevision); everything before it has
	// revision <= the target and is no longer reachable via Watch.
	cut := int(revision - m.compactRevision)
	if cut > len(m.log) {
		cut = len(m.log)
	}
	for i := 0; i < cut; i++ {
		m.log[i] = nil
	}
	m.log = m.log[cut:]

	m.compactRevision = revision
	return rev, nil
}

// seekKey returns the BTree seek target for a List/Range query and a flag
// indicating whether the iteration should match the seek key exactly. When
// startKey is empty (or equal to prefix), iteration begins at prefix; if
// prefix has no trailing slash this is treated as an exact-key lookup.
// Otherwise startKey is normalized into the prefix's namespace and used as
// the resume point for paginated range scans.
func seekKey(prefix, startKey string) (string, bool) {
	exact := !strings.HasSuffix(prefix, "/") && startKey == ""
	if startKey == "" || startKey == prefix {
		return prefix, exact
	}
	base := strings.TrimSuffix(prefix, "/")
	startKey = strings.TrimPrefix(startKey, base)
	startKey = strings.TrimPrefix(startKey, "/")
	if startKey != "" {
		return base + "/" + startKey, false
	}
	return prefix, exact
}
