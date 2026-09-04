package sqllog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k3s-io/kine/pkg/broadcaster"
	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

const minCompactBatchSize = 100

// maxPollStall is how long the poll loop may go without advancing before compaction stops holding
// its target back to the poll cursor. Under load the cursor should advance continuously, so this
// limit wouldn't be hit. When the datastore is idle it sits at the current revision and the limit
// isn't needed either. If it stops moving for this long the poll loop is assumed to be wedged for
// some unrelated reason, and compaction is allowed to proceed so that the database does not grow
// unbounded. The poll loop will then hit a compacted revision and restart the watch stream,
// matching what happens in the etcd-based code in k8s.io/apiserver.
const maxPollStall = time.Minute

// maxCompactHold bounds how far compaction may be held back from the revision it would otherwise
// compact to, while waiting on the poll loop. We need both this and maxPollStall because, under
// sustained overload the poll cursor keeps advancing, just more slowly than the write head, so the
// hold would grow without limit and take the size of the database with it. Past this distance the
// reader is not likely going to catch up, so compaction proceeds anyway and the poll loop takes the
// same compacted-revision restart it would have taken had we never held it back.
const maxCompactHold = 10000

type SQLLog struct {
	sync.RWMutex

	d           server.Dialect
	broadcaster broadcaster.Broadcaster
	ctx         context.Context
	notify      chan int64
	currentRev  atomic.Int64
	polledRev   atomic.Int64
	polled      *sync.Cond

	// polling is true while the poll loop is running; polledAt records the last time polledRev
	// actually advanced. Compaction consults both so that it does not delete rows the poll loop has
	// not read yet. See compact().
	polling  atomic.Bool
	polledAt atomic.Int64

	// compactorOnce guards the compactor goroutine. The poll loop can now exit
	// and be restarted by the broadcaster, which calls startWatch again; without
	// this we would leak an additional compactor on every restart.
	compactorOnce sync.Once

	compactInterval       time.Duration
	compactIntervalJitter int
	compactTimeout        time.Duration
	compactMinRetain      int64
	compactBatchSize      int64
	pollBatchSize         int64
}

func New(d server.Dialect, compactInterval time.Duration, compactIntervalJitter int, compactTimeout time.Duration, compactMinRetain int64, compactBatchSize int64, pollBatchSize int64) *SQLLog {
	l := &SQLLog{
		d:                     d,
		notify:                make(chan int64, 1024),
		compactInterval:       compactInterval,
		compactIntervalJitter: compactIntervalJitter,
		compactTimeout:        compactTimeout,
		compactMinRetain:      compactMinRetain,
		compactBatchSize:      compactBatchSize,
		pollBatchSize:         pollBatchSize,
	}
	l.polled = sync.NewCond(l.RLocker())
	return l
}

func (s *SQLLog) Start(ctx context.Context) error {
	if s.compactBatchSize < minCompactBatchSize {
		return fmt.Errorf("compact-batch-size %d too small: must be at least %d", s.compactBatchSize, minCompactBatchSize)
	}

	s.ctx = ctx
	return s.compactStart(s.ctx)
}

func (s *SQLLog) compactStart(ctx context.Context) error {
	logrus.Tracef("COMPACTSTART")

	rows, err := s.d.After(ctx, "compact_rev_key", "", 0, 0)
	if err != nil {
		return err
	}

	_, _, events, err := RowsToEvents(rows, true, true)
	if err != nil {
		return err
	}

	logrus.Tracef("COMPACTSTART len(events)=%v", len(events))

	if len(events) == 0 {
		_, err := s.Append(ctx, &server.Event{
			Create: true,
			KV: &server.KeyValue{
				Key:   "compact_rev_key",
				Value: []byte(""),
			},
		})
		return err
	} else if len(events) == 1 {
		return nil
	}

	t, err := s.d.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer t.MustRollback()

	// this is to work around a bug in which we ended up with two compact_rev_key rows
	maxRev := int64(0)
	maxID := int64(0)
	for _, event := range events {
		if event.PrevKV != nil && event.PrevKV.ModRevision > maxRev {
			maxRev = event.PrevKV.ModRevision
			maxID = event.KV.ModRevision
		}
		logrus.Tracef("COMPACTSTART maxRev=%v maxID=%v", maxRev, maxID)
	}

	for _, event := range events {
		logrus.Tracef("COMPACTSTART event.KV.ModRevision=%v maxID=%v", event.KV.ModRevision, maxID)
		if event.KV.ModRevision == maxID {
			continue
		}
		if err := t.DeleteRevision(ctx, event.KV.ModRevision); err != nil {
			return err
		}
	}

	return t.Commit()
}

// compactor periodically compacts historical versions of keys.
// It will compact keys with versions older than given interval, but never within the last 1000 revisions.
// In other words, after compaction, it will only contain key revisions set during last interval.
// Any API call for the older versions of keys will return error.
// Interval is the time interval between each compaction. The first compaction happens after "interval".
// This logic is directly cribbed from k8s.io/apiserver/pkg/storage/etcd3/compact.go
func (s *SQLLog) compactor(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	compactRev, _ := s.d.GetCompactRevision(s.ctx)
	targetCompactRev, _ := s.CurrentRevision(s.ctx)

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
		compactRev, targetCompactRev = s.compactIter(compactRev, targetCompactRev)
	}
}

func (s *SQLLog) compactIter(compactRev, targetCompactRev int64) (int64, int64) {
	logrus.Tracef("COMPACT running compactRev=%d targetCompactRev=%d", compactRev, targetCompactRev)
	// Break up the compaction into smaller batches to avoid locking the database with excessively
	// long transactions. When things are working normally deletes should proceed quite quickly, but if
	// run against a database where compaction has stalled (see rancher/k3s#1311) it may take a long time
	// (several hundred ms) just for the database to execute the subquery to select the revisions to delete.

	var (
		resultLabel    string
		iterCompactRev int64
		iterStart      time.Time
		iterCount      int64
		compactedRev   int64
		currentRev     int64
		err            error
	)

	resultLabel = metrics.ResultSuccess
	iterCompactRev = compactRev
	compactedRev = compactRev
	iterStart = time.Now()
	iterCount = 0

	for iterCompactRev < targetCompactRev {
		// Set move iteration target compactBatchSize revisions forward, or
		// just as far as we need to hit the compaction target if that would
		// overshoot it.
		iterCompactRev += s.compactBatchSize
		if iterCompactRev > targetCompactRev {
			iterCompactRev = targetCompactRev
		}

		// only update the compacted and current revisions if they are valid,
		// but break out of the loop on any error.
		compacted, current, cerr := s.compact(compactedRev, iterCompactRev)
		if compacted != 0 && current != 0 {
			compactedRev = compacted
			currentRev = current
		}
		if cerr != nil {
			err = cerr
			break
		}
		iterCount++
	}

	if iterCount > 0 {
		logrus.Infof("COMPACT compacted from %d to %d in %d transactions over %s", compactRev, compactedRev, iterCount, time.Since(iterStart).Round(time.Millisecond))

		// post-compact operation errors are not critical, but should be reported
		if perr := s.postCompact(); perr != nil {
			logrus.Errorf("Post-compact operations failed: %v", perr)
		}
	}

	// Only store the final results for this compact interval if currentRev is
	// updated to the current compact revision.
	//
	// Note that one or more of the small-batch compact transactions may have
	// succeeded and moved the compact revision forward, even if err is non-nil.
	if currentRev > 0 {
		compactRev = compactedRev
		targetCompactRev = currentRev
	}

	// ErrCompacted indicates that no further work is necessary - either compactRev changed since the
	// last iteration because another client has compacted, or the requested revision has already been compacted.
	if err != nil && err != server.ErrCompacted {
		logrus.Errorf("Compact failed: %v", err)
		resultLabel = metrics.ResultError
	}
	metrics.CompactTotal.WithLabelValues(resultLabel).Inc()

	return compactRev, targetCompactRev
}

// compact removes deleted or replaced rows from the database, and updates the compact rev key.
// compactRev is the current compact revision; targetCompactRev is the revision to compact to.
// If compactRev does not match what's in the database, we know that someone else has compacted and we don't need to do it.
// Deletion of rows and update of the compact rev key is done within a single transaction. The transaction is rolled back on any error.
//
// On success, the function returns the revision compacted to, and the revision that we should try to compact to next time (the current revision).
// ErrCompacted is returned if the current revision is stale, or the target revision has already been compacted.
// In this case the compact and current revisions from the database are returned.
// On any other error, the returned compact and current revisions should not be used.
//
// This logic is cribbed from k8s.io/apiserver/pkg/storage/etcd3/compact.go
func (s *SQLLog) compact(compactRev int64, targetCompactRev int64) (int64, int64, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s.compactTimeout)
	defer cancel()

	t, err := s.d.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer t.MustRollback()

	currentRev, err := t.CurrentRevision(s.ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get current revision: %w", err)
	}

	dbCompactRev, err := t.GetCompactRevision(s.ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get compact revision: %w", err)
	}

	// Check to see if another node already compacted. This is normal on a multi-server cluster.
	if compactRev != dbCompactRev {
		logrus.Infof("COMPACT compact revision changed since last iteration: %d => %d", compactRev, dbCompactRev)
		return dbCompactRev, currentRev, server.ErrCompacted
	}

	// Ensure that we never compact the most recent 1000 revisions
	targetCompactRev = safeCompactRev(targetCompactRev, currentRev, s.compactMinRetain)

	// Never compact past the poll loop's position while it is making progress. The poll loop is not
	// a client watcher that can be told to resync. It is this process' _only_ reader of the log,
	// and every watch is fed from it. Deleting rows it has not read yet leaves a permanent hole
	// that cannot be filled, causing a hang. See the actual poll() loop for why this is a problem
	// and how it is handled.
	if s.polling.Load() && time.Since(time.Unix(0, s.polledAt.Load())) < maxPollStall {
		if polledRev := s.polledRev.Load(); polledRev > 0 && targetCompactRev > polledRev {
			// Hold back to the poll cursor, but never by more than maxCompactHold.
			if heldRev := max(polledRev, targetCompactRev-maxCompactHold); heldRev < targetCompactRev {
				logrus.Debugf("COMPACT holding target revision %d back to %d for poll cursor %d", targetCompactRev, heldRev, polledRev)
				targetCompactRev = heldRev
			}
		}
	}

	// Don't bother compacting to a revision that has already been compacted
	if targetCompactRev <= compactRev {
		logrus.Tracef("COMPACT revision %d has already been compacted", targetCompactRev)
		return dbCompactRev, currentRev, server.ErrCompacted
	}

	logrus.Infof("COMPACT compactRev=%d targetCompactRev=%d currentRev=%d", compactRev, targetCompactRev, currentRev)

	start := time.Now()
	deletedRows, err := t.Compact(s.ctx, targetCompactRev)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to compact to revision %d: %w", targetCompactRev, err)
	}

	if err := t.SetCompactRevision(s.ctx, targetCompactRev); err != nil {
		return 0, 0, fmt.Errorf("failed to record compact revision: %w", err)
	}

	// only commit the transaction if we make it all the way through deleting and
	// updating the compact revision without any errors. The deferred rollback
	// becomes a no-op if the transaction is committed.
	t.MustCommit()
	logrus.Infof("COMPACT deleted %d rows from %d revisions in %s - compacted to %d/%d", deletedRows, (targetCompactRev - compactRev), time.Since(start), targetCompactRev, currentRev)

	return targetCompactRev, currentRev, nil
}

// postCompact executes any driver-specific cleanup after a successful compaction pass.
// The actual operation depends on the Dialect implementation; for example, the SQLite driver
// uses this to run a WAL checkpoint (PRAGMA wal_checkpoint) to flush committed pages back
// into the main database file (this does NOT reclaim free pages or shrink the database file;
// disk space reclamation is handled separately via VACUUM at startup).
func (s *SQLLog) postCompact() error {
	return s.d.PostCompact(s.ctx)
}

func (s *SQLLog) CurrentRevision(ctx context.Context) (int64, error) {
	currRev := s.currentRev.Load()
	if currRev != 0 {
		return currRev, nil
	}
	lastRev, err := s.d.CurrentRevision(ctx)
	if err != nil {
		return lastRev, err
	}
	if s.currentRev.CompareAndSwap(currRev, lastRev) {
		return lastRev, nil
	}
	return s.currentRev.Load(), nil
}

func (s *SQLLog) CompactRevision(ctx context.Context) (int64, error) {
	return s.d.GetCompactRevision(ctx)
}

func (s *SQLLog) After(ctx context.Context, key, end string, revision, limit int64) (int64, server.Events, error) {
	rows, err := s.d.After(ctx, key, end, revision, limit)
	if err != nil {
		return 0, nil, err
	}

	rev, compact, result, err := RowsToEvents(rows, true, true)

	if revision > 0 && len(result) == 0 {
		// a zero length result won't have the compact or current revisions so get them manually
		rev, err = s.CurrentRevision(ctx)
		if err != nil {
			return 0, nil, err
		}
		compact, err = s.d.GetCompactRevision(ctx)
		if err != nil {
			return 0, nil, err
		}
	}

	if revision > 0 && revision < compact {
		return rev, nil, server.ErrCompacted
	}

	return rev, result, err
}

func (s *SQLLog) List(ctx context.Context, key, end string, limit, revision int64, includeDeleted, keysOnly bool) (int64, server.Events, error) {
	var (
		rows *sql.Rows
		err  error
	)

	key = s.d.TranslateStartKey(key)

	if revision == 0 {
		rows, err = s.d.ListCurrent(ctx, key, end, limit, includeDeleted, keysOnly)
	} else {
		rows, err = s.d.List(ctx, key, end, limit, revision, includeDeleted, keysOnly)
	}
	if err != nil {
		return 0, nil, err
	}

	rev, compact, result, err := RowsToEvents(rows, !keysOnly, false)
	if err != nil {
		return 0, nil, err
	}

	if len(result) == 0 {
		// a zero length result won't have the compact or current revisions so get them manually
		rev, err = s.CurrentRevision(ctx)
		if err != nil {
			return 0, nil, err
		}
		compact, err = s.d.GetCompactRevision(ctx)
		if err != nil {
			return 0, nil, err
		}
	}

	if revision > rev {
		return rev, nil, server.ErrFutureRev
	}

	if revision > 0 && revision < compact {
		return rev, nil, server.ErrCompacted
	}

	select {
	case s.notify <- rev:
	default:
	}

	return rev, result, err
}

// rowsToEvents converts database rows to KV store events.
// if val is false, rows must not include the current value
// if prev is false, rows must additionally not include the previous value
func RowsToEvents(rows *sql.Rows, val, prev bool) (int64, int64, server.Events, error) {
	var (
		result  server.Events
		rev     int64
		compact int64
	)
	defer rows.Close()

	for rows.Next() {
		event, err := scan(rows, &rev, &compact, val, prev)
		if err != nil {
			return 0, 0, nil, err
		}
		result = append(result, event)
	}

	return rev, compact, result, nil
}

func (s *SQLLog) Watch(ctx context.Context, key, end string) <-chan server.Events {
	res := make(chan server.Events, 100)
	values, err := s.broadcaster.Subscribe(ctx, s.startWatch)
	if err != nil {
		return nil
	}

	go func() {
		defer close(res)
		for i := range values {
			if events, ok := filter(i, key, end); ok {
				res <- events
			}
		}
	}()

	return res
}

func filter(events server.Events, key, end string) (server.Events, bool) {
	// optimization: do not allocate a new Events slice to filter into if there is only a single entry
	if len(events) == 1 {
		if events[0].InRange(key, end) {
			return events, true
		}
		return nil, false
	}

	filteredEvents := make(server.Events, 0, len(events))
	for _, event := range events {
		if event.InRange(key, end) {
			filteredEvents = append(filteredEvents, event)
		}
	}

	return filteredEvents, len(filteredEvents) > 0
}

func (s *SQLLog) startWatch() (chan server.Events, error) {
	pollStart, err := s.d.CurrentRevision(s.ctx)
	if err != nil {
		return nil, err
	}

	c := make(chan server.Events)

	if s.compactIntervalJitter < 0 || s.compactIntervalJitter > 100 {
		panic("jitterPercent must be between 0 and 100")
	}
	maxJitter := float64(s.compactIntervalJitter) / 100.0 * float64(s.compactInterval)
	jitter := time.Duration(rand.Float64()*2*maxJitter - maxJitter)

	// The compactor is started only once for the lifetime of the SQLLog. The poll loop may exit and
	// be restarted (see poll()), which brings us back through startWatch, and a second compactor
	// would double-compact.
	if s.compactInterval <= 0 {
		logrus.Debugf("COMPACT disabled; automatic compaction will not occur")
	} else {
		s.compactorOnce.Do(func() {
			go s.compactor(s.compactInterval + jitter)
		})
	}

	go s.poll(c, pollStart)
	return c, nil
}

func (s *SQLLog) poll(result chan server.Events, pollStart int64) {
	var (
		skip         int64
		skipTime     time.Time
		waitForMore  = true
		pollRevision = pollStart
		lastPolled   = pollStart
		trace        = logrus.IsLevelEnabled(logrus.TraceLevel)
	)

	wait := time.NewTicker(time.Second)
	defer wait.Stop()
	defer close(result)

	// This is where we mark that the poll loop is running and record the last time it advanced. The
	// compactor consults both of these to avoid deleting rows that the poll loop has not yet read.
	s.polling.Store(true)
	s.polledAt.Store(time.Now().UnixNano())
	defer s.polling.Store(false)

	for {
		if waitForMore {
			select {
			case <-s.ctx.Done():
				return
			case check := <-s.notify:
				if check <= pollRevision {
					continue
				}
			case <-wait.C:
			}
		}
		waitForMore = true

		//  update polled revision to reflect what rows have already been seen
		s.Lock()
		if pollRevision != lastPolled {
			lastPolled = pollRevision
			s.polledAt.Store(time.Now().UnixNano())
		}
		s.polledRev.Store(pollRevision)
		s.polled.Broadcast()
		s.Unlock()

		rows, err := s.d.After(s.ctx, "", "", pollRevision, s.pollBatchSize)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logrus.Errorf("Failed to list latest changes: %v", err)
			}
			continue
		}

		_, compactRev, events, err := RowsToEvents(rows, true, true)
		if err != nil {
			logrus.Errorf("fail to convert rows changes: %v", err)
			continue
		}

		if trace {
			logrus.Tracef("POLL AFTER %d, limit=%d, events=%d", pollRevision, s.pollBatchSize, len(events))
		}

		if len(events) == 0 {
			continue
		}

		waitForMore = len(events) < 100

		var (
			rev        = pollRevision
			saveLast   = false
			sequential = make(server.Events, len(events))
		)

		for i, event := range events {
			next := rev + 1
			// Ensure that we are notifying events in a sequential fashion. For example if we find row 4 before 3
			// we don't want to notify row 4 because 3 is essentially dropped forever.
			if event.KV.ModRevision != next {
				if trace {
					logrus.Tracef("MODREVISION GAP: expected %v, got %v", next, event.KV.ModRevision)
				}
				// NOTE: If the missing revision is at or below the compact revision then its row
				// has been deleted and is never coming back. Filling the gap would advance the poll
				// cursor past a revision whose event was never broadcast, silently desyncing every
				// watcher. The major problem with this is that filling only advances the cursor one
				// revision per round trip (at least in the one implementation of the Dialect
				// interface). So in a busy system with lots of writers, this unintentionally
				// throttles this loop to a crawl by doing one insert at a time, which writes can
				// easily outpace. Essentially this leads to all watchers returning nothing (and any
				// new watchers also not working because it is all fed from this same poll)
				//
				// So we do what etcd does when a watcher falls behind the compact revision: kill
				// the poll by returning, which closes the broadcast, which cancels every watcher.
				// Clients re-establish, and Watch() returns ErrCompacted with the compact revision
				// for any client that is now below it, so the apiserver relists. A fresh poll loop
				// then starts at the current revision.
				if compactRev > 0 && next <= compactRev {
					logrus.Errorf("POLL revision %d has been compacted (compact revision %d); restarting watch stream so clients resync", next, compactRev)
					return
				}
				if canSkipRevision(next, skip, skipTime) {
					// This situation should never happen, but we have it here as a fallback just for unknown reasons
					// we don't want to pause all watches forever
					logrus.Errorf("GAP %s, revision=%d, delete=%v, next=%d", event.KV.Key, event.KV.ModRevision, event.Delete, next)
				} else if skip != next {
					// This is the first time we have encountered this missing revision, so record time start
					// and trigger a quick retry for simple out of order events
					skip = next
					skipTime = time.Now()
					select {
					case s.notify <- next:
					default:
					}
					// Some drivers increment the revision sequence at the start of the insert
					// transaction, but the row does not become visible to us until the transaction
					// completes. This looks like a skip, but creating a fill record too quickly
					// will cause the insert to fail and the transaction to roll back. Allow the
					// driver to inject an extra delay into the retry before filling.
					s.d.FillRetryDelay(s.ctx)
					break
				} else {
					if err := s.d.Fill(s.ctx, next); err == nil {
						if trace {
							logrus.Tracef("FILL, revision=%d, err=%v", next, err)
						}
						select {
						case s.notify <- next:
						default:
						}
					} else {
						if trace {
							logrus.Tracef("FILL FAILED, revision=%d, err=%v", next, err)
						}
					}
					break
				}
			}

			// we have done something now that we should save the last revision.  We don't save here now because
			// the next loop could fail leading to saving the reported revision without reporting it.  In practice this
			// loop right now has no error exit so the next loop shouldn't fail, but if we for some reason add a method
			// that returns error, that would be a tricky bug to find.  So instead we only save the last revision at
			// the same time we write to the channel.
			saveLast = true
			rev = event.KV.ModRevision
			if s.d.IsFill(event.KV.Key) {
				if trace {
					logrus.Tracef("BROADCAST SKIPPED FOR FILL %s, revision=%d, delete=%v", event.KV.Key, event.KV.ModRevision, event.Delete)
				}
			} else {
				sequential[i] = event
				if trace {
					logrus.Tracef("BROADCAST %s, revision=%d, delete=%v", event.KV.Key, event.KV.ModRevision, event.Delete)
				}
			}
		}

		if saveLast {
			s.currentRev.CompareAndSwap(pollRevision, rev)
			pollRevision = rev
			result <- sequential
		}
	}
}

func canSkipRevision(rev, skip int64, skipTime time.Time) bool {
	return rev == skip && time.Since(skipTime) > time.Second
}

func (s *SQLLog) Count(ctx context.Context, key, end string, revision int64) (int64, int64, error) {
	key = s.d.TranslateStartKey(key)

	if revision == 0 {
		return s.d.CountCurrent(ctx, key, end)
	}

	rev, compact, rows, err := s.d.Count(ctx, key, end, revision)
	if err != nil {
		return 0, 0, err
	}
	if revision > rev {
		return rev, 0, server.ErrFutureRev
	}
	if revision < compact {
		return rev, 0, server.ErrCompacted
	}
	return rev, rows, nil
}

func (s *SQLLog) Append(ctx context.Context, event *server.Event) (int64, error) {
	e := *event
	if e.KV == nil {
		e.KV = &server.KeyValue{}
	}
	if e.PrevKV == nil {
		e.PrevKV = &server.KeyValue{}
	}

	currentRev := s.currentRev.Load()
	rev, err := s.d.Insert(ctx, e.KV.Key,
		e.Create,
		e.Delete,
		e.KV.CreateRevision,
		e.PrevKV.ModRevision,
		e.KV.Lease,
		e.KV.Value,
	)
	if err != nil {
		return 0, err
	}

	// notify the polling loop of the new revision.
	select {
	case s.notify <- rev:
	default:
	}

	// currentRev may have moved ahead due to other inserts between when Insert returned
	// and now; ensure that we don't roll it back if it has changed elsewhere.
	s.currentRev.CompareAndSwap(currentRev, rev)

	return rev, nil
}

// scan scans the current row's columns into a server.Event struct.
// If a valid event is scanned, the passed rev and compact vars are also updated.
func scan(rows *sql.Rows, rev *int64, compact *int64, val, prev bool) (*server.Event, error) {
	event := &server.Event{
		KV:     &server.KeyValue{},
		PrevKV: &server.KeyValue{},
	}
	currentRev := &sql.NullInt64{}
	compactRev := &sql.NullInt64{}
	colCount := 9
	if val {
		colCount++
		if prev {
			colCount++
		}
	}

	dests := make([]any, colCount)
	dests[0] = currentRev
	dests[1] = compactRev
	dests[2] = &event.KV.ModRevision
	dests[3] = &event.KV.Key
	dests[4] = &event.Create
	dests[5] = &event.Delete
	dests[6] = &event.KV.CreateRevision
	dests[7] = &event.PrevKV.ModRevision
	dests[8] = &event.KV.Lease
	if val {
		dests[9] = &event.KV.Value
		if prev {
			dests[10] = &event.PrevKV.Value
		}
	}

	err := rows.Scan(dests...)
	if err != nil {
		return nil, err
	}

	if event.Create {
		event.KV.CreateRevision = event.KV.ModRevision
		event.PrevKV = nil
	} else {
		event.PrevKV.CreateRevision = event.KV.CreateRevision
		event.PrevKV.Lease = event.KV.Lease
		event.PrevKV.Key = event.KV.Key
	}

	*rev = currentRev.Int64
	*compact = compactRev.Int64
	return event, nil
}

// safeCompactRev ensures that we never compact the most recent 1000 revisions.
func safeCompactRev(targetCompactRev int64, currentRev int64, compactMinRetain int64) int64 {
	safeRev := currentRev - compactMinRetain
	if targetCompactRev < safeRev {
		safeRev = targetCompactRev
	}
	if safeRev < 0 {
		safeRev = 0
	}
	return safeRev
}

func (s *SQLLog) DbSize(ctx context.Context) (int64, error) {
	return s.d.GetSize(ctx)
}

func (s *SQLLog) Compact(ctx context.Context, targetCompactRev int64) (int64, error) {
	currentRev, _ := s.CurrentRevision(ctx)
	if targetCompactRev > currentRev {
		return 0, server.ErrFutureRev
	}
	compactRev, _ := s.d.GetCompactRevision(s.ctx)
	if targetCompactRev <= compactRev {
		return 0, server.ErrCompacted
	}
	// manual compact is a no-op unless automatic compaction is disabled
	if s.compactInterval <= 0 {
		s.compactIter(compactRev, targetCompactRev)
		return s.CurrentRevision(ctx)
	}
	return currentRev, nil
}

func (s *SQLLog) WaitForSyncTo(revision int64) {
	s.polled.L.Lock()
	for s.polledRev.Load() < revision {
		s.polled.Wait()
	}
	s.polled.L.Unlock()
}
