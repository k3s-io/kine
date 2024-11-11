package sqllog

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/k3s-io/kine/pkg/broadcaster"
	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	compactInterval  = 5 * time.Minute
	compactTimeout   = 5 * time.Second
	compactMinRetain = 1000
	compactBatchSize = 1000
	pollBatchSize    = 500
)

type SQLLog struct {
	d           server.Dialect
	broadcaster broadcaster.Broadcaster
	ctx         context.Context
	notify      chan int64
	currentRev  int64
}

func New(d server.Dialect) *SQLLog {
	l := &SQLLog{
		d:      d,
		notify: make(chan int64, 1024),
	}
	return l
}

func (s *SQLLog) Start(ctx context.Context) error {
	s.ctx = ctx
	return s.compactStart(s.ctx)
}

func (s *SQLLog) compactStart(ctx context.Context) error {
	logrus.Tracef("COMPACTSTART")

	rows, err := s.d.After(ctx, "compact_rev_key", 0, 0)
	if err != nil {
		return err
	}

	_, _, events, err := RowsToEvents(rows)
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
	logrus.Tracef("COMPACT starting compactRev=%d targetCompactRev=%d", compactRev, targetCompactRev)

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}

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
			iterCompactRev += compactBatchSize
			if iterCompactRev > targetCompactRev {
				iterCompactRev = targetCompactRev
			}

			// only update the compacted and current revisions if they are valid,
			// but break out of the inner loop on any error.
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
			logrus.Infof("COMPACT compacted from %d to %d in %d transactions over %s", compactRev, compactedRev, iterCount, time.Now().Sub(iterStart).Round(time.Millisecond))

			// post-compact operation errors are not critical, but should be reported
			if perr := s.postCompact(); perr != nil {
				logrus.Errorf("Post-compact operations failed: %v", perr)
			}
		}

		// Store the final results for this compact interval.
		// Note that one or more of the small-batch compact transactions
		// may have succeeded and moved the compact revision forward, even if err is non-nil.
		compactRev = compactedRev
		targetCompactRev = currentRev

		// ErrCompacted indicates that no further work is necessary - either compactRev changed since the
		// last iteration because another client has compacted, or the requested revision has already been compacted.
		if err != nil && err != server.ErrCompacted {
			logrus.Errorf("Compact failed: %v", err)
			resultLabel = metrics.ResultError
		}
		metrics.CompactTotal.WithLabelValues(resultLabel).Inc()
	}
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
	ctx, cancel := context.WithTimeout(s.ctx, compactTimeout)
	defer cancel()

	t, err := s.d.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to begin transaction")
	}
	defer t.MustRollback()

	currentRev, err := t.CurrentRevision(s.ctx)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to get current revision")
	}

	dbCompactRev, err := t.GetCompactRevision(s.ctx)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to get compact revision")
	}

	// Check to see if another node already compacted. This is normal on a multi-server cluster.
	if compactRev != dbCompactRev {
		logrus.Infof("COMPACT compact revision changed since last iteration: %d => %d", compactRev, dbCompactRev)
		return dbCompactRev, currentRev, server.ErrCompacted
	}

	// Ensure that we never compact the most recent 1000 revisions
	targetCompactRev = safeCompactRev(targetCompactRev, currentRev)

	// Don't bother compacting to a revision that has already been compacted
	if targetCompactRev <= compactRev {
		logrus.Tracef("COMPACT revision %d has already been compacted", targetCompactRev)
		return dbCompactRev, currentRev, server.ErrCompacted
	}

	logrus.Infof("COMPACT compactRev=%d targetCompactRev=%d currentRev=%d", compactRev, targetCompactRev, currentRev)

	start := time.Now()
	deletedRows, err := t.Compact(s.ctx, targetCompactRev)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "failed to compact to revision %d", targetCompactRev)
	}

	if err := t.SetCompactRevision(s.ctx, targetCompactRev); err != nil {
		return 0, 0, errors.Wrap(err, "failed to record compact revision")
	}

	// only commit the transaction if we make it all the way through deleting and
	// updating the compact revision without any errors. The deferred rollback
	// becomes a no-op if the transaction is committed.
	t.MustCommit()
	logrus.Infof("COMPACT deleted %d rows from %d revisions in %s - compacted to %d/%d", deletedRows, (targetCompactRev - compactRev), time.Since(start), targetCompactRev, currentRev)

	return targetCompactRev, currentRev, nil
}

// postCompact executes any post-compact database cleanup - vacuuming, WAL truncate, etc.
func (s *SQLLog) postCompact() error {
	return s.d.PostCompact(s.ctx)
}

func (s *SQLLog) CurrentRevision(ctx context.Context) (int64, error) {
	if s.currentRev != 0 {
		return s.currentRev, nil
	}
	return s.d.CurrentRevision(ctx)
}

func (s *SQLLog) CompactRevision(ctx context.Context) (int64, error) {
	return s.d.GetCompactRevision(ctx)
}

func (s *SQLLog) After(ctx context.Context, prefix string, revision, limit int64) (int64, []*server.Event, error) {
	if strings.HasSuffix(prefix, "/") {
		prefix += "%"
	}

	rows, err := s.d.After(ctx, prefix, revision, limit)
	if err != nil {
		return 0, nil, err
	}

	rev, compact, result, err := RowsToEvents(rows)

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

func (s *SQLLog) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted bool) (int64, []*server.Event, error) {
	var (
		rows *sql.Rows
		err  error
	)

	// It's assumed that when there is a start key that that key exists.
	if strings.HasSuffix(prefix, "/") {
		// In the situation of a list start the startKey will not exist so set to ""
		if prefix == startKey {
			startKey = ""
		}
		prefix += "%"
	} else {
		// Also if this isn't a list there is no reason to pass startKey
		startKey = ""
	}

	if revision == 0 {
		rows, err = s.d.ListCurrent(ctx, prefix, startKey, limit, includeDeleted)
	} else {
		rows, err = s.d.List(ctx, prefix, startKey, limit, revision, includeDeleted)
	}
	if err != nil {
		return 0, nil, err
	}

	rev, compact, result, err := RowsToEvents(rows)
	if err != nil {
		return 0, nil, err
	}

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

func RowsToEvents(rows *sql.Rows) (int64, int64, []*server.Event, error) {
	var (
		result  []*server.Event
		rev     int64
		compact int64
	)
	defer rows.Close()

	for rows.Next() {
		event := &server.Event{}
		if err := scan(rows, &rev, &compact, event); err != nil {
			return 0, 0, nil, err
		}
		result = append(result, event)
	}

	return rev, compact, result, nil
}

func (s *SQLLog) Watch(ctx context.Context, prefix string) <-chan []*server.Event {
	res := make(chan []*server.Event, 100)
	values, err := s.broadcaster.Subscribe(ctx, s.startWatch)
	if err != nil {
		return nil
	}

	checkPrefix := strings.HasSuffix(prefix, "/")

	go func() {
		defer close(res)
		for i := range values {
			events, ok := filter(i, checkPrefix, prefix)
			if ok {
				res <- events
			}
		}
	}()

	return res
}

func filter(events interface{}, checkPrefix bool, prefix string) ([]*server.Event, bool) {
	eventList := events.([]*server.Event)
	filteredEventList := make([]*server.Event, 0, len(eventList))

	for _, event := range eventList {
		if (checkPrefix && strings.HasPrefix(event.KV.Key, prefix)) || event.KV.Key == prefix {
			filteredEventList = append(filteredEventList, event)
		}
	}

	return filteredEventList, len(filteredEventList) > 0
}

func (s *SQLLog) startWatch() (chan interface{}, error) {
	pollStart, err := s.d.CurrentRevision(s.ctx)
	if err != nil {
		return nil, err
	}

	c := make(chan interface{})
	// start compaction and polling at the same time to watch starts
	// at the oldest revision, but compaction doesn't create gaps
	go s.compactor(compactInterval)
	go s.poll(c, pollStart)
	return c, nil
}

func (s *SQLLog) poll(result chan interface{}, pollStart int64) {
	s.currentRev = pollStart

	var (
		skip        int64
		skipTime    time.Time
		waitForMore = true
	)

	wait := time.NewTicker(time.Second)
	defer wait.Stop()
	defer close(result)

	for {
		if waitForMore {
			select {
			case <-s.ctx.Done():
				return
			case check := <-s.notify:
				if check <= s.currentRev {
					continue
				}
			case <-wait.C:
			}
		}
		waitForMore = true

		rows, err := s.d.After(s.ctx, "%", s.currentRev, pollBatchSize)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logrus.Errorf("fail to list latest changes: %v", err)
			}
			continue
		}

		_, _, events, err := RowsToEvents(rows)
		if err != nil {
			logrus.Errorf("fail to convert rows changes: %v", err)
			continue
		}

		logrus.Tracef("POLL AFTER %d, limit=%d, events=%d", s.currentRev, pollBatchSize, len(events))

		if len(events) == 0 {
			continue
		}

		waitForMore = len(events) < 100

		rev := s.currentRev
		var (
			sequential []*server.Event
			saveLast   bool
		)

		for _, event := range events {
			next := rev + 1
			// Ensure that we are notifying events in a sequential fashion. For example if we find row 4 before 3
			// we don't want to notify row 4 because 3 is essentially dropped forever.
			if event.KV.ModRevision != next {
				logrus.Tracef("MODREVISION GAP: expected %v, got %v", next, event.KV.ModRevision)
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
						logrus.Tracef("FILL, revision=%d, err=%v", next, err)
						select {
						case s.notify <- next:
						default:
						}
					} else {
						logrus.Tracef("FILL FAILED, revision=%d, err=%v", next, err)
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
				logrus.Tracef("NOT TRIGGER FILL %s, revision=%d, delete=%v", event.KV.Key, event.KV.ModRevision, event.Delete)
			} else {
				sequential = append(sequential, event)
				logrus.Tracef("TRIGGERED %s, revision=%d, delete=%v", event.KV.Key, event.KV.ModRevision, event.Delete)
			}
		}

		if saveLast {
			s.currentRev = rev
			if len(sequential) > 0 {
				result <- sequential
			}
		}
	}
}

func canSkipRevision(rev, skip int64, skipTime time.Time) bool {
	return rev == skip && time.Since(skipTime) > time.Second
}

func (s *SQLLog) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	if strings.HasSuffix(prefix, "/") {
		prefix += "%"
	}

	if revision == 0 {
		return s.d.CountCurrent(ctx, prefix, startKey)
	}
	return s.d.Count(ctx, prefix, startKey, revision)
}

func (s *SQLLog) Append(ctx context.Context, event *server.Event) (int64, error) {
	e := *event
	if e.KV == nil {
		e.KV = &server.KeyValue{}
	}
	if e.PrevKV == nil {
		e.PrevKV = &server.KeyValue{}
	}

	rev, err := s.d.Insert(ctx, e.KV.Key,
		e.Create,
		e.Delete,
		e.KV.CreateRevision,
		e.PrevKV.ModRevision,
		e.KV.Lease,
		e.KV.Value,
		e.PrevKV.Value,
	)
	if err != nil {
		return 0, err
	}
	select {
	case s.notify <- rev:
	default:
	}
	return rev, nil
}

func scan(rows *sql.Rows, rev *int64, compact *int64, event *server.Event) error {
	event.KV = &server.KeyValue{}
	event.PrevKV = &server.KeyValue{}

	c := &sql.NullInt64{}

	err := rows.Scan(
		rev,
		c,
		&event.KV.ModRevision,
		&event.KV.Key,
		&event.Create,
		&event.Delete,
		&event.KV.CreateRevision,
		&event.PrevKV.ModRevision,
		&event.KV.Lease,
		&event.KV.Value,
		&event.PrevKV.Value,
	)
	if err != nil {
		return err
	}

	if event.Create {
		event.KV.CreateRevision = event.KV.ModRevision
		event.PrevKV = nil
	}

	*compact = c.Int64
	return nil
}

// safeCompactRev ensures that we never compact the most recent 1000 revisions.
func safeCompactRev(targetCompactRev int64, currentRev int64) int64 {
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

func (s *SQLLog) Compact(ctx context.Context, revision int64) (int64, error) {
	return s.d.Compact(ctx, revision)
}
