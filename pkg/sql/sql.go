package sql

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/prometheus/common/log"
	"github.com/rancher/kine/pkg/backend"
	"github.com/rancher/kine/pkg/broadcaster"
	"github.com/sirupsen/logrus"
)

type Log struct {
	d           Driver
	broadcaster broadcaster.Broadcaster
	ctx         context.Context
	//notify      chan int64
}

func New(ctx context.Context, d Driver) *Log {
	l := &Log{
		d: d,
		//notify: make(chan int64, 1024),
	}
	l.Start(ctx)
	return l
}

type Driver interface {
	ListCurrent(ctx context.Context, prefix string, limit int64) (*sql.Rows, error)
	List(ctx context.Context, prefix, startKey string, limit, revision int64) (*sql.Rows, error)
	Count(ctx context.Context, prefix string) (int64, int64, error)
	CurrentRevision(ctx context.Context) (int64, error)
	Since(ctx context.Context, rev int64) (*sql.Rows, error)
	Insert(ctx context.Context, key string, create, delete bool, previousRevision int64, ttl int64, value, prevValue []byte) (int64, error)
	GetRevision(ctx context.Context, revision int64) (*sql.Rows, error)
	DeleteRevision(ctx context.Context, revision int64) error
	GetCompactRevision(ctx context.Context) (int64, error)
	SetCompactRevision(ctx context.Context, revision int64) error
}

func (s *Log) Start(ctx context.Context) {
	s.ctx = ctx
	go s.compact()
}

func (s *Log) compact() {
	t := time.NewTicker(2 * time.Second)

outer:
	for {
		end, err := s.d.CurrentRevision(s.ctx)
		if err != nil {
			logrus.Errorf("failed to get current revision: %v", err)
			continue
		}

		//end -= 100

		cursor, err := s.d.GetCompactRevision(s.ctx)
		if err != nil {
			logrus.Errorf("failed to get compact revision: %v", err)
			continue
		}

		// Purposefully start at the current and redo the current as
		// it could have failed before actually compacting

		for ; cursor <= end; cursor++ {
			rows, err := s.d.GetRevision(s.ctx, cursor+1)
			if err != nil {
				logrus.Errorf("failed to get revision %d: %v", cursor, err)
				continue outer
			}

			_, _, events, err := RowsToEvents(rows)
			if err != nil {
				logrus.Errorf("failed to convert to events: %v", err)
				continue outer
			}

			if len(events) == 0 {
				continue
			}

			event := events[0]

			if event.KV.Key == "compact_rev_key" {
				// don't compact the compact key
				continue
			}

			if event.PrevKV != nil && event.PrevKV.ModRevision != 0 {
				if err := s.d.DeleteRevision(s.ctx, event.PrevKV.ModRevision); err != nil {
					logrus.Errorf("failed to delete revision %d: %v", event.PrevKV.ModRevision, err)
					continue outer
				}
			}

			if event.Delete {
				if err := s.d.DeleteRevision(s.ctx, cursor); err != nil {
					logrus.Errorf("failed to delete current revision %d: %v", cursor, err)
					continue outer
				}
			}

			if err := s.d.SetCompactRevision(s.ctx, cursor+1); err != nil {
				logrus.Errorf("failed to record compact revision: %v", err)
				continue outer
			}
		}

		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}

	}
}

func (s *Log) List(ctx context.Context, prefix, startKey string, limit, revision int64) (int64, []*backend.Event, error) {
	var (
		rows *sql.Rows
		err  error
	)

	if strings.HasSuffix(prefix, "/") {
		prefix = prefix[:len(prefix)-1] + "%"
	}

	if revision == 0 {
		rows, err = s.d.ListCurrent(ctx, prefix, limit)
	} else {
		rows, err = s.d.List(ctx, prefix, startKey, limit, revision)
	}
	if err != nil {
		return 0, nil, err
	}

	rev, compact, result, err := RowsToEvents(rows)
	if revision > 0 && revision <= compact {
		return 0, nil, backend.ErrCompacted
	}
	//if err == nil {
	//	select {
	//	case s.notify <- rev:
	//	default:
	//	}
	//}
	return rev, result, err
}

func RowsToEvents(rows *sql.Rows) (int64, int64, []*backend.Event, error) {
	var (
		result  []*backend.Event
		rev     int64
		compact int64
	)
	defer rows.Close()

	for rows.Next() {
		event := &backend.Event{}
		if err := scan(rows, &rev, &compact, event); err != nil {
			return 0, 0, nil, err
		}
		result = append(result, event)
	}

	return rev, compact, result, nil
}

func (s *Log) Watch(ctx context.Context, prefix string) <-chan []*backend.Event {
	res := make(chan []*backend.Event)
	values, err := s.broadcaster.Subscribe(ctx, s.start)
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

func filter(events interface{}, checkPrefix bool, prefix string) ([]*backend.Event, bool) {
	eventList := events.([]*backend.Event)
	filteredEventList := make([]*backend.Event, 0, len(eventList))

	for _, event := range eventList {
		if (checkPrefix && strings.HasPrefix(event.KV.Key, prefix)) || event.KV.Key == prefix {
			filteredEventList = append(filteredEventList, event)
		}
	}

	return filteredEventList, len(filteredEventList) > 0
}

func (s *Log) start() (chan interface{}, error) {
	c := make(chan interface{})
	go s.poll(c)
	return c, nil
}

func (s *Log) poll(result chan interface{}) {
	var (
		last int64
		err  error
	)

	wait := time.NewTicker(time.Second)
	defer wait.Stop()
	defer close(result)

	for {
		if last == 0 {
			last, err = s.d.CurrentRevision(s.ctx)
			if err != nil {
				log.Errorf("failed to get current revision: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}
		}

		select {
		case <-s.ctx.Done():
			return
		//case check := <-s.notify:
		//	if check <= last {
		//		continue
		//	}
		case <-wait.C:
		}

		rows, err := s.d.Since(s.ctx, last)
		if err != nil {
			log.Errorf("fail to list lastest changes: %v", err)
			continue
		}

		rev, _, events, err := RowsToEvents(rows)
		if err != nil {
			log.Errorf("fail to convert rows changes: %v", err)
			continue
		}

		result <- events
		last = rev
	}
}

func (s *Log) Count(ctx context.Context, prefix string) (int64, int64, error) {
	if strings.HasSuffix(prefix, "/") {
		prefix = prefix[:len(prefix)-2] + "%"
	}
	return s.d.Count(ctx, prefix)
}

func (s *Log) Append(ctx context.Context, event *backend.Event) (int64, error) {
	e := *event
	if e.KV == nil {
		e.KV = &backend.KeyValue{}
	}
	if e.PrevKV == nil {
		e.PrevKV = &backend.KeyValue{}
	}

	rev, err := s.d.Insert(ctx, event.KV.Key,
		event.Create,
		event.Delete,
		event.PrevKV.ModRevision,
		event.KV.Lease,
		event.KV.Value,
		event.PrevKV.Value,
	)
	if err != nil {
		return 0, err
	}
	//select {
	//case s.notify <- rev:
	//default:
	//}
	return rev, nil
}

func scan(rows *sql.Rows, rev *int64, compact *int64, event *backend.Event) error {
	event.KV = &backend.KeyValue{}
	event.PrevKV = &backend.KeyValue{}

	c := &sql.NullInt64{}

	err := rows.Scan(
		rev,
		c,
		&event.KV.ModRevision,
		&event.KV.Key,
		&event.Create,
		&event.Delete,
		&event.PrevKV.ModRevision,
		&event.KV.Lease,
		&event.KV.Value,
		&event.PrevKV.Value,
	)
	if err != nil {
		return err
	}

	if event.Create {
		event.PrevKV = nil
	}

	*compact = c.Int64
	return nil
}
