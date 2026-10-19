//go:build kine_split_driver

package split

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

func init() {
	drivers.Register("split", New)
}

func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	logrus.Info("using split driver")
	dsn, driverName, ok := strings.Cut(cfg.DataSourceName, "&driver=")
	if !ok {
		return false, nil, errors.New("driver parameter not found")
	}
	constructor, ok := drivers.Get(driverName)
	if !ok {
		return false, nil, fmt.Errorf("no constructor for driver %s", driverName)
	}

	cfg.DataSourceName = dsn
	cfg.Endpoint = driverName + "://" + dsn
	cfg.MetricsRegisterer = nil

	logrus.Infof("Opening split %s writer for %s", driverName, dsn)
	_, writer, err := constructor(ctx, wg, cfg)
	if err != nil {
		return false, nil, fmt.Errorf("failed to create writer: %w", err)
	}
	logrus.Infof("Opening split %s reader for %s", driverName, dsn)
	_, reader, err := constructor(ctx, wg, cfg)
	if err != nil {
		return false, nil, fmt.Errorf("failed to create reader: %w", err)
	}
	return true, &Split{reader: reader, writer: writer}, nil
}

// explicit interface check
var _ server.Backend = &Split{}

// Split opens two identical drivers, and splits reads and writes across them.
// This allows simulating multiple kine servers sharing a common database, in a single process.
type Split struct {
	reader server.Backend
	writer server.Backend
}

// start both
func (s *Split) Start(ctx context.Context) error {
	return errors.Join(s.writer.Start(ctx), s.reader.Start(ctx))
}

// writer gets create/update/delete/compact
func (s *Split) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	return s.writer.Create(ctx, key, value, lease)
}
func (s *Split) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	return s.writer.Update(ctx, key, value, revision, lease)
}
func (s *Split) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	return s.writer.Delete(ctx, key, revision)
}
func (s *Split) Compact(ctx context.Context, revision int64) (int64, error) {
	return s.writer.Compact(ctx, revision)
}

// reader gets get/list/liststream/count/watch/dbsize/currentrevision
func (s *Split) Get(ctx context.Context, key string, revision int64, keysOnly bool) (int64, *server.KeyValue, error) {
	return s.reader.Get(ctx, key, revision, keysOnly)
}
func (s *Split) List(ctx context.Context, key, end string, limit, revision int64, keysOnly bool) (int64, []*server.KeyValue, error) {
	return s.reader.List(ctx, key, end, limit, revision, keysOnly)
}
func (s *Split) ListStream(ctx context.Context, key, end string, limit, revision int64, keysOnly bool) server.ListResult {
	return s.reader.ListStream(ctx, key, end, limit, revision, keysOnly)
}
func (s *Split) Count(ctx context.Context, key, end string, revision int64) (int64, int64, error) {
	return s.reader.Count(ctx, key, end, revision)
}
func (s *Split) Watch(ctx context.Context, revision int64) server.WatchResult {
	return s.reader.Watch(ctx, revision)
}
func (s *Split) DbSize(ctx context.Context) (int64, error) {
	return s.reader.DbSize(ctx)
}
func (s *Split) CurrentRevision(ctx context.Context) (int64, error) {
	return s.reader.CurrentRevision(ctx)
}
