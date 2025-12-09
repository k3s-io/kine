//go:build cgo
// +build cgo

package sqlite

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/mattn/go-sqlite3"
)

func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	dataSourceName := cfg.DataSourceName
	if dataSourceName == "" {
		if err := os.MkdirAll("./db", 0700); err != nil {
			return false, nil, err
		}
		dataSourceName = "./db/state.db?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate"
	}
	cfg.DataSourceName = dataSourceName
	backend, _, err := NewVariant(ctx, wg, "sqlite3", cfg)
	return false, backend, err
}

func translateErr(err error) error {
	if err, ok := err.(sqlite3.Error); ok && err.ExtendedCode == sqlite3.ErrConstraintUnique {
		return server.ErrKeyExists
	}
	return err
}

func errorCode(err error) string {
	if err == nil {
		return ""
	}
	if err, ok := err.(sqlite3.Error); ok {
		return fmt.Sprint(err.ExtendedCode)
	}
	return err.Error()
}
