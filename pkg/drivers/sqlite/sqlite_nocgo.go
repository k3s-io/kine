//go:build !cgo
// +build !cgo

package sqlite

import (
	"context"
	"fmt"

	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/prometheus/client_golang/prometheus"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	// pure go sqlite driver
	_ "modernc.org/sqlite"
)

func New(ctx context.Context, dataSourceName string, connPoolConfig generic.ConnectionPoolConfig, metricsRegisterer prometheus.Registerer) (server.Backend, error) {
	// https://gitlab.com/cznic/sqlite/-/blob/master/sqlite.go#L50
	// modernc.org/sqlite driver name is "sqlite"
	backend, _, err := NewVariant(ctx, "sqlite", dataSourceName, connPoolConfig, metricsRegisterer)
	return backend, err
}

func translateError(err error) error {
	// https://gitlab.com/cznic/sqlite/-/blob/master/sqlite.go#L780
	// modernc.org/sqlite error codes are the same as "ExtendedCode" in mattn/go-sqlite3
	if err, ok := err.(*sqlite.Error); ok && err.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE {
		return server.ErrKeyExists
	}
	return err
}

func errCode(err error) string {
	if err == nil {
		return ""
	}
	if err, ok := err.(*sqlite.Error); ok {
		return fmt.Sprint(err.Code())
	}
	return err.Error()
}
