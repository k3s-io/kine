//go:build cgo
// +build cgo

package sqlite

import (
	"context"
	"fmt"

	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus"

	// cgo sqlite driver
	_ "github.com/mattn/go-sqlite3"
)

func New(ctx context.Context, dataSourceName string, connPoolConfig generic.ConnectionPoolConfig, metricsRegisterer prometheus.Registerer) (server.Backend, error) {
	backend, _, err := NewVariant(ctx, "sqlite3", dataSourceName, connPoolConfig, metricsRegisterer)
	return backend, err
}

func translateError(err error) error {
	if err, ok := err.(sqlite3.Error); ok && err.ExtendedCode == sqlite3.ErrConstraintUnique {
		return server.ErrKeyExists
	}
	return err
}

func errCode(err error) string {
	if err == nil {
		return ""
	}
	if err, ok := err.(sqlite3.Error); ok {
		return fmt.Sprint(err.ExtendedCode)
	}
	return err.Error()
}
