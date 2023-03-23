//go:build !dqlite
// +build !dqlite

package dqlite

import (
	"context"
	"errors"

	"github.com/AdamShannag/kine/pkg/drivers/generic"
	"github.com/AdamShannag/kine/pkg/server"
	"github.com/prometheus/client_golang/prometheus"
)

func New(ctx context.Context, datasourceName string, connPoolConfig generic.ConnectionPoolConfig, metricsRegisterer prometheus.Registerer) (server.Backend, error) {
	return nil, errors.New(`this binary is built without dqlite support, compile with "-tags dqlite"`)
}
