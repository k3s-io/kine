//go:build !dqlite
// +build !dqlite

package dqlite

import (
	"context"
	"errors"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
)

func New(ctx context.Context, cfg *drivers.Config) (bool, server.Backend, error) {
	return false, nil, errors.New(`this binary is built without dqlite support, compile with "-tags dqlite"`)
}

func init() {
	drivers.Register("dqlite", New)
}
