package http

import (
	"context"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
)

func New(ctx context.Context, cfg *drivers.Config) (leaderElect bool, backend server.Backend, err error) {
	return true, nil, nil
}

func init() {
	drivers.Register("http", New)
	drivers.Register("https", New)
}
