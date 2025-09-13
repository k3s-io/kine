package http

import (
	"context"
	"sync"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
)

func New(_ context.Context, _ *sync.WaitGroup, _ *drivers.Config) (leaderElect bool, backend server.Backend, err error) {
	return true, nil, nil
}

func init() {
	drivers.Register("http", New)
	drivers.Register("https", New)
}
