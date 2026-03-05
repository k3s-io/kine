package remote

import (
	"context"
	"sync"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
)

// New returns a driver for a remote grpc socket. Kine will not build a new backend, but will
// simply pass the connection through to the specified endpoint.
func New(_ context.Context, _ *sync.WaitGroup, _ *drivers.Config) (leaderElect bool, backend server.Backend, err error) {
	return true, nil, nil
}

func init() {
	drivers.Register("http", New)
	drivers.Register("https", New)
	drivers.Register("unix", New)
	drivers.Register("unixs", New)
}
