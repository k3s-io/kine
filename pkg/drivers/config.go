package drivers

import (
	"time"

	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/identity"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/prometheus/client_golang/prometheus"
)

type Config struct {
	MetricsRegisterer     prometheus.Registerer
	Endpoint              string
	Scheme                string
	DataSourceName        string
	ConnectionPoolConfig  generic.ConnectionPoolConfig
	BackendTLSConfig      tls.Config
	CompactInterval       time.Duration
	CompactIntervalJitter int
	CompactTimeout        time.Duration
	CompactMinRetain      int64
	CompactBatchSize      int64
	PollBatchSize         int64
	TokenSource           identity.TokenSource
}
