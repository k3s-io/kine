package endpoint

import (
	"context"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/identity"
	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/k3s-io/kine/pkg/util"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

const (
	KineSocket          = "unix://kine.sock"
	GracefulStopTimeout = 2 * time.Second
)

type Config struct {
	GRPCServer            *grpc.Server
	WaitGroup             *sync.WaitGroup
	Listener              string
	Endpoint              string
	ConnectionPoolConfig  generic.ConnectionPoolConfig
	ServerTLSConfig       tls.Config
	BackendTLSConfig      tls.Config
	MetricsRegisterer     prometheus.Registerer
	NotifyInterval        time.Duration
	EmulatedETCDVersion   string
	CompactInterval       time.Duration
	CompactIntervalJitter int
	CompactTimeout        time.Duration
	CompactMinRetain      int64
	CompactBatchSize      int64
	PollBatchSize         int64
	LogFormat             string
	IdentityProvider      string
}

type ETCDConfig struct {
	Endpoints   []string
	TLSConfig   tls.Config
	LeaderElect bool
}

func Listen(ctx context.Context, config Config) (etcd ETCDConfig, rerr error) {
	wg := waitGroup(config)
	bctx, bcancel := context.WithCancel(context.Background())
	defer func() {
		if rerr != nil {
			bcancel()
		}
	}()

	identityProvider, err := identity.New(bctx, config.IdentityProvider)
	if err != nil {
		return ETCDConfig{}, errors.Wrap(err, "failed to create identity provider for "+config.IdentityProvider)
	}

	leaderElect, backend, err := drivers.New(bctx, wg, &drivers.Config{
		MetricsRegisterer:     config.MetricsRegisterer,
		Endpoint:              config.Endpoint,
		BackendTLSConfig:      config.BackendTLSConfig,
		ConnectionPoolConfig:  config.ConnectionPoolConfig,
		CompactInterval:       config.CompactInterval,
		CompactIntervalJitter: config.CompactIntervalJitter,
		CompactTimeout:        config.CompactTimeout,
		CompactMinRetain:      config.CompactMinRetain,
		CompactBatchSize:      config.CompactBatchSize,
		PollBatchSize:         config.PollBatchSize,
		TokenSource:           identityProvider,
	})

	if err != nil {
		// Don't print the endpoint string in the error message as it may contain
		// credentials - but we do want to indicate whether the failure was in the
		// default or provided value.
		epType := "default endpoint"
		if config.Endpoint != "" {
			epType = "configured endpoint"
		}
		return ETCDConfig{}, errors.Wrap(err, "failed to create driver for "+epType)
	}

	if backend == nil {
		bcancel()
		return ETCDConfig{
			Endpoints:   strings.Split(config.Endpoint, ","),
			TLSConfig:   config.BackendTLSConfig,
			LeaderElect: leaderElect,
		}, nil
	}

	if config.MetricsRegisterer != nil {
		config.MetricsRegisterer.MustRegister(
			metrics.SQLTotal,
			metrics.SQLTime,
			metrics.CompactTotal,
			metrics.InsertErrorsTotal,
		)
	}

	grpcServer, err := grpcServer(config)
	if err != nil {
		return ETCDConfig{}, errors.Wrap(err, "creating GRPC server")
	}

	go func() {
		<-ctx.Done()
		logrus.Infof("Waiting up to %s for graceful shutdown of Kine GRPC server...", GracefulStopTimeout)
		timer := time.AfterFunc(GracefulStopTimeout, grpcServer.Stop)
		defer timer.Stop()
		grpcServer.GracefulStop()
		bcancel()
	}()

	if err := backend.Start(bctx); err != nil {
		return ETCDConfig{}, errors.Wrap(err, "starting kine backend")
	}

	// set up GRPC server and register services
	b := server.New(backend, endpointScheme(config), config.NotifyInterval, config.EmulatedETCDVersion)
	b.Register(grpcServer)

	// Create raw listener and wrap in cmux for protocol switching
	listener, err := createListener(bctx, config)
	if err != nil {
		return ETCDConfig{}, errors.Wrap(err, "creating listener")
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, context.Canceled) {
			logrus.Errorf("Kine GPRC server exited: %v", err)
		}
	}()

	endpoint := endpointURL(config, listener)
	logrus.Infof("Kine available at %s", endpoint)

	return ETCDConfig{
		LeaderElect: leaderElect,
		Endpoints:   []string{endpoint},
		TLSConfig: tls.Config{
			CAFile: config.ServerTLSConfig.CAFile,
		},
	}, nil
}

// endpointURL returns a URI string suitable for use as a local etcd endpoint.
// For TCP sockets, it is assumed that the port can be reached via the loopback address.
func endpointURL(config Config, listener net.Listener) string {
	scheme := endpointScheme(config)
	address := listener.Addr().String()
	if !strings.HasPrefix(scheme, "unix") {
		_, port, err := net.SplitHostPort(address)
		if err != nil {
			logrus.Warnf("failed to get listener port: %v", err)
			port = "2379"
		}
		address = "127.0.0.1:" + port
	}

	return scheme + "://" + address
}

// endpointScheme returns the URI scheme for the listener specified by the configuration.
func endpointScheme(config Config) string {
	if config.Listener == "" {
		config.Listener = KineSocket
	}

	scheme, _ := util.SchemeAndAddress(config.Listener)
	if scheme != "unix" {
		scheme = "http"
	}

	if config.ServerTLSConfig.CertFile != "" && config.ServerTLSConfig.KeyFile != "" {
		// yes, etcd supports the "unixs" scheme for TLS over unix sockets
		scheme += "s"
	}

	return scheme
}

// createListener returns a listener bound to the requested protocol and address.
func createListener(ctx context.Context, config Config) (ret net.Listener, rerr error) {
	if config.Listener == "" {
		config.Listener = KineSocket
	}
	scheme, address := util.SchemeAndAddress(config.Listener)

	if scheme == "unix" {
		if err := os.Remove(address); err != nil && !os.IsNotExist(err) {
			logrus.Warnf("failed to remove socket %s: %v", address, err)
		}
		defer func() {
			if err := os.Chmod(address, 0600); err != nil {
				rerr = err
			}
		}()
	} else {
		scheme = "tcp"
	}

	lc := net.ListenConfig{}
	return lc.Listen(ctx, scheme, address)
}

// waitGroup returns either the provided WaitGroup, or creates a new one
func waitGroup(config Config) *sync.WaitGroup {
	if config.WaitGroup != nil {
		return config.WaitGroup
	}
	return &sync.WaitGroup{}
}

// grpcServer returns either a preconfigured GRPC server, or builds a new GRPC
// server using upstream keepalive defaults plus the local Server TLS configuration.
func grpcServer(config Config) (*grpc.Server, error) {
	if config.GRPCServer != nil {
		return config.GRPCServer, nil
	}

	gopts := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             embed.DefaultGRPCKeepAliveMinTime,
			PermitWithoutStream: false,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    embed.DefaultGRPCKeepAliveInterval,
			Timeout: embed.DefaultGRPCKeepAliveTimeout,
		}),
	}

	if config.ServerTLSConfig.CertFile != "" && config.ServerTLSConfig.KeyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(config.ServerTLSConfig.CertFile, config.ServerTLSConfig.KeyFile)
		if err != nil {
			return nil, err
		}
		gopts = append(gopts, grpc.Creds(creds))
	}

	return grpc.NewServer(gopts...), nil
}
