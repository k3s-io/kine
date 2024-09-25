package nats

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	natsserver "github.com/k3s-io/kine/pkg/drivers/nats/server"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
)

const (
	defaultBucket     = "kine"
	defaultReplicas   = 1
	defaultRevHistory = 10
	defaultSlowMethod = 500 * time.Millisecond
)

var (
	// Missing errors in the nats.go client library.
	jsClusterNotAvailErr = &jetstream.APIError{
		Code:      503,
		ErrorCode: 10008,
	}

	jsNoSuitablePeersErr = &jetstream.APIError{
		Code:      400,
		ErrorCode: 10005,
	}

	jsWrongLastSeqErr = &jetstream.APIError{
		Code:      400,
		ErrorCode: jetstream.JSErrCodeStreamWrongLastSequence,
	}
)

// New return an implementation of server.Backend using NATS + JetStream.
// See the `examples/nats.md` file for examples of connection strings.
func New(ctx context.Context, cfg *drivers.Config) (bool, server.Backend, error) {
	backend, err := newBackend(ctx, cfg.Endpoint, cfg.BackendTLSConfig, false)
	return true, backend, err
}

// NewLegacy return an implementation of server.Backend using NATS + JetStream
// with legacy jetstream:// behavior, ignoring the embedded server.
func NewLegacy(ctx context.Context, cfg *drivers.Config) (bool, server.Backend, error) {
	backend, err := newBackend(ctx, cfg.DataSourceName, cfg.BackendTLSConfig, true)
	return true, backend, err

}

func newBackend(ctx context.Context, connection string, tlsInfo tls.Config, legacy bool) (server.Backend, error) {
	config, err := parseConnection(connection, tlsInfo)
	if err != nil {
		return nil, err
	}

	nopts := append(
		config.clientOptions,
		nats.Name("kine using bucket: "+config.bucket),
		nats.MaxReconnects(-1),
	)

	// Run an embedded server if available and not disabled.
	var ns natsserver.Server
	cancel := func() {}

	if !legacy && natsserver.Embedded && !config.noEmbed {
		logrus.Infof("using an embedded NATS server")

		ns, err = natsserver.New(&natsserver.Config{
			Host:          config.host,
			Port:          config.port,
			ConfigFile:    config.serverConfig,
			DontListen:    config.dontListen,
			StdoutLogging: config.stdoutLogging,
			DataDir:       config.dataDir,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create embedded NATS server: %w", err)
		}

		if config.dontListen {
			nopts = append(nopts, nats.InProcessServer(ns))
		}

		// Start the server.
		go ns.Start()
		logrus.Infof("started embedded NATS server")
		time.Sleep(100 * time.Millisecond)

		// Wait for the server to be ready.
		var retries int
		for {
			if ns.Ready() {
				logrus.Infof("embedded NATS server is ready for client connections")
				break
			}
			retries++
			logrus.Infof("waiting for embedded NATS server to be ready: %d", retries)
			time.Sleep(100 * time.Millisecond)
		}

		// Use the local server's client URL.
		config.clientURL = ns.ClientURL()

		ctx, cancel = context.WithCancel(ctx)
	}

	if !config.dontListen {
		logrus.Infof("connecting to %s", config.clientURL)
	}

	logrus.Infof("using bucket: %s", config.bucket)

	nopts = append(nopts,
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logrus.Errorf("NATS disconnected: %s", err)
		}),
		nats.DiscoveredServersHandler(func(nc *nats.Conn) {
			logrus.Infof("NATS discovered servers: %v", nc.Servers())
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			logrus.Errorf("NATS error callback: %s", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logrus.Infof("NATS reconnected: %v", nc.ConnectedUrl())
		}),
	)

	nc, err := nats.Connect(config.clientURL, nopts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to NATS server: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to get JetStream context: %w", err)
	}

	bucket, err := getOrCreateBucket(ctx, js, config)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to get or create bucket: %w", err)
	}

	// Previous versions of KINE disabled direct gets on the bucket, however
	// that caused issues with `get` operations possibly timing out. This
	// check ensures that direct gets are enabled or enables them implicitly.
	if err := ensureDirectGets(ctx, js, config); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to enable direct gets: %w", err)
	}

	logrus.Infof("bucket initialized: %s", config.bucket)

	ekv := NewKeyValue(ctx, bucket, js)

	// Reference the global logger, since it appears log levels are
	// applied globally.
	l := logrus.StandardLogger()

	backend := Backend{
		nc:     nc,
		l:      l,
		kv:     ekv,
		js:     js,
		cancel: cancel,
	}

	if ns != nil {
		// TODO: No method on backend.Driver exists to indicate a shutdown.
		sigch := make(chan os.Signal, 1)
		signal.Notify(sigch, os.Interrupt)
		go func() {
			<-sigch
			backend.Close()
			ns.Shutdown()
			logrus.Infof("embedded NATS server shutdown")
		}()
	}

	return &BackendLogger{
		logger:    l,
		backend:   &backend,
		threshold: config.slowThreshold,
	}, nil
}

func getOrCreateBucket(ctx context.Context, js jetstream.JetStream, config *Config) (jetstream.KeyValue, error) {
	bucket, err := js.KeyValue(ctx, config.bucket)
	if err == nil {
		return bucket, nil
	}

	// If it does not exist, attempt to create it.
	for {
		bucket, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket:      config.bucket,
			Description: "Holds kine key/values",
			History:     config.revHistory,
			Replicas:    config.replicas,
		})
		if err == nil {
			return bucket, nil
		}

		// Check for timeout errors and retry.
		if errors.Is(err, context.DeadlineExceeded) {
			logrus.Warnf("timed out waiting for bucket %s to be created. retrying", config.bucket)
			continue
		}

		// Concurrent creation can cause this error.
		if jetstream.ErrStreamNameAlreadyInUse.APIError().Is(err) {
			return js.KeyValue(ctx, config.bucket)
		}

		// Check for temporary JetStream errors when the cluster is unhealthy and retry.
		if jsClusterNotAvailErr.Is(err) || jsNoSuitablePeersErr.Is(err) {
			logrus.Warn(err.Error())
			time.Sleep(time.Second)
			continue
		}

		// Some unexpected error.
		return nil, fmt.Errorf("failed to initialize KV bucket: %w", err)
	}
}

func ensureDirectGets(ctx context.Context, js jetstream.JetStream, config *Config) error {
	for {
		str, err := js.Stream(ctx, fmt.Sprintf("KV_%s", config.bucket))
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to get stream info: %w", err)
		}

		scfg := str.CachedInfo().Config

		// All good.
		if scfg.AllowDirect {
			return nil
		}

		scfg.AllowDirect = true

		_, err = js.UpdateStream(ctx, scfg)
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to update stream config: %w", err)
		}

		return nil
	}
}

func init() {
	drivers.Register("nats", New)
	drivers.Register("jetstream", NewLegacy)
}
