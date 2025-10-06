package app

import (
	"fmt"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/endpoint"
	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/k3s-io/kine/pkg/signals"
	"github.com/k3s-io/kine/pkg/version"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var (
	config                 endpoint.Config
	metricsConfig          metrics.Config
	metricsIgnoreTLSConfig bool
)

func New() *cli.App {
	app := cli.NewApp()
	app.Name = "kine"
	app.Usage = "Minimal etcd v3 API to support custom Kubernetes storage engines"
	app.Version = fmt.Sprintf("%s (%s)", version.Version, version.GitCommit)
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:        "listen-address",
			Value:       "0.0.0.0:2379",
			Destination: &config.Listener,
			EnvVars:     []string{"KINE_LISTEN_ADDRESS"},
		},
		&cli.StringFlag{
			Name:        "endpoint",
			Usage:       "Storage endpoint (default is sqlite)",
			Destination: &config.Endpoint,
			EnvVars:     []string{"KINE_ENDPOINT"},
		},
		&cli.StringFlag{
			Name:        "identity-provider",
			Usage:       "Identity provider for temporary credentials (valid values are: aws)",
			Destination: &config.IdentityProvider,
			EnvVars:     []string{"KINE_IDENTITY_PROVIDER"},
		},
		&cli.StringFlag{
			Name:        "ca-file",
			Usage:       "CA cert for DB connection",
			Destination: &config.BackendTLSConfig.CAFile,
			EnvVars:     []string{"KINE_CA_FILE"},
		},
		&cli.StringFlag{
			Name:        "cert-file",
			Usage:       "Certificate for DB connection",
			Destination: &config.BackendTLSConfig.CertFile,
			EnvVars:     []string{"KINE_CERT_FILE"},
		},
		&cli.StringFlag{
			Name:        "key-file",
			Usage:       "Key file for DB connection",
			Destination: &config.BackendTLSConfig.KeyFile,
			EnvVars:     []string{"KINE_KEY_FILE"},
		},
		&cli.BoolFlag{
			Name:        "skip-verify",
			Usage:       "Whether the TLS client should verify the server certificate.",
			Destination: &config.BackendTLSConfig.SkipVerify,
			Value:       false,
			EnvVars:     []string{"KINE_SKIP_VERIFY"},
		},
		&cli.StringFlag{
			Name:        "log-format",
			Usage:       "Log format to use. Options are 'plain' or 'json'.",
			Destination: &config.LogFormat,
			Value:       "plain",
			EnvVars:     []string{"KINE_LOG_FORMAT"},
		},
		&cli.StringFlag{
			Name:        "metrics-bind-address",
			Usage:       "The address the metric endpoint binds to. Default :8080, set 0 to disable metrics serving.",
			Destination: &metricsConfig.ServerAddress,
			Value:       ":8080",
			EnvVars:     []string{"KINE_METRICS_BIND_ADDRESS"},
		},
		&cli.StringFlag{
			Name:        "server-cert-file",
			Usage:       "Certificate for etcd connection",
			Destination: &config.ServerTLSConfig.CertFile,
			EnvVars:     []string{"KINE_SERVER_CERT_FILE"},
		},
		&cli.StringFlag{
			Name:        "server-key-file",
			Usage:       "Key file for etcd connection",
			Destination: &config.ServerTLSConfig.KeyFile,
			EnvVars:     []string{"KINE_SERVER_KEY_FILE"},
		},
		&cli.IntFlag{
			Name:        "datastore-max-idle-connections",
			Usage:       "Maximum number of idle connections retained by datastore. If value = 0, the system default will be used. If value < 0, idle connections will not be reused.",
			Destination: &config.ConnectionPoolConfig.MaxIdle,
			Value:       0,
			EnvVars:     []string{"KINE_DATASTORE_MAX_IDLE_CONNECTIONS"},
		},
		&cli.IntFlag{
			Name:        "datastore-max-open-connections",
			Usage:       "Maximum number of open connections used by datastore. If value <= 0, then there is no limit",
			Destination: &config.ConnectionPoolConfig.MaxOpen,
			Value:       0,
			EnvVars:     []string{"KINE_DATASTORE_MAX_OPEN_CONNECTIONS"},
		},
		&cli.DurationFlag{
			Name:        "datastore-connection-max-lifetime",
			Usage:       "Maximum amount of time a connection may be reused. If value <= 0, then there is no limit.",
			Destination: &config.ConnectionPoolConfig.MaxLifetime,
			Value:       0,
			EnvVars:     []string{"KINE_DATASTORE_CONNECTION_MAX_LIFETIME"},
		},
		&cli.DurationFlag{
			Name:        "slow-sql-threshold",
			Usage:       "The duration which SQL executed longer than will be logged at level info. Default 1s, set <= 0 to disable slow SQL log.",
			Destination: &metrics.SlowSQLThreshold,
			Value:       time.Second,
			EnvVars:     []string{"KINE_SLOW_SQL_THRESHOLD"},
		},
		&cli.DurationFlag{
			Name:        "slow-sql-warning-threshold",
			Usage:       "The duration which SQL executed longer than will be logged at level warn. Default 5s.",
			Destination: &metrics.SlowSQLWarningThreshold,
			Value:       5 * time.Second,
			EnvVars:     []string{"KINE_SLOW_SQL_WARNING_THRESHOLD"},
		},
		&cli.BoolFlag{
			Name:        "metrics-enable-profiling",
			Usage:       "Enable net/http/pprof handlers on the metrics bind address. Default is false.",
			Destination: &metricsConfig.EnableProfiling,
			EnvVars:     []string{"KINE_METRICS_ENABLE_PROFILING"},
		},
		&cli.BoolFlag{
			Name:        "metrics-ignore-tls-config",
			Usage:       "Ignore TLS config for metrics server. Default is false.",
			Destination: &metricsIgnoreTLSConfig,
			Value:       false,
			EnvVars:     []string{"KINE_METRICS_IGNORE_TLS_CONFIG"},
		},
		&cli.DurationFlag{
			Name:        "watch-progress-notify-interval",
			Usage:       "Interval between periodic watch progress notifications. Default is 5s to ensure support for watch progress notifications.",
			Destination: &config.NotifyInterval,
			Value:       time.Second * 5,
			EnvVars:     []string{"KINE_WATCH_PROGRESS_NOTIFY_INTERVAL"},
		},
		&cli.StringFlag{
			Name:        "emulated-etcd-version",
			Usage:       "The emulated etcd version to return on a call to the status endpoint. Defaults to 3.5.13, in order to indicate support for watch progress notifications.",
			Destination: &config.EmulatedETCDVersion,
			Value:       "3.5.13",
			EnvVars:     []string{"KINE_EMULATED_ETCD_VERSION"},
		},
		&cli.DurationFlag{
			Name:        "compact-interval",
			Usage:       "Interval between automatic compaction. Default is 5m.",
			Destination: &config.CompactInterval,
			Value:       5 * time.Minute,
			EnvVars:     []string{"KINE_COMPACT_INTERVAL"},
		},
		&cli.IntFlag{
			Name:        "compact-interval-jitter",
			Usage:       "Percentage of jitter to apply to interval durations. A value of 10 will apply a jitter of +/-10 percent to the interval duration. It cannot be negative, and must be less than 100. Default is 0.",
			Destination: &config.CompactIntervalJitter,
			Value:       0,
			EnvVars:     []string{"KINE_COMPACT_INTERVAL_JITTER"},
		},
		&cli.DurationFlag{
			Name:        "compact-timeout",
			Usage:       "Timeout for automatic compaction. Default is 5s.",
			Destination: &config.CompactTimeout,
			Value:       5 * time.Second,
			EnvVars:     []string{"KINE_COMPACT_TIMEOUT"},
		},
		&cli.Int64Flag{
			Name:        "compact-min-retain",
			Usage:       "Minimum number of revisions to retain when compacting. Default is 1000.",
			Destination: &config.CompactMinRetain,
			Value:       1000,
			EnvVars:     []string{"KINE_COMPACT_MIN_RETAIN"},
		},
		&cli.Int64Flag{
			Name:        "compact-batch-size",
			Usage:       "Number of revisions to compact in a single batch. Default is 1000.",
			Destination: &config.CompactBatchSize,
			Value:       1000,
			EnvVars:     []string{"KINE_COMPACT_BATCH_SIZE"},
		},
		&cli.Int64Flag{
			Name:        "poll-batch-size",
			Usage:       "Number of revisions to poll in a single batch. Default is 500.",
			Destination: &config.PollBatchSize,
			Value:       500,
			EnvVars:     []string{"KINE_POLL_BATCH_SIZE"},
		},
		&cli.BoolFlag{
			Name:    "debug",
			EnvVars: []string{"KINE_DEBUG"},
		},
	}
	app.Action = run
	return app
}

func run(c *cli.Context) (rerr error) {
	if config.LogFormat == "plain" {
		logrus.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: time.RFC3339Nano,
		})
	} else if config.LogFormat == "json" {
		logrus.SetFormatter(&logrus.JSONFormatter{
			// To align with https://cloud.google.com/logging/docs/structured-logging
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyLevel: "severity",
				logrus.FieldKeyMsg:   "message",
			},
		})
	} else {
		return fmt.Errorf("invalid log format: %s", config.LogFormat)
	}

	if c.Bool("debug") {
		logrus.SetLevel(logrus.TraceLevel)
	}
	ctx := signals.SetupSignalContext()

	if !metricsIgnoreTLSConfig {
		metricsConfig.ServerTLSConfig = config.ServerTLSConfig
	}
	config.MetricsRegisterer = metrics.Registry
	metrics.RegisterCoreCollectors()

	config.WaitGroup = &sync.WaitGroup{}
	_, err := endpoint.Listen(ctx, config)
	if err != nil {
		return err
	}

	go metrics.Serve(ctx, metricsConfig)

	// Wait for WaitGroup to finish before exiting, and capture error from
	// context if it is not already set.
	defer func() {
		config.WaitGroup.Wait()
		if rerr == nil {
			rerr = ctx.Err()
		}
	}()

	return nil
}

// Config returns the endpoint config provided by parsing the provided CLI flags.
func Config(args []string) endpoint.Config {
	a := New()
	a.Action = func(*cli.Context) error { return nil }
	a.Run(append([]string{"kine"}, args...))
	return config
}
