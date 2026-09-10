package app

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/endpoint"
	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/k3s-io/kine/pkg/signals"
	"github.com/k3s-io/kine/pkg/version"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/writer"
	"github.com/urfave/cli/v3"
)

var (
	config                 endpoint.Config
	metricsConfig          metrics.Config
	metricsIgnoreTLSConfig bool
)

func New() *cli.Command {
	app := &cli.Command{
		Name:    "kine",
		Usage:   "Minimal etcd v3 API to support custom Kubernetes storage engines",
		Version: fmt.Sprintf("%s (%s)", version.Version, version.GitCommit),
	}
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:        "listen-address",
			Usage:       "Address to listen on for connections from etcd clients.",
			Value:       "0.0.0.0:2379",
			Destination: &config.Listener,
			Sources:     cli.EnvVars("KINE_LISTEN_ADDRESS"),
		},
		&cli.StringFlag{
			Name:        "endpoint",
			Usage:       "Storage endpoint (default is sqlite).",
			Destination: &config.Endpoint,
			Sources:     cli.EnvVars("KINE_ENDPOINT"),
		},
		&cli.StringFlag{
			Name:        "ca-file",
			Usage:       "CA cert for DB connection.",
			Destination: &config.BackendTLSConfig.CAFile,
			Sources:     cli.EnvVars("KINE_CA_FILE"),
		},
		&cli.StringFlag{
			Name:        "cert-file",
			Usage:       "Certificate for DB connection.",
			Destination: &config.BackendTLSConfig.CertFile,
			Sources:     cli.EnvVars("KINE_CERT_FILE"),
		},
		&cli.StringFlag{
			Name:        "key-file",
			Usage:       "Key file for DB connection.",
			Destination: &config.BackendTLSConfig.KeyFile,
			Sources:     cli.EnvVars("KINE_KEY_FILE"),
		},
		&cli.BoolFlag{
			Name:        "skip-verify",
			Usage:       "Whether the TLS client should verify the server certificate.",
			Destination: &config.BackendTLSConfig.SkipVerify,
			Value:       false,
			Sources:     cli.EnvVars("KINE_SKIP_VERIFY"),
		},
		&cli.StringFlag{
			Name:        "log-format",
			Usage:       "Log format to use; options are 'plain' or 'json'.",
			Destination: &config.LogFormat,
			Value:       "plain",
			Sources:     cli.EnvVars("KINE_LOG_FORMAT"),
		},
		&cli.StringFlag{
			Name:        "metrics-bind-address",
			Usage:       "The address the metric endpoint binds to. Default :8080, set 0 to disable metrics serving.",
			Destination: &metricsConfig.ServerAddress,
			Value:       ":8080",
			Sources:     cli.EnvVars("KINE_METRICS_BIND_ADDRESS"),
		},
		&cli.StringFlag{
			Name:        "server-cert-file",
			Usage:       "Certificate for etcd connection.",
			Destination: &config.ServerTLSConfig.CertFile,
			Sources:     cli.EnvVars("KINE_SERVER_CERT_FILE"),
		},
		&cli.StringFlag{
			Name:        "server-key-file",
			Usage:       "Key file for etcd connection.",
			Destination: &config.ServerTLSConfig.KeyFile,
			Sources:     cli.EnvVars("KINE_SERVER_KEY_FILE"),
		},
		&cli.StringFlag{
			Name:        "trusted-ca-file",
			Usage:       "CA certificate for verifying client certificates.",
			Destination: &config.ServerTLSConfig.TrustedCAFile,
			Sources:     cli.EnvVars("KINE_TRUSTED_CA_FILE"),
		},
		&cli.IntFlag{
			Name:        "datastore-max-idle-connections",
			Usage:       "Maximum number of idle connections retained by datastore. If value = 0, the system default will be used. If value < 0, idle connections will not be reused.",
			Destination: &config.ConnectionPoolConfig.MaxIdle,
			Value:       20,
			Sources:     cli.EnvVars("KINE_DATASTORE_MAX_IDLE_CONNECTIONS"),
		},
		&cli.IntFlag{
			Name:        "datastore-max-open-connections",
			Usage:       "Maximum number of open connections used by datastore. If value <= 0, then there is no limit.",
			Destination: &config.ConnectionPoolConfig.MaxOpen,
			Value:       0,
			Sources:     cli.EnvVars("KINE_DATASTORE_MAX_OPEN_CONNECTIONS"),
		},
		&cli.DurationFlag{
			Name:        "datastore-connection-max-lifetime",
			Usage:       "Maximum amount of time a connection may be reused. If value <= 0, then there is no limit.",
			Destination: &config.ConnectionPoolConfig.MaxLifetime,
			Value:       0,
			Sources:     cli.EnvVars("KINE_DATASTORE_CONNECTION_MAX_LIFETIME"),
		},
		&cli.DurationFlag{
			Name:        "datastore-connection-max-idle-time",
			Usage:       "Maximum amount of time a connection remain idle before being closed. If value <= 0, then there is no limit.",
			Destination: &config.ConnectionPoolConfig.MaxIdleTime,
			Value:       time.Minute * 2,
			Sources:     cli.EnvVars("KINE_DATASTORE_CONNECTION_MAX_IDLE_TIME"),
		},
		&cli.DurationFlag{
			Name:        "slow-sql-threshold",
			Usage:       "The duration which SQL executed longer than will be logged at level info. Default 1s, set <= 0 to disable slow SQL log.",
			Destination: &metrics.SlowSQLThreshold,
			Value:       time.Second,
			Sources:     cli.EnvVars("KINE_SLOW_SQL_THRESHOLD"),
		},
		&cli.DurationFlag{
			Name:        "slow-sql-warning-threshold",
			Usage:       "The duration which SQL executed longer than will be logged at level warn. Default 5s.",
			Destination: &metrics.SlowSQLWarningThreshold,
			Value:       5 * time.Second,
			Sources:     cli.EnvVars("KINE_SLOW_SQL_WARNING_THRESHOLD"),
		},
		&cli.BoolFlag{
			Name:        "metrics-enable-profiling",
			Usage:       "Enable net/http/pprof handlers on the metrics bind address. Default is false.",
			Destination: &metricsConfig.EnableProfiling,
			Sources:     cli.EnvVars("KINE_METRICS_ENABLE_PROFILING"),
		},
		&cli.BoolFlag{
			Name:        "metrics-ignore-tls-config",
			Usage:       "Ignore TLS config for metrics server. Default is false.",
			Destination: &metricsIgnoreTLSConfig,
			Value:       false,
			Sources:     cli.EnvVars("KINE_METRICS_IGNORE_TLS_CONFIG"),
		},
		&cli.DurationFlag{
			Name:        "watch-progress-notify-interval",
			Usage:       "Interval between periodic watch progress notifications. Default is 5s to ensure support for watch progress notifications.",
			Destination: &config.NotifyInterval,
			Value:       time.Second * 5,
			Sources:     cli.EnvVars("KINE_WATCH_PROGRESS_NOTIFY_INTERVAL"),
		},
		&cli.StringFlag{
			Name:        "emulated-etcd-version",
			Usage:       "The emulated etcd version to return on a call to the status endpoint. Defaults to 3.6.11, in order to indicate support for watch progress notifications.",
			Destination: &config.EmulatedETCDVersion,
			Value:       "3.6.11",
			Sources:     cli.EnvVars("KINE_EMULATED_ETCD_VERSION"),
		},
		&cli.DurationFlag{
			Name:        "compact-interval",
			Usage:       "Interval between automatic compaction. Default is off so that compact may be managed by the apiserver.",
			Destination: &config.CompactInterval,
			Value:       0,
			Sources:     cli.EnvVars("KINE_COMPACT_INTERVAL"),
		},
		&cli.IntFlag{
			Name:        "compact-interval-jitter",
			Usage:       "Percentage of jitter to apply to interval durations. A value of 10 will apply a jitter of +/-10 percent to the interval duration. It cannot be negative, and must be less than 100. Default is 0.",
			Destination: &config.CompactIntervalJitter,
			Value:       0,
			Sources:     cli.EnvVars("KINE_COMPACT_INTERVAL_JITTER"),
		},
		&cli.DurationFlag{
			Name:        "compact-timeout",
			Usage:       "Timeout for automatic compaction. Default is 5s.",
			Destination: &config.CompactTimeout,
			Value:       5 * time.Second,
			Sources:     cli.EnvVars("KINE_COMPACT_TIMEOUT"),
		},
		&cli.Int64Flag{
			Name:        "compact-min-retain",
			Usage:       "Minimum number of revisions to retain when compacting. Default is 1000.",
			Destination: &config.CompactMinRetain,
			Value:       1000,
			Sources:     cli.EnvVars("KINE_COMPACT_MIN_RETAIN"),
		},
		&cli.Int64Flag{
			Name:        "compact-batch-size",
			Usage:       "Number of revisions to compact in a single batch. Default is 1000.",
			Destination: &config.CompactBatchSize,
			Value:       1000,
			Sources:     cli.EnvVars("KINE_COMPACT_BATCH_SIZE"),
		},
		&cli.Int64Flag{
			Name:        "poll-batch-size",
			Usage:       "Number of revisions to poll in a single batch. Default is 500.",
			Destination: &config.PollBatchSize,
			Value:       500,
			Sources:     cli.EnvVars("KINE_POLL_BATCH_SIZE"),
		},
		&cli.StringFlag{
			Name:        "peer-bind-address",
			Usage:       "gRPC listen address (host:port) for the t4 peer WAL-streaming server. Empty means single-node mode. Example: 0.0.0.0:3380.",
			Destination: &config.PeerConfig.BindAddress,
			Sources:     cli.EnvVars("KINE_PEER_BIND_ADDRESS"),
		},
		&cli.StringFlag{
			Name:        "peer-advertise-address",
			Usage:       "Address other t4 nodes use to reach this node's peer server. Defaults to --peer-bind-address.",
			Destination: &config.PeerConfig.AdvertiseAddress,
			Sources:     cli.EnvVars("KINE_PEER_ADVERTISE_ADDRESS"),
		},
		&cli.StringFlag{
			Name:        "s3-bucket",
			Usage:       "S3 bucket for the t4 driver. Empty means local-only (no S3 durability).",
			Destination: &config.S3Config.Bucket,
			Sources:     cli.EnvVars("KINE_S3_BUCKET"),
		},
		&cli.StringFlag{
			Name:        "s3-folder",
			Usage:       "Optional key prefix inside the t4 driver's S3 bucket.",
			Destination: &config.S3Config.Folder,
			Sources:     cli.EnvVars("KINE_S3_FOLDER"),
		},
		&cli.StringFlag{
			Name:        "s3-endpoint",
			Usage:       "Custom S3-compatible endpoint URL for the t4 driver (MinIO, Ceph, etc.).",
			Destination: &config.S3Config.Endpoint,
			Sources:     cli.EnvVars("KINE_S3_ENDPOINT"),
		},
		&cli.StringFlag{
			Name:        "s3-region",
			Usage:       "AWS region for the t4 driver. Default is us-east-1.",
			Destination: &config.S3Config.Region,
			Sources:     cli.EnvVars("AWS_REGION", "AWS_DEFAULT_REGION"),
		},
		&cli.StringFlag{
			Name:        "s3-access-key",
			Usage:       "Static S3 access key for the t4 driver. Must be set together with --s3-secret-key.",
			Destination: &config.S3Config.AccessKey,
			Sources:     cli.EnvVars("AWS_ACCESS_KEY_ID"),
		},
		&cli.StringFlag{
			Name:        "s3-secret-key",
			Usage:       "Static S3 secret key for the t4 driver.",
			Destination: &config.S3Config.SecretKey,
			Sources:     cli.EnvVars("AWS_SECRET_ACCESS_KEY"),
		},
		&cli.StringFlag{
			Name:        "s3-session-token",
			Usage:       "Optional S3 session token for the t4 driver (for temporary STS credentials). Used only when --s3-access-key is also set.",
			Destination: &config.S3Config.SessionToken,
			Sources:     cli.EnvVars("AWS_SESSION_TOKEN"),
		},
		&cli.StringFlag{
			Name:        "s3-profile",
			Usage:       "AWS profile name to engage the ambient AWS credentials chain for the t4 driver (env vars, ~/.aws/credentials, IMDS). Ignored when --s3-access-key is set.",
			Destination: &config.S3Config.Profile,
			Sources:     cli.EnvVars("AWS_PROFILE"),
		},
		&cli.StringFlag{
			Name:        "s3-ca-bundle",
			Usage:       "Path to a PEM CA bundle used to trust HTTPS S3 endpoints for the t4 driver. Useful for MinIO behind a private CA.",
			Destination: &config.S3Config.CABundle,
			Sources:     cli.EnvVars("AWS_CA_BUNDLE"),
		},
		&cli.BoolFlag{
			Name:    "debug",
			Usage:   "Enable debug logging to stdout.",
			Sources: cli.EnvVars("KINE_DEBUG"),
		},
	}
	app.Action = run
	return app
}

func run(_ context.Context, cmd *cli.Command) (rerr error) {
	if cmd.Args().Len() != 0 {
		return fmt.Errorf("%s does not accept positional arguments, only flags", cmd.Root().Name)
	}

	if config.LogFormat == "plain" {
		logrus.SetFormatter(&logrus.TextFormatter{
			ForceColors:     true,
			FullTimestamp:   true,
			TimestampFormat: time.RFC3339Nano,
		})
	} else if config.LogFormat == "json" {
		logrus.SetFormatter(&logrus.JSONFormatter{
			// To align with https://cloud.google.com/logging/docs/structured-logging
			TimestampFormat: time.RFC3339Nano,
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyLevel: "severity",
				logrus.FieldKeyMsg:   "message",
			},
		})
	} else {
		return fmt.Errorf("invalid log format: %s", config.LogFormat)
	}

	// send info/error/warning to stderr
	logrus.SetOutput(ioutil.Discard)
	logrus.AddHook(&writer.Hook{Writer: os.Stderr, LogLevels: []logrus.Level{logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel, logrus.WarnLevel, logrus.InfoLevel}})

	if cmd.Bool("debug") {
		// send debug/trace to stdout
		logrus.SetLevel(logrus.TraceLevel)
		logrus.AddHook(&writer.Hook{Writer: os.Stdout, LogLevels: []logrus.Level{logrus.DebugLevel, logrus.TraceLevel}})
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
	a.Action = func(context.Context, *cli.Command) error { return nil }
	a.Run(context.Background(), append([]string{"kine"}, args...))
	return config
}
