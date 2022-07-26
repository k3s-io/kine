package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/k3s-io/kine/pkg/endpoint"
	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/k3s-io/kine/pkg/version"
	"github.com/rancher/wrangler/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

var (
	config        endpoint.Config
	metricsConfig metrics.Config
)

func main() {
	app := cli.NewApp()
	app.Name = "kine"
	app.Usage = "Minimal etcd v3 API to support custom Kubernetes storage engines"
	app.Version = fmt.Sprintf("%s (%s)", version.Version, version.GitCommit)
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "listen-address",
			Value:       "0.0.0.0:2379",
			Destination: &config.Listener,
		},
		cli.StringFlag{
			Name:        "endpoint",
			Usage:       "Storage endpoint (default is sqlite)",
			Destination: &config.Endpoint,
		},
		cli.StringFlag{
			Name:        "ca-file",
			Usage:       "CA cert for DB connection",
			Destination: &config.BackendTLSConfig.CAFile,
		},
		cli.StringFlag{
			Name:        "cert-file",
			Usage:       "Certificate for DB connection",
			Destination: &config.BackendTLSConfig.CertFile,
		},
		cli.StringFlag{
			Name:        "server-cert-file",
			Usage:       "Certificate for etcd connection",
			Destination: &config.ServerTLSConfig.CertFile,
		},
		cli.StringFlag{
			Name:        "server-key-file",
			Usage:       "Key file for etcd connection",
			Destination: &config.ServerTLSConfig.KeyFile,
		},
		cli.IntFlag{
			Name:        "datastore-max-idle-connections",
			Usage:       "Maximum number of idle connections retained by datastore. If value = 0, the system default will be used. If value < 0, idle connections will not be reused.",
			Destination: &config.ConnectionPoolConfig.MaxIdle,
			Value:       0,
		},
		cli.IntFlag{
			Name:        "datastore-max-open-connections",
			Usage:       "Maximum number of open connections used by datastore. If value <= 0, then there is no limit",
			Destination: &config.ConnectionPoolConfig.MaxOpen,
			Value:       0,
		},
		cli.DurationFlag{
			Name:        "datastore-connection-max-lifetime",
			Usage:       "Maximum amount of time a connection may be reused. If value <= 0, then there is no limit.",
			Destination: &config.ConnectionPoolConfig.MaxLifetime,
			Value:       0,
		},
		cli.StringFlag{
			Name:        "key-file",
			Usage:       "Key file for DB connection",
			Destination: &config.BackendTLSConfig.KeyFile,
		},
		cli.StringFlag{
			Name:        "metrics-bind-address",
			Usage:       "The address the metric endpoint binds to. Default :8080, set 0 to disable metrics serving.",
			Destination: &metricsConfig.ServerAddress,
			Value:       ":8080",
		},
		cli.DurationFlag{
			Name:        "slow-sql-threshold",
			Usage:       "The duration which SQL executed longer than will be logged. Default 1s, set <= 0 to disable slow SQL log.",
			Destination: &metrics.SlowSQLThreshold,
			Value:       time.Second,
		},
		cli.StringFlag{
			Name:        "data-dir",
			Usage:       "Storage path (default is ./ )",
			Destination: &config.DataDir,
		},
		cli.BoolFlag{Name: "debug"},
	}
	app.Action = run

	if err := app.Run(os.Args); err != nil {
		if !errors.Is(err, context.Canceled) {
			logrus.Fatal(err)
		}
	}
}

func run(c *cli.Context) error {
	if c.Bool("debug") {
		logrus.SetLevel(logrus.TraceLevel)
	}
	ctx := signals.SetupSignalHandler(context.Background())
	metricsConfig.ServerTLSConfig = config.ServerTLSConfig
	go metrics.Serve(ctx, metricsConfig)
	config.MetricsRegisterer = metrics.Registry
	_, err := endpoint.Listen(ctx, config)
	if err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}
