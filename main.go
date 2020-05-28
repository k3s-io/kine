package main

import (
	"context"
	"os"
	"time"

	"github.com/rancher/kine/pkg/endpoint"
	"github.com/rancher/wrangler/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

var (
	config endpoint.Config
)

func main() {
	app := cli.NewApp()
	app.Name = "kine"
	app.Description = "Minimal etcd v3 API to support custom Kubernetes storage engines"
	app.Usage = "Mini"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "listen-address",
			Value:       "tcp://0.0.0.0:2379",
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
			Destination: &config.CAFile,
		},
		cli.StringFlag{
			Name:        "cert-file",
			Usage:       "Certificate for DB connection",
			Destination: &config.CertFile,
		},
		cli.IntFlag{
			Name:        "datastore-max-idle-connections",
			Usage:       "Maximum number of idle connections used by datastore. If num <= 0, then no connections are retained",
			Destination: &config.ConnectionPoolConfig.MaxIdle,
			Value:       2,
		},
		cli.IntFlag{
			Name:        "datastore-max-open-connections",
			Usage:       "Maximum number of idle connections used by datastore. If num <= 0, then there is no limit",
			Destination: &config.ConnectionPoolConfig.MaxOpen,
			Value:       0,
		},
		cli.DurationFlag{
			Name:        "datastore-connection-max-lifetime",
			Usage:       "Maximum amount of time a connection may be reused. Defined as a parsable string, e.g., 1s, 2500ms, and 1h30m are all accepted values",
			Destination: &config.ConnectionPoolConfig.MaxLifetime,
			Value:       time.Second,
		},
		cli.StringFlag{
			Name:        "key-file",
			Usage:       "Key file for DB connection",
			Destination: &config.KeyFile,
		},
		cli.BoolFlag{Name: "debug"},
	}
	app.Action = run

	if err := app.Run(os.Args); err != nil {
		logrus.Fatal(err)
	}
}

func run(c *cli.Context) error {
	if c.Bool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	}
	ctx := signals.SetupSignalHandler(context.Background())
	_, err := endpoint.Listen(ctx, config)
	if err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}
