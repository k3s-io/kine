package main

import (
	"context"
	"os"

	"github.com/pkg/math"
	"github.com/rancher/wrangler/pkg/signals"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/rancher/kine/pkg/endpoint"
)

var (
	config endpoint.Config
)

func main() {
	cli.VersionFlag = cli.BoolFlag{
		Name:  "version, V",
		Usage: "print the version",
	}

	app := cli.NewApp()
	app.Name = "kine"
	app.Description = "Minimal etcd v3 API to support custom Kubernetes storage engines"
	app.Usage = "Mini"
	app.Version = "0.4.0"

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
			Destination: &config.KeyFile,
		},
		cli.BoolFlag{
			Name:  "debug",
			Usage: "Enable debug mode for diagnostics",
		},
		cli.IntFlag{
			Name:        "v, verbose",
			Usage:       "Set the verbosity level (0=Panic Only, 1=Fatal, ..., 6=Trace)",
			Value:       -1,
			Destination: &config.Features.VerboseLevel,
		},
		cli.BoolFlag{
			Name:        "alpha-use-new-backend",
			Usage:       "Use the new experimental gorm based database backend",
			Destination: &config.Features.UseAlphaBackend,
		},
	}
	app.Action = run

	if err := app.Run(os.Args); err != nil {
		logrus.Fatal(err)
	}
}

func run(c *cli.Context) error {
	if c.Bool("debug") {
		if config.IsValidVerboseLevel() {
			logrus.Warn("you've requested verbose level and debug mode together yourself, whoever is more verbose will be superseding")
		}
		config.Features.VerboseLevel = math.Max(config.Features.VerboseLevel, int(logrus.DebugLevel))
	}

	if config.IsValidVerboseLevel() {
		logrus.SetLevel(logrus.Level(config.Features.VerboseLevel))
	}

	if config.Features.UseAlphaBackend {
		logrus.Warn("you are using an experimental backend powered by gorm. it is not guaranteed everything will work for now")
	}

	ctx := signals.SetupSignalHandler(context.Background())
	_, err := endpoint.Listen(ctx, config)
	if err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}
