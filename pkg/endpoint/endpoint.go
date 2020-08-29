package endpoint

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/rancher/kine/pkg/alpha/gorm"
	"github.com/rancher/kine/pkg/alpha/gorm/compat"
	gormMssql "github.com/rancher/kine/pkg/drivers/alpha/gorm/mssql"
	gormMysql "github.com/rancher/kine/pkg/drivers/alpha/gorm/mysql"
	gormPgsql "github.com/rancher/kine/pkg/drivers/alpha/gorm/pgsql"
	gormSqlite "github.com/rancher/kine/pkg/drivers/alpha/gorm/sqlite"
	"github.com/rancher/kine/pkg/drivers/dqlite"
	"github.com/rancher/kine/pkg/drivers/generic"
	"github.com/rancher/kine/pkg/drivers/mysql"
	"github.com/rancher/kine/pkg/drivers/pgsql"
	"github.com/rancher/kine/pkg/drivers/sqlite"
	"github.com/rancher/kine/pkg/logstructured"
	"github.com/rancher/kine/pkg/logstructured/sqllog"
	"github.com/rancher/kine/pkg/server"
	"github.com/rancher/kine/pkg/tls"
)

const (
	KineSocket      = "unix://kine.sock"
	SQLiteBackend   = "sqlite"
	DQLiteBackend   = "dqlite"
	ETCDBackend     = "etcd3"
	MySQLBackend    = "mysql"
	PostgresBackend = "postgres"
	MsSQLBackend    = "sqlserver"
)

type Config struct {
	GRPCServer           *grpc.Server
	Listener             string
	Endpoint             string
	ConnectionPoolConfig generic.ConnectionPoolConfig
	Features             struct {
		VerboseLevel    int
		UseAlphaBackend bool
	}
	tls.Config
}

func (c *Config) IsValidVerboseLevel() bool {
	return c.Features.VerboseLevel >= 0
}

type ETCDConfig struct {
	Endpoints   []string
	TLSConfig   tls.Config
	LeaderElect bool
}

func Listen(ctx context.Context, config Config) (ETCDConfig, error) {
	driver, dsn := ParseStorageEndpoint(config.Endpoint)
	if driver == ETCDBackend {
		return ETCDConfig{
			Endpoints:   strings.Split(config.Endpoint, ","),
			TLSConfig:   config.Config,
			LeaderElect: true,
		}, nil
	}

	leaderelect, backend, err := getKineStorageBackend(ctx, driver, dsn, config)
	if err != nil {
		return ETCDConfig{}, errors.Wrap(err, "building kine")
	}

	if err := backend.Start(ctx); err != nil {
		return ETCDConfig{}, errors.Wrap(err, "starting kine backend")
	}

	listen := config.Listener
	if listen == "" {
		listen = KineSocket
	}

	b := server.New(backend)
	grpcServer := grpcServer(config)
	b.Register(grpcServer)

	listener, err := createListener(listen)
	if err != nil {
		return ETCDConfig{}, err
	}

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			logrus.Errorf("Kine server shutdown: %v", err)
		}
		<-ctx.Done()
		grpcServer.Stop()
		listener.Close()
	}()

	return ETCDConfig{
		LeaderElect: leaderelect,
		Endpoints:   []string{listen},
		TLSConfig:   tls.Config{},
	}, nil
}

func createListener(listen string) (ret net.Listener, rerr error) {
	network, address := networkAndAddress(listen)

	if network == "unix" {
		if err := os.Remove(address); err != nil && !os.IsNotExist(err) {
			logrus.Warnf("failed to remove socket %s: %v", address, err)
		}
		defer func() {
			if err := os.Chmod(address, 0600); err != nil {
				rerr = err
			}
		}()
	}

	logrus.Infof("Kine listening on %s://%s", network, address)
	return net.Listen(network, address)
}

func grpcServer(config Config) *grpc.Server {
	if config.GRPCServer != nil {
		return config.GRPCServer
	}
	return grpc.NewServer()
}

func getKineStorageBackend(ctx context.Context, driver, dsn string, cfg Config) (bool, server.Backend, error) {
	var (
		backend     server.Backend
		leaderElect = true
		err         error
	)

	if cfg.Features.UseAlphaBackend {
		if retLeaderElect, backend, err := createKineStorageAlphaBackend(ctx, driver, dsn, cfg); err == nil {
			return retLeaderElect, backend, err
		}
		logrus.Warn("unable to create alpha backend, falling back to use standard backend")
	}

	switch driver {
	case SQLiteBackend:
		leaderElect = false
		backend, err = sqlite.New(ctx, dsn, cfg.ConnectionPoolConfig)
	case DQLiteBackend:
		backend, err = dqlite.New(ctx, dsn, cfg.ConnectionPoolConfig)
	case PostgresBackend:
		backend, err = pgsql.New(ctx, dsn, cfg.Config, cfg.ConnectionPoolConfig)
	case MySQLBackend:
		backend, err = mysql.New(ctx, dsn, cfg.Config, cfg.ConnectionPoolConfig)
	default:
		return false, nil, fmt.Errorf("storage backend is not defined")
	}

	return leaderElect, backend, err
}

func createKineStorageAlphaBackend(ctx context.Context, driverPrefix string, dsn string, cfg Config) (leaderElect bool, backend server.Backend, err error) {
	var driver gorm.Driver
	switch driverPrefix {
	case SQLiteBackend:
		leaderElect = false
		driver = &gormSqlite.Driver{}
	case PostgresBackend:
		leaderElect = true
		driver = &gormPgsql.Driver{}
	case MySQLBackend:
		leaderElect = true
		driver = &gormMysql.Driver{}
	case MsSQLBackend:
		leaderElect = true
		driver = &gormMssql.Driver{}
	default:
		err = fmt.Errorf("storage backend is not defined")
		return
	}
	dsn, err = driver.PrepareDSN(dsn, cfg.Config)
	if err != nil {
		return
	}
	dialector := driver.GetOpenFunctor()(dsn)
	dbBackend, err := gorm.New(ctx, dialector, driver)
	if err != nil {
		return
	}
	compatBackend, err := compat.New(dbBackend)
	if err != nil {
		return
	}
	backend = logstructured.New(sqllog.New(compatBackend))
	return
}

func ParseStorageEndpoint(storageEndpoint string) (string, string) {
	network, address := networkAndAddress(storageEndpoint)
	switch network {
	case "":
		return SQLiteBackend, ""
	case "http":
		fallthrough
	case "https":
		return ETCDBackend, address
	}
	return network, address
}

func networkAndAddress(str string) (string, string) {
	parts := strings.SplitN(str, "://", 2)
	if len(parts) > 1 {
		return parts[0], parts[1]
	}
	return "", parts[0]
}
