package endpoint

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rancher/kine/pkg/drivers/dqlite"
	"github.com/rancher/kine/pkg/drivers/mysql"
	"github.com/rancher/kine/pkg/drivers/pgsql"
	"github.com/rancher/kine/pkg/drivers/sqlite"
	"github.com/rancher/kine/pkg/server"
	"github.com/rancher/kine/pkg/tls"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const (
	KineSocket      = "unix://kine.sock"
	SQLiteBackend   = "sqlite"
	DQLiteBackend   = "dqlite"
	ETCDBackend     = "etcd3"
	MySQLBackend    = "mysql"
	PostgresBackend = "postgres"
)

type Config struct {
	GRPCServer *grpc.Server
	Listener   string
	Endpoint   string

	tls.Config
}
type ETCDConfig struct {
	Endpoints   []string
	TLSConfig   tls.Config
	LeaderElect bool
}

func Listen(ctx context.Context, config Config) (ETCDConfig, error) {
	driver, dsn, err := ParseStorageEndpoint(config.Endpoint)
	if driver == ETCDBackend {
		return ETCDConfig{
			Endpoints:   strings.Split(config.Endpoint, ","),
			TLSConfig:   config.Config,
			LeaderElect: true,
		}, nil
	}
	if err != nil {
		return ETCDConfig{}, errors.Wrap(err, "parsing storage point")
	}

	type Response struct {
		leaderElect bool
		backend     server.Backend
		err         error
	}
	ch := make(chan Response, 1)

	go func() {
		leaderElect, backend, err := getKineStorageBackend(ctx, driver, dsn, config)
		res := Response{
			leaderElect: leaderElect,
			backend:     backend,
			err:         err,
		}
		ch <- res
	}()

	var (
		leaderElect bool
		backend     server.Backend
	)
	const storageDriveTimeout = time.Second * 10
	select {
	case res := <-ch:
		leaderElect = res.leaderElect
		backend = res.backend
		err = res.err

	case <-time.After(storageDriveTimeout):
		return ETCDConfig{}, errors.New("getKineDriver timed out, please check your connection/host port number")
	}

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
		LeaderElect: leaderElect,
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
	switch driver {
	case SQLiteBackend:
		leaderElect = false
		backend, err = sqlite.New(ctx, dsn)
	case DQLiteBackend:
		backend, err = dqlite.New(ctx, dsn)
	case PostgresBackend:
		backend, err = pgsql.New(ctx, dsn, cfg.Config)
	case MySQLBackend:
		backend, err = mysql.New(ctx, dsn, cfg.Config)
	default:
		return false, nil, fmt.Errorf("storage backend is not defined")
	}

	return leaderElect, backend, err
}

func ParseStorageEndpoint(storageEndpoint string) (string, string, error) {
	network, address := networkAndAddress(storageEndpoint)
	var err error
	switch network {
	case "":
		if address != "" {
			err = fmt.Errorf("invalid endpoint value")
		}
		return SQLiteBackend, "", err
	case "http":
		fallthrough
	case "https":
		return ETCDBackend, address, err
	}
	return network, address, err
}

func networkAndAddress(str string) (string, string) {
	parts := strings.SplitN(str, "://", 2)
	if len(parts) > 1 {
		return parts[0], parts[1]
	}
	return "", parts[0]
}
