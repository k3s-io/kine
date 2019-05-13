package main

import (
	"context"
	"net"

	"github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/rancher/kine/pkg/bridge"
	"github.com/rancher/kine/pkg/log"
	"github.com/rancher/kine/pkg/sql"
	"github.com/rancher/kine/pkg/sqlite"
	"github.com/rancher/wrangler/pkg/signals"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	// sqlite
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	if err := run(); err != nil {
		logrus.Fatal(err)
	}
}

func run() error {
	logrus.SetLevel(logrus.TraceLevel)

	ctx := signals.SetupSignalHandler(context.Background())
	d, err := sqlite.Open("")
	if err != nil {
		return err
	}

	s := sql.New(ctx, d)
	l := log.New(s)
	b := bridge.New(l)

	server := grpc.NewServer()
	etcdserverpb.RegisterWatchServer(server, b)
	etcdserverpb.RegisterKVServer(server, b)

	hsrv := health.NewServer()
	hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, hsrv)

	lis, err := net.Listen("tcp", ":2379")
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		server.Stop()
	}()

	return server.Serve(lis)
}
