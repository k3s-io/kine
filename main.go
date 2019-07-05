package main

import (
	"context"
	"net"

	"github.com/rancher/kine/pkg/drivers/sqlite"
	"github.com/rancher/kine/pkg/server"
	"github.com/rancher/wrangler/pkg/signals"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		logrus.Fatal(err)
	}
}

func run() error {
	logrus.SetLevel(logrus.TraceLevel)

	ctx := signals.SetupSignalHandler(context.Background())
	backend, err := sqlite.New("")
	if err != nil {
		return err
	}

	b := server.New(backend)

	grpcServer := grpc.NewServer()
	b.Register(grpcServer)

	lis, err := net.Listen("tcp", ":2379")
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		grpcServer.Stop()
	}()

	return grpcServer.Serve(lis)
}
