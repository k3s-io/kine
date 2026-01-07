package server

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type KVServerBridge struct {
	emulatedETCDVersion string
	limited             *LimitedServer
	health              *health.Server
}

func New(backend Backend, scheme string, notifyInterval time.Duration, emulatedETCDVersion string) *KVServerBridge {
	return &KVServerBridge{
		emulatedETCDVersion: emulatedETCDVersion,
		health:              health.NewServer(),
		limited: &LimitedServer{
			notifyInterval: notifyInterval,
			backend:        backend,
			scheme:         scheme,
		},
	}
}

func (k *KVServerBridge) Register(server *grpc.Server) {
	etcdserverpb.RegisterLeaseServer(server, k)
	etcdserverpb.RegisterWatchServer(server, k)
	etcdserverpb.RegisterKVServer(server, k)
	etcdserverpb.RegisterClusterServer(server, k)
	etcdserverpb.RegisterMaintenanceServer(server, k)

	k.health.SetServingStatus("", healthpb.HealthCheckResponse_UNKNOWN)
	healthpb.RegisterHealthServer(server, k.health)

	reflection.Register(server)
}

func (k *KVServerBridge) Healthcheck(ctx context.Context, interval, timeout time.Duration) error {
	tick := time.NewTicker(interval)
	defer tick.Stop()

	// first healthcheck is done ASAP
	k.healthcheck(ctx, timeout)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-tick.C:
			k.healthcheck(ctx, timeout)
		}
	}
}

func (k *KVServerBridge) healthcheck(ctx context.Context, timeout time.Duration) {
	hctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := k.limited.backend.Health(hctx)
	if err != nil {
		logrus.Errorf("Healthcheck failed: %v", err)
		k.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	} else {
		k.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	}
}
