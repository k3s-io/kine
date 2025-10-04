package testserver

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/app"
	"github.com/k3s-io/kine/pkg/endpoint"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
)

// This is a drop-in replacement for the embedded etcd test server at:
// https://github.com/kubernetes/kubernetes/blob/v1.34.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/testserver/test_server.go

func NewTestConfig(t testing.TB) *embed.Config {
	cfg := embed.NewConfig()

	clientURL := url.URL{Scheme: "unix", Path: "/tmp/kine.sock"}

	cfg.ListenClientUrls = []url.URL{clientURL}
	cfg.Dir = t.TempDir()
	os.Chmod(cfg.Dir, 0700)
	return cfg
}

func RunEtcd(t testing.TB, cfg *embed.Config) *kubernetes.Client {
	t.Helper()

	if cfg == nil {
		cfg = NewTestConfig(t)
	}

	ctx, cancel := context.WithCancel(t.Context())
	wg := &sync.WaitGroup{}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	config := app.Config(nil)
	config.WaitGroup = wg
	config.Listener = cfg.ListenClientUrls[0].String()
	config.CompactInterval = 0
	config.CompactMinRetain = 0
	if config.Endpoint == "" {
		// use a unique database dir for each test
		config.Endpoint = fmt.Sprintf("sqlite://%s/state.db?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate", cfg.Dir)
	} else {
		// use a unique database name for each test
		// we can't just use the tempdir name as it is derived from the test name and may be very long, so we just use a short hash
		hash := sha256.Sum256([]byte(cfg.Dir))
		config.Endpoint += fmt.Sprintf("_%.8x", hash)
	}

	logrus.SetLevel(logrus.TraceLevel)
	logrus.SetOutput(&testWriter{tb: t})

	e, err := endpoint.Listen(ctx, config)
	if err != nil {
		t.Fatal(err)
	}

	tlsConfig, err := e.TLSConfig.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}

	// We don't have a ready channel, so just sleep for a bit
	time.Sleep(time.Second)

	client, err := kubernetes.New(clientv3.Config{
		TLS:         tlsConfig,
		Endpoints:   e.Endpoints,
		DialTimeout: 10 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
		Logger:      zaptest.NewLogger(t, zaptest.Level(zapcore.ErrorLevel)).Named("etcd-client"),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.KV = storagetesting.NewKVRecorder(client.KV)
	client.Kubernetes = storagetesting.NewKubernetesRecorder(client.Kubernetes)
	return client
}

// TODO: replace this with *testing.TB.Output() once we switch to go1.25
type testWriter struct {
	tb testing.TB
}

func (tw *testWriter) Write(p []byte) (int, error) {
	tw.tb.Logf("%s", p)
	return 0, nil
}
