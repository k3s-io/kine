package testserver

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"os"
	"strings"
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
)

// This is a drop-in replacement for the embedded etcd test server at:
// https://github.com/kubernetes/kubernetes/blob/v1.32.9/staging/src/k8s.io/apiserver/pkg/storage/etcd3/testserver/test_server.go

func NewTestConfig(t testing.TB) *embed.Config {
	cfg := embed.NewConfig()

	clientURL := url.URL{Scheme: "unix", Path: "/tmp/kine.sock"}

	cfg.ListenClientUrls = []url.URL{clientURL}
	cfg.ExperimentalWatchProgressNotifyInterval = 5 * time.Second
	cfg.Dir = t.TempDir()
	os.Chmod(cfg.Dir, 0700)
	return cfg
}

func RunEtcd(t testing.TB, cfg *embed.Config) *kubernetes.Client {
	t.Helper()
	logrus.SetLevel(logrus.TraceLevel)
	logrus.SetOutput(&testWriter{tb: t})

	if cfg == nil {
		cfg = NewTestConfig(t)
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	config := app.Config(nil)
	config.WaitGroup = wg
	config.Listener = cfg.ListenClientUrls[0].String()
	config.NotifyInterval = cfg.ExperimentalWatchProgressNotifyInterval
	config.CompactInterval = 0
	config.CompactMinRetain = 0

	// use a unique database for each test
	dsn, err := setDatabasePath(config.Endpoint, cfg.Dir)
	if err != nil {
		t.Fatalf("Failed to set database path: %v", err)
	}
	config.Endpoint = dsn

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
	return client
}

func setDatabasePath(endpoint, dir string) (string, error) {
	// append a hash of the temp dir to the db name for remote engines where we cannot set the db path directly
	hash := fmt.Sprintf("%.8x", sha256.Sum256([]byte(dir)))
	scheme, _, _ := strings.Cut(endpoint, "://")
	switch scheme {
	case "", "sqlite":
		// empty scheme means default sqlite db
		return fmt.Sprintf("sqlite://%s/state.db?_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate", dir), nil
	case "nats", "jetstream":
		// nats doesn't have databases, so set the bucket param instead
		ep, err := url.Parse(endpoint)
		if err != nil {
			return "", err
		}
		v := ep.Query()
		v.Set("bucket", hash)
		ep.RawQuery = v.Encode()
		if ep.Host == "" {
			ep.Opaque = "//" + ep.Path
		}
		return ep.String(), nil
	case "mysql":
		//  mysql DSNs are not valid URLs, so just insert the hash before the query string, if any
		path, query, _ := strings.Cut(endpoint, "?")
		return path + hash + "?" + query, nil
	default:
		ep, err := url.Parse(endpoint)
		if err != nil {
			return "", err
		}
		ep.Path += hash
		return ep.String(), nil
	}
}

// TODO: replace this with *testing.TB.Output() once we switch to go1.25
type testWriter struct {
	tb testing.TB
}

func (tw *testWriter) Write(p []byte) (int, error) {
	tw.tb.Logf("%s", p)
	return 0, nil
}
