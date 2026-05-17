// Package t4 provides a kine backend driver backed by t4 — an embeddable,
// S3-durable key-value store. t4 handles WAL management, periodic
// checkpoints, leader election, and follower replication; this package wires
// those capabilities into the kine Backend interface.
//
// # Configuration split
//
// Configuration is split between two surfaces:
//
//   - DSN: carries the local data-dir (DSN path, sqlite-style) and
//     datastore tuning knobs (node-id, service-name, checkpoint-interval,
//     segment-max-age, follower-max-retries). This matches how mysql/pgsql/
//     nats encode tuning in their connection strings.
//   - drivers.Config (kine CLI flags or programmatic struct): carries
//     kine-server-shaped settings — peer bind/advertise addresses, the S3
//     bucket and credentials, and peer mTLS (reusing the existing
//     --ca-file / --cert-file / --key-file / --skip-verify since t4 nodes
//     act as clients to their peers). The flags are not t4-namespaced so
//     they can be shared with future drivers that need the same shape.
//
// # DSN format
//
//	t4:///path/to/data-dir[?param=value&...]
//	t4://relative/data-dir[?param=value&...]
//
// The DSN path is the local data-dir. Default is "db" in the current
// working directory (sqlite parity). S3 durability is configured via
// --s3-bucket and friends.
//
// # DSN parameters
//
//	node-id              Stable unique node ID (default: hostname)
//	service-name         Kubernetes headless service FQDN. When set, enables
//	                     multi-node mode automatically: peer bind defaults
//	                     to 0.0.0.0:3380 and peer advertise defaults to
//	                     <hostname>.<service-name>:<port>, which is the
//	                     stable DNS name assigned by a Kubernetes headless
//	                     service. Must be a fully-qualified domain name
//	                     (e.g. kine.default.svc.cluster.local) — gRPC does
//	                     not expand DNS search domains.
//	checkpoint-interval  Checkpoint write interval, e.g. 15m (default: 15m)
//	segment-max-age      WAL segment rotation age, e.g. 10s (default: 10s)
//	follower-max-retries Consecutive stream failures before takeover (default: 5)
//
// # CLI flags (drivers.PeerConfig + drivers.S3Config)
//
//   - PeerConfig.BindAddress / --peer-bind-address
//   - PeerConfig.AdvertiseAddress / --peer-advertise-address
//   - S3Config.Bucket / --s3-bucket
//   - S3Config.Folder / --s3-folder
//   - S3Config.Endpoint / --s3-endpoint
//   - S3Config.Region / --s3-region (env: AWS_REGION / AWS_DEFAULT_REGION)
//   - S3Config.AccessKey / --s3-access-key (env: AWS_ACCESS_KEY_ID)
//   - S3Config.SecretKey / --s3-secret-key (env: AWS_SECRET_ACCESS_KEY)
//   - S3Config.SessionToken / --s3-session-token (env: AWS_SESSION_TOKEN)
//   - S3Config.Profile / --s3-profile (env: AWS_PROFILE)
//   - S3Config.CABundle / --s3-ca-bundle (env: AWS_CA_BUNDLE)
//
// Peer mTLS reuses --ca-file / --cert-file / --key-file / --skip-verify
// (drivers.Config.BackendTLSConfig). The same cert+key serves as the
// leader's server identity and the follower's client identity.
//
// One of AccessKey+SecretKey or Profile must be set when --s3-bucket
// is configured, otherwise t4 fails fast on startup.
//
// # Examples
//
// Single node, local only:
//
//	--endpoint t4:///var/lib/t4
//
// Single node with S3 durability (creds via AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY):
//
//	--endpoint t4:///var/lib/t4 --s3-bucket my-bucket --s3-folder prefix
//
// Three-node cluster on Kubernetes:
//
//	--endpoint "t4:///var/lib/t4?service-name=kine.kube-system.svc.cluster.local" \
//	  --s3-bucket my-bucket --s3-folder prefix
package t4

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	kserver "github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"github.com/t4db/t4"
	t4obj "github.com/t4db/t4/pkg/object"
	"google.golang.org/grpc/credentials"
)

func init() {
	drivers.Register("t4", New)
}

// New is the kine driver constructor for the "t4" scheme.
func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (leaderElect bool, b kserver.Backend, err error) {
	// nodeCtx is the startup context for t4.Open / S3 client construction.
	// It is independent of the kine lifecycle ctx so the long-lived S3 client
	// is not bound to kine shutdown; we cancel it explicitly after node.Close
	// returns.
	nodeCtx, cancel := context.WithCancel(context.Background())
	nodeCfg, err := parseConfig(nodeCtx, cfg)
	if err != nil {
		cancel()
		return false, nil, fmt.Errorf("t4 driver: parse DSN: %w", err)
	}

	node, err := t4.Open(*nodeCfg)
	if err != nil {
		cancel()
		return false, nil, fmt.Errorf("t4 driver: open node: %w", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		<-ctx.Done()
		logrus.Infof("Closing t4 node...")
		if err := node.Close(); err != nil {
			logrus.Errorf("Failed to close t4 node: %v", err)
		}
	}()

	// t4 is shared-storage and supports multi-server clusters, so kubernetes
	// components (apiserver, controller-manager) need leader election enabled.
	return true, &backend{node: node}, nil
}

// parseConfig translates the DSN (everything after "t4://") and the
// kine-level settings on drivers.Config into a t4.Config. The DSN carries
// the local data-dir (path) and datastore-tuning knobs (query string),
// sqlite-style; bucket/prefix, peer addresses, and credentials live on
// drivers.Config.
func parseConfig(ctx context.Context, cfg *drivers.Config) (*t4.Config, error) {
	dataDir, query, _ := strings.Cut(cfg.DataSourceName, "?")
	q, err := url.ParseQuery(query)
	if err != nil {
		return nil, fmt.Errorf("parse DSN query: %w", err)
	}

	nodeCfg := &t4.Config{
		DataDir: dataDir,
	}
	if nodeCfg.DataDir == "" {
		nodeCfg.DataDir = "db"
	}

	if cfg.S3Config.Bucket != "" {
		store, err := t4obj.NewS3StoreFromConfig(ctx, t4obj.S3Config{
			Bucket:          cfg.S3Config.Bucket,
			Prefix:          cfg.S3Config.Folder,
			Endpoint:        cfg.S3Config.Endpoint,
			Region:          cfg.S3Config.Region,
			Profile:         cfg.S3Config.Profile,
			AccessKeyID:     cfg.S3Config.AccessKey,
			SecretAccessKey: cfg.S3Config.SecretKey,
			SessionToken:    cfg.S3Config.SessionToken,
			CABundle:        cfg.S3Config.CABundle,
		})
		if err != nil {
			return nil, fmt.Errorf("create S3 store: %w", err)
		}
		nodeCfg.ObjectStore = store
	}

	// Datastore tuning — stays in the DSN, matching the existing kine
	// pattern for mysql/pgsql/nats. Server-shaped settings (peer bind /
	// advertise addresses, peer TLS, S3 endpoint+creds) come from
	// drivers.Config and have dedicated CLI flags.
	if v := q.Get("node-id"); v != "" {
		nodeCfg.NodeID = v
	}

	nodeCfg.PeerListenAddr = cfg.PeerConfig.BindAddress
	nodeCfg.AdvertisePeerAddr = cfg.PeerConfig.AdvertiseAddress

	// service-name enables multi-node mode without manual peer address
	// configuration: peer-bind defaults to 0.0.0.0:3380 and advertise
	// defaults to <hostname>.<service-name>:<port>, which is the stable
	// DNS name assigned by a Kubernetes headless service. Both can still
	// be overridden via --peer-bind-address / --peer-advertise-address.
	svc := q.Get("service-name")
	if svc != "" {
		if nodeCfg.PeerListenAddr == "" {
			nodeCfg.PeerListenAddr = "0.0.0.0:3380"
		}
		if nodeCfg.AdvertisePeerAddr == "" {
			_, port, err := net.SplitHostPort(nodeCfg.PeerListenAddr)
			if err != nil {
				return nil, fmt.Errorf("parse peer bind address %q: %w", nodeCfg.PeerListenAddr, err)
			}
			hostname, err := os.Hostname()
			if err != nil {
				return nil, fmt.Errorf("resolve hostname for peer advertise address: %w", err)
			}
			nodeCfg.AdvertisePeerAddr = hostname + "." + svc + ":" + port
		}
	}

	// Multi-node mode requires an explicit advertise address unless
	// service-name auto-config supplied one. The driver does not probe
	// network interfaces — guessing the right IP on multi-NIC / VPN /
	// dual-stack hosts is unreliable. Matches etcd's behavior: peer-urls
	// are explicit.
	if nodeCfg.PeerListenAddr != "" && nodeCfg.AdvertisePeerAddr == "" {
		return nil, errors.New("--peer-advertise-address is required when --peer-bind-address is set (or set service-name=... in the DSN to auto-derive it)")
	}

	if v := q.Get("checkpoint-interval"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("checkpoint-interval: %w", err)
		}
		nodeCfg.CheckpointInterval = d
	}
	if v := q.Get("segment-max-age"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("segment-max-age: %w", err)
		}
		nodeCfg.SegmentMaxAge = d
	}
	if v := q.Get("follower-max-retries"); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
			return nil, fmt.Errorf("follower-max-retries: %w", err)
		}
		nodeCfg.FollowerMaxRetries = n
	}

	// Peer TLS reuses the existing BackendTLSConfig (--ca-file / --cert-file
	// / --key-file / --skip-verify): t4 nodes are clients to their peers,
	// which fits the same role the BackendTLSConfig serves for the other
	// drivers. The same cert+key is also presented by the leader's server
	// when followers connect inbound.
	server, client, err := peerTLSCredentials(
		cfg.BackendTLSConfig.CertFile,
		cfg.BackendTLSConfig.KeyFile,
		cfg.BackendTLSConfig.CAFile,
		cfg.BackendTLSConfig.SkipVerify,
	)
	if err != nil {
		return nil, err
	}
	nodeCfg.PeerServerTLS = server
	nodeCfg.PeerClientTLS = client

	return nodeCfg, nil
}

// peerTLSCredentials builds gRPC TransportCredentials for the t4 peer
// connections (WAL streaming). The same cert/key is used for both the leader's
// gRPC server and a follower's gRPC client, with the CA verifying peers in
// both directions — i.e. mutual TLS. All three files must be set together;
// omitting all three disables TLS (plaintext, only safe inside a trusted
// network).
func peerTLSCredentials(certFile, keyFile, caFile string, skipVerify bool) (server, client credentials.TransportCredentials, err error) {
	if certFile == "" && keyFile == "" && caFile == "" {
		return nil, nil, nil
	}
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, nil, errors.New("peer TLS requires peer-cert-file, peer-key-file, and peer-ca-file together")
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load peer TLS keypair: %w", err)
	}

	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read peer CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, nil, fmt.Errorf("peer CA %s: no certificates parsed", caFile)
	}

	serverCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}
	clientCfg := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            pool,
		InsecureSkipVerify: skipVerify,
		MinVersion:         tls.VersionTLS12,
	}
	if skipVerify {
		serverCfg.ClientAuth = tls.RequestClientCert
	}
	return credentials.NewTLS(serverCfg), credentials.NewTLS(clientCfg), nil
}
