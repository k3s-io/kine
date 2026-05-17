// Package t4 provides a kine backend driver backed by t4 — an embeddable,
// S3-durable key-value store. t4 handles WAL management, periodic
// checkpoints, leader election, and follower replication; this package wires
// those capabilities into the kine Backend interface.
//
// # DSN format
//
//	t4://[bucket[/prefix]][?param=value&...]
//
// When bucket is omitted the node runs in offline mode (local durability only).
//
// # Parameters
//
//	data-dir            Local storage directory (default: /var/lib/t4)
//	node-id             Stable unique node ID (default: hostname)
//	peer-listen         gRPC listen address for WAL streaming, e.g. 0.0.0.0:3380
//	                    Required to enable multi-node mode (set automatically
//	                    when service-name is provided).
//	advertise-peer      Advertised peer address (default: peer-listen value).
//	                    Set automatically when service-name is provided.
//	peer-port           Peer gRPC port used by service-name auto-config (default: 3380)
//	service-name        Kubernetes headless service FQDN. When set, enables
//	                    multi-node mode automatically: peer-listen is set to
//	                    0.0.0.0:<peer-port> and advertise-peer is set to
//	                    <hostname>.<service-name>:<peer-port>. Simplifies Hosted Control Plane setups.
//	                    Must be fully-qualified (e.g. svc.default.svc.cluster.local)
//	                    — gRPC does not expand DNS search domains.
//	s3-endpoint         Custom S3 endpoint URL (MinIO, Ceph, etc.)
//	region              AWS region (default: us-east-1)
//	checkpoint-interval Checkpoint write interval, e.g. 15m (default: 15m)
//	segment-max-age     WAL segment rotation age, e.g. 10s (default: 10s)
//	follower-max-retries Consecutive stream failures before takeover (default: 5)
//	peer-cert-file      Path to this node's TLS certificate for the peer
//	                    gRPC server and client. Enables mTLS for WAL streaming
//	                    when set together with peer-key-file and peer-ca-file.
//	peer-key-file       Path to the private key matching peer-cert-file.
//	peer-ca-file        Path to the CA bundle used to verify peer certificates
//	                    on both sides (server verifies clients, client verifies
//	                    server). Required when peer-cert-file is set.
//	peer-skip-verify    Disable peer certificate verification (debug only).
//
// # Examples
//
// Single node, local only:
//
//	t4://?data-dir=/var/lib/t4
//
// Single node with S3 durability:
//
//	t4://my-bucket/prefix?data-dir=/var/lib/t4
//
// Three-node cluster:
//
//	t4://my-bucket/prefix?data-dir=/var/lib/t4&node-id=node-a&peer-listen=0.0.0.0:3380&advertise-peer=node-a.internal:3380
package t4

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/k3s-io/kine/pkg/drivers"
	kserver "github.com/k3s-io/kine/pkg/server"
	"github.com/t4db/t4"
	t4obj "github.com/t4db/t4/pkg/object"
	"google.golang.org/grpc/credentials"
)

func init() {
	drivers.Register("t4", New)
}

// New is the kine driver constructor for the "t4" scheme.
func New(ctx context.Context, _ *sync.WaitGroup, cfg *drivers.Config) (leaderElect bool, b kserver.Backend, err error) {
	nodeCfg, err := parseConfig(ctx, cfg.DataSourceName)
	if err != nil {
		return false, nil, fmt.Errorf("t4 driver: parse DSN: %w", err)
	}

	node, err := t4.Open(*nodeCfg)
	if err != nil {
		return false, nil, fmt.Errorf("t4 driver: open node: %w", err)
	}

	// leaderElect=false: t4 handles its own leader election via S3.
	return false, &backend{node: node}, nil
}

// parseConfig parses the DataSourceName (everything after "t4://") into a
// t4.Config.
func parseConfig(ctx context.Context, dsn string) (*t4.Config, error) {
	// Re-add the scheme so url.Parse handles it correctly.
	u, err := url.Parse("t4://" + dsn)
	if err != nil {
		return nil, err
	}

	q := u.Query()

	cfg := &t4.Config{
		DataDir: q.Get("data-dir"),
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "/var/lib/t4"
	}

	// S3 bucket and prefix from host/path.
	bucket := u.Hostname()
	prefix := strings.TrimPrefix(u.Path, "/")

	if bucket != "" {
		store, err := newS3Store(ctx, bucket, prefix, q.Get("s3-endpoint"), q.Get("region"))
		if err != nil {
			return nil, fmt.Errorf("create S3 store: %w", err)
		}
		cfg.ObjectStore = store
	}

	// Optional fields.
	if v := q.Get("node-id"); v != "" {
		cfg.NodeID = v
	}

	peerPort := q.Get("peer-port")
	if peerPort == "" {
		peerPort = "3380"
	}

	// service-name enables multi-node mode without manual peer address
	// configuration. When set, peer-listen defaults to 0.0.0.0:<peer-port>
	// and advertise-peer defaults to <hostname>.<service-name>:<peer-port>,
	// which is the stable DNS name assigned by a Kubernetes headless service.
	// Both can still be overridden explicitly via peer-listen / advertise-peer.
	if svc := q.Get("service-name"); svc != "" && q.Get("peer-listen") == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("resolve hostname for advertise-peer: %w", err)
		}
		cfg.PeerListenAddr = "0.0.0.0:" + peerPort
		cfg.AdvertisePeerAddr = hostname + "." + svc + ":" + peerPort
	}

	if v := q.Get("peer-listen"); v != "" {
		cfg.PeerListenAddr = v
		cfg.AdvertisePeerAddr = v // default; may be overridden below
	}
	if v := q.Get("advertise-peer"); v != "" {
		cfg.AdvertisePeerAddr = v
	}
	if v := q.Get("checkpoint-interval"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("checkpoint-interval: %w", err)
		}
		cfg.CheckpointInterval = d
	}
	if v := q.Get("segment-max-age"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("segment-max-age: %w", err)
		}
		cfg.SegmentMaxAge = d
	}
	if v := q.Get("follower-max-retries"); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
			return nil, fmt.Errorf("follower-max-retries: %w", err)
		}
		cfg.FollowerMaxRetries = n
	}

	server, client, err := peerTLSCredentials(
		q.Get("peer-cert-file"),
		q.Get("peer-key-file"),
		q.Get("peer-ca-file"),
		q.Get("peer-skip-verify") == "true",
	)
	if err != nil {
		return nil, err
	}
	cfg.PeerServerTLS = server
	cfg.PeerClientTLS = client

	return cfg, nil
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
		return nil, nil, fmt.Errorf("peer TLS requires peer-cert-file, peer-key-file, and peer-ca-file together")
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

// newS3Store creates an object.Store backed by the given S3 bucket.
func newS3Store(ctx context.Context, bucket, prefix, endpoint, region string) (t4obj.Store, error) {
	opts, err := awsConfigOptions(endpoint, region)
	if err != nil {
		return nil, err
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}

	s3opts := []func(*awss3.Options){}
	if endpoint != "" {
		s3opts = append(s3opts, func(o *awss3.Options) {
			o.UsePathStyle = true
		})
	}

	client := awss3.NewFromConfig(awsCfg, s3opts...)
	return t4obj.NewS3Store(client, bucket, prefix), nil
}

func awsConfigOptions(endpoint, region string) ([]func(*awsconfig.LoadOptions) error, error) {
	opts := []func(*awsconfig.LoadOptions) error{}

	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	} else {
		opts = append(opts, awsconfig.WithRegion("us-east-1"))
	}

	if endpoint != "" {
		opts = append(opts, awsconfig.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint}, nil
			}),
		))
	}

	accessKeyID := os.Getenv("T4_S3_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("T4_S3_SECRET_ACCESS_KEY")
	profile := os.Getenv("T4_S3_PROFILE")
	switch {
	case accessKeyID != "" && secretAccessKey != "":
		opts = append(opts, awsconfig.WithCredentialsProvider(
			awscredentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, ""),
		))
	case accessKeyID != "" || secretAccessKey != "":
		return nil, fmt.Errorf("T4_S3_ACCESS_KEY_ID and T4_S3_SECRET_ACCESS_KEY must be set together")
	case profile != "":
		opts = append(opts, awsconfig.WithSharedConfigProfile(profile))
	default:
		return nil, fmt.Errorf("S3 credentials require T4_S3_ACCESS_KEY_ID and T4_S3_SECRET_ACCESS_KEY, or T4_S3_PROFILE to use the default AWS credential chain")
	}

	return opts, nil
}
