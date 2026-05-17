package drivers

import (
	"time"

	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/prometheus/client_golang/prometheus"
)

type Config struct {
	MetricsRegisterer     prometheus.Registerer
	Endpoint              string
	Scheme                string
	DataSourceName        string
	ConnectionPoolConfig  generic.ConnectionPoolConfig
	BackendTLSConfig      tls.Config
	CompactInterval       time.Duration
	CompactIntervalJitter int
	CompactTimeout        time.Duration
	CompactMinRetain      int64
	CompactBatchSize      int64
	PollBatchSize         int64
	PeerConfig            PeerConfig
	S3Config              S3Config
}

// PeerConfig and S3Config carry kine-server-shaped settings consumed by the
// t4 driver — peer bind/advertise addresses, the S3 bucket and credentials.
// They live on drivers.Config (rather than the t4 DSN) so kine embedders
// such as k3s can configure these programmatically. The peer-TLS material
// is read from drivers.Config.BackendTLSConfig — t4 nodes act as clients to
// their peers, so the existing --ca-file / --cert-file / --key-file /
// --skip-verify flags carry that role. Datastore tuning
// (checkpoint-interval, etc.) stays in the DSN, matching the existing kine
// pattern for mysql/pgsql/nats. The structs are intentionally generic so
// future drivers that need the same shape can reuse them.
type PeerConfig struct {
	// BindAddress is the gRPC listen address (host:port) for t4's
	// WAL-streaming server, e.g. "0.0.0.0:3380". Empty means single-node
	// mode (no peer server).
	BindAddress string

	// AdvertiseAddress is the address other T4 nodes use to reach this
	// node's peer server. Defaults to BindAddress.
	AdvertiseAddress string
}

type S3Config struct {
	// Bucket is the S3 bucket name. Empty means local-only mode
	// (no S3 durability).
	Bucket string

	// Folder is an optional key prefix inside the bucket (no trailing
	// slash needed). Equivalent to k3s's --etcd-s3-folder.
	Folder string

	// Endpoint overrides the S3 endpoint URL for MinIO and other
	// S3-compatible stores. Empty means the standard AWS endpoint.
	Endpoint string

	// Region overrides the AWS region. Defaults to us-east-1 when empty.
	Region string

	// AccessKey and SecretKey provide static S3 credentials. Both must
	// be set together. SessionToken is optional (set when using temporary
	// credentials from STS) and is ignored unless AccessKey is also set.
	// Field/flag naming follows k3s's etcd-s3 snapshot convention.
	AccessKey    string
	SecretKey    string
	SessionToken string

	// Profile engages the ambient AWS credentials chain — env vars
	// (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN), then
	// the named profile in ~/.aws/credentials, then EC2/EKS IMDS.
	Profile string

	// CABundle is an optional PEM-encoded CA-certificate bundle path used
	// to trust HTTPS S3 endpoints (MinIO behind a private CA, etc.). Empty
	// means the system trust store.
	CABundle string
}
