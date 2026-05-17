# t4 Driver for Kine

Backs kine with [t4](https://github.com/t4db/t4) — an embeddable,
S3-durable key-value store. t4 handles WAL management, periodic
checkpoints, and its own leader election (via an S3 lock object) internally.

The driver reports `leaderElect=true` to kine, so Kubernetes components
(apiserver, controller-manager) leader-elect normally when kine runs
embedded in k3s.

## Configuration

Configuration is split between two surfaces:

- **DSN** — identifies the datastore (bucket + prefix) and carries
  datastore-tuning knobs (`data-dir`, `node-id`, `service-name`,
  `checkpoint-interval`, `segment-max-age`, `follower-max-retries`). This
  matches how mysql / pgsql / nats encode database name and tuning in
  their connection strings.
- **kine CLI flags** (or, for embedders, `drivers.Config.PeerConfig` /
  `drivers.Config.S3Config`) — kine-server-shaped settings: peer
  bind/advertise addresses, S3 bucket and credentials, peer mTLS files
  (reusing the existing `--ca-file` / `--cert-file` / `--key-file` /
  `--skip-verify`). Same pattern as `--listen-address`,
  `--server-cert-file`, `--metrics-bind-address`. The structs are not
  t4-namespaced so future drivers that need the same shape can reuse them.

### Endpoint Format

```
t4:///path/to/local/data-dir[?param=value&...]
t4://relative/data-dir[?param=value&...]
```

The DSN path is the local data-dir (parity with the sqlite driver). Default
is `db` in the current working directory. S3 durability is configured via
`--s3-bucket` and the related flags below.

### DSN parameters

| Parameter              | Default       | Description                                                                                                                                                                                                                                                                                            |
|------------------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| _path_                 | `db`          | Local directory for the Pebble database and WAL segments (taken from the DSN path, like sqlite).                                                                                                                                                                                                       |
| `node-id`              | hostname      | Stable unique identifier for this node. Must be consistent across restarts.                                                                                                                                                                                                                            |
| `service-name`         | —             | Kubernetes headless service FQDN. When set, enables multi-node mode automatically: peer bind defaults to `0.0.0.0:3380` and peer advertise defaults to `<hostname>.<service-name>:<port>`. Must be a fully-qualified domain name (e.g. `kine.default.svc.cluster.local`) — gRPC does not expand DNS search domains. |
| `checkpoint-interval`  | `15m`         | How often the leader writes a full checkpoint to S3.                                                                                                                                                                                                                                                   |
| `segment-max-age`      | `10s`         | Maximum age of a WAL segment before it is rotated and uploaded.                                                                                                                                                                                                                                        |
| `follower-max-retries` | `5`           | Consecutive stream failures a follower tolerates before attempting a leader takeover.                                                                                                                                                                                                                  |

### CLI flags / environment

S3 and peer addresses:

| Flag                       | Env var                              | Notes                                                                                                                |
|----------------------------|--------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| `--peer-bind-address`      | `KINE_PEER_BIND_ADDRESS`             | gRPC listen `host:port` for the peer WAL-streaming server. Empty → single-node mode. Example: `0.0.0.0:3380`.        |
| `--peer-advertise-address` | `KINE_PEER_ADVERTISE_ADDRESS`        | Address other t4 nodes use to reach this node. Defaults to the bind address.                                         |
| `--s3-bucket`              | `KINE_S3_BUCKET`                     | S3 bucket name. Empty means local-only.                                                                              |
| `--s3-folder`              | `KINE_S3_FOLDER`                     | Optional key prefix inside the bucket.                                                                               |
| `--s3-endpoint`            | `KINE_S3_ENDPOINT`                   | Custom S3-compatible endpoint URL (MinIO, Ceph, etc.).                                                               |
| `--s3-region`              | `AWS_REGION` / `AWS_DEFAULT_REGION`  | AWS region. Default `us-east-1`.                                                                                     |
| `--s3-access-key`          | `AWS_ACCESS_KEY_ID`                  | Static S3 access key. Set together with `--s3-secret-key`.                                                           |
| `--s3-secret-key`          | `AWS_SECRET_ACCESS_KEY`              | Static S3 secret key.                                                                                                |
| `--s3-session-token`       | `AWS_SESSION_TOKEN`                  | Optional S3 session token (temporary STS credentials). Used only with `--s3-access-key`.                             |
| `--s3-profile`             | `AWS_PROFILE`                        | Engages the ambient AWS credentials chain (env vars, `~/.aws/credentials`, IMDS). Ignored when an access key is set. |
| `--s3-ca-bundle`           | `AWS_CA_BUNDLE`                      | PEM CA bundle used to trust HTTPS S3 endpoints (MinIO behind a private CA, etc.).                                    |

Peer mTLS reuses kine's existing client-TLS flags (t4 nodes act as clients
to their peers): `--cert-file`, `--key-file`, `--ca-file`, `--skip-verify`.

## Examples

**Local only (no S3, single node):**

```
kine --endpoint "t4:///var/lib/t4"
```

**Single node with S3 durability:**

```
kine \
  --endpoint "t4:///var/lib/t4" \
  --s3-bucket my-bucket \
  --s3-folder k3s
```

**Single node, MinIO** (S3 endpoint and credentials via CLI flags / env):

```
kine \
  --endpoint "t4:///var/lib/t4" \
  --s3-bucket my-bucket \
  --s3-folder k3s \
  --s3-endpoint http://minio:9000 \
  --s3-access-key minioadmin \
  --s3-secret-key minioadmin
```

**Three-node cluster on Kubernetes (recommended):**

```
kine \
  --endpoint "t4:///var/lib/t4?service-name=kine.kube-system.svc.cluster.local" \
  --s3-bucket my-bucket \
  --s3-folder k3s
```

`node-id` defaults to the pod hostname, and peer bind / advertise addresses are
derived from `service-name` automatically. All three nodes use the same DSN and
flags — no per-node configuration needed. S3 credentials come from
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (or IRSA, etc.) in the pod env.

**Three-node cluster (manual peer config):**

```
# node-a
kine \
  --endpoint "t4:///var/lib/t4?node-id=node-a" \
  --s3-bucket my-bucket --s3-folder k3s \
  --peer-bind-address 0.0.0.0:3380 \
  --peer-advertise-address node-a.internal:3380

# node-b
kine \
  --endpoint "t4:///var/lib/t4?node-id=node-b" \
  --s3-bucket my-bucket --s3-folder k3s \
  --peer-bind-address 0.0.0.0:3380 \
  --peer-advertise-address node-b.internal:3380

# node-c
kine \
  --endpoint "t4:///var/lib/t4?node-id=node-c" \
  --s3-bucket my-bucket --s3-folder k3s \
  --peer-bind-address 0.0.0.0:3380 \
  --peer-advertise-address node-c.internal:3380
```

## Peer TLS

WAL streaming between t4 nodes runs over gRPC. By default the peer channel is
plaintext, which is only safe inside a trusted network. Peer mTLS reuses
kine's existing client-TLS flags — `--cert-file`, `--key-file`, `--ca-file`
— because t4 nodes act as clients to their peers:

| Flag           | Role                                                                                              |
|----------------|---------------------------------------------------------------------------------------------------|
| `--cert-file`  | This node's certificate — presented as the leader's server cert *and* the follower's client cert. |
| `--key-file`   | Private key matching the cert file.                                                               |
| `--ca-file`    | CA bundle that signed every node's peer cert. Used to verify the peer on both sides.              |

All three must be set together. The leader's gRPC server requires and verifies
the follower's client certificate (`tls.RequireAndVerifyClientCert`), and the
follower verifies the leader's server certificate against the same CA.

**Three-node cluster with mTLS:**

```
kine \
  --endpoint "t4:///var/lib/t4?service-name=kine.kube-system.svc.cluster.local" \
  --s3-bucket my-bucket --s3-folder k3s \
  --cert-file /etc/t4/peer.crt \
  --key-file /etc/t4/peer.key \
  --ca-file /etc/t4/peer-ca.crt
```

Certificate SANs must cover the address each node advertises via
`--peer-advertise-address` (or the auto-generated
`<hostname>.<service-name>` when `service-name` is set). For Kubernetes
headless services, issuing one wildcard cert for `*.<service-name>` is the
simplest path.

`--skip-verify` disables peer-cert verification on the client side and
downgrades the server to `tls.RequestClientCert` — useful for short-lived
debug clusters, but never enable it in production.

## S3 Credentials

S3 credentials live on `drivers.Config.S3Config` (CLI flag / env / programmatic
struct) rather than in the DSN. This keeps secrets out of the kine endpoint
string and gives kine embedders (k3s, k0s, …) direct control.

One of the following must be set when `--s3-bucket` is configured, otherwise
the driver fails fast on startup:

- `--s3-access-key` (env: `AWS_ACCESS_KEY_ID`) + `--s3-secret-key` (env:
  `AWS_SECRET_ACCESS_KEY`) for static credentials. `--s3-session-token`
  (env: `AWS_SESSION_TOKEN`) is optional for temporary STS credentials.
- `--s3-profile` (env: `AWS_PROFILE`) to engage the ambient AWS credentials
  chain (`AWS_*` env vars, `~/.aws/credentials`, EC2/EKS IMDS) scoped to
  the named profile.

## S3 Bucket Layout

All objects are stored under the configured prefix:

```
<prefix>/manifest/latest          ← checkpoint manifest (JSON)
<prefix>/checkpoint/<term>/<rev>  ← checkpoint archives
<prefix>/wal/<seq>.seg            ← uploaded WAL segments
<prefix>/election/lock            ← leader election lock
```

Multiple clusters can safely share one bucket by using distinct prefixes.

## Multi-node Behaviour

- All nodes point at the same S3 bucket and prefix.
- On startup each node races to write the S3 election lock. The winner becomes leader; the rest become followers.
- Followers stream the WAL from the leader in real time and serve reads locally.
- Writes sent to a follower are forwarded to the leader transparently.
- If the leader becomes unreachable for `follower-max-retries` consecutive failures, a follower overwrites the lock and
  takes over.

No external coordination service (etcd, ZooKeeper, Raft quorum) is required.

## Startup Behaviour

On every start, the driver:

1. Reads `manifest/latest` from S3 to find the latest checkpoint.
2. If the local database is absent, downloads and restores the checkpoint.
3. Replays any local WAL segments newer than the checkpoint.
4. Replays any S3 WAL segments newer than the checkpoint and not yet local.
5. Runs leader election (multi-node) or becomes single-node.

A node that loses its disk recovers automatically from S3 with no manual
intervention.
