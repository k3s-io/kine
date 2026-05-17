# t4 Driver for Kine

Backs kine with [t4](https://github.com/t4db/t4) — an embeddable,
S3-durable key-value store. t4 handles WAL management, periodic
checkpoints, and leader election internally, so kine's own leader-election
path is bypassed (`leaderElect=false`).

## Endpoint Format

```
t4://[bucket[/prefix]][?param=value&...]
```

When `bucket` is omitted the node runs in local-only mode (no S3 durability).

## Parameters

| Parameter              | Default             | Description                                                                                                                                                                                                                                                                                                                    |
|------------------------|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `data-dir`             | `/var/lib/t4`       | Local directory for the Pebble database and WAL segments.                                                                                                                                                                                                                                                                      |
| `node-id`              | hostname            | Stable unique identifier for this node. Must be consistent across restarts.                                                                                                                                                                                                                                                    |
| `peer-listen`          | —                   | gRPC listen address for WAL streaming, e.g. `0.0.0.0:3380`. Required to enable multi-node mode (set automatically when `service-name` is provided).                                                                                                                                                                            |
| `advertise-peer`       | `peer-listen` value | Address that other nodes use to reach this node's peer server. Set this when `peer-listen` binds `0.0.0.0` (set automatically when `service-name` is provided).                                                                                                                                                                |
| `peer-port`            | `3380`              | Peer gRPC port used by `service-name` auto-config.                                                                                                                                                                                                                                                                             |
| `service-name`         | —                   | Kubernetes headless service FQDN. When set, enables multi-node mode automatically: `peer-listen` is set to `0.0.0.0:<peer-port>` and `advertise-peer` is set to `<hostname>.<service-name>:<peer-port>`. Must be a fully-qualified domain name (e.g. `kine.default.svc.cluster.local`) — gRPC does not use DNS search domains. |
| `s3-endpoint`          | —                   | Custom S3-compatible endpoint URL (MinIO, Ceph, etc.). Enables path-style requests automatically.                                                                                                                                                                                                                              |
| `region`               | `us-east-1`         | AWS region.                                                                                                                                                                                                                                                                                                                    |
| `checkpoint-interval`  | `15m`               | How often the leader writes a full checkpoint to S3.                                                                                                                                                                                                                                                                           |
| `segment-max-age`      | `10s`               | Maximum age of a WAL segment before it is rotated and uploaded.                                                                                                                                                                                                                                                                |
| `follower-max-retries` | `5`                 | Consecutive stream failures a follower tolerates before attempting a leader takeover.                                                                                                                                                                                                                                          |
| `peer-cert-file`       | —                   | Path to this node's TLS certificate for the peer gRPC server and client. Setting `peer-cert-file`, `peer-key-file`, and `peer-ca-file` together enables mutual TLS for WAL streaming. Omit all three for plaintext (only safe inside a trusted network).                                                                       |
| `peer-key-file`        | —                   | Path to the private key matching `peer-cert-file`.                                                                                                                                                                                                                                                                             |
| `peer-ca-file`         | —                   | Path to the CA bundle used to verify peer certificates on both sides — the leader's server verifies the follower client cert, and a follower verifies the leader server cert against the same CA.                                                                                                                              |
| `peer-skip-verify`     | `false`             | Set to `true` to disable peer certificate verification (debug only).                                                                                                                                                                                                                                                           |

## Examples

**Local only (no S3, single node):**

```
t4://?data-dir=/var/lib/t4
```

**Single node with S3 durability:**

```
t4://my-bucket/k3s?data-dir=/var/lib/t4
```

**Single node, MinIO:**

```
t4://my-bucket/k3s?data-dir=/var/lib/t4&s3-endpoint=http://minio:9000&region=us-east-1
```

**Three-node cluster on Kubernetes (recommended):**

```
t4://my-bucket/k3s?data-dir=/var/lib/t4&service-name=kine.kube-system.svc.cluster.local
```

`node-id` defaults to the pod hostname, and `peer-listen` / `advertise-peer` are derived from `service-name`
automatically. All three nodes use the same DSN — no per-node configuration needed.

**Three-node cluster (manual peer config):**

```
# node-a
t4://my-bucket/k3s?data-dir=/var/lib/t4&node-id=node-a&peer-listen=0.0.0.0:3380&advertise-peer=node-a.internal:3380

# node-b
t4://my-bucket/k3s?data-dir=/var/lib/t4&node-id=node-b&peer-listen=0.0.0.0:3380&advertise-peer=node-b.internal:3380

# node-c
t4://my-bucket/k3s?data-dir=/var/lib/t4&node-id=node-c&peer-listen=0.0.0.0:3380&advertise-peer=node-c.internal:3380
```

## Peer TLS

WAL streaming between t4 nodes runs over gRPC. By default the peer channel is
plaintext, which is only safe inside a trusted network. To enable mutual TLS,
provision each node with the same set of files and add them to the DSN:

| File                | Role                                                                             |
|---------------------|----------------------------------------------------------------------------------|
| `peer-cert-file`    | This node's certificate — presented as the leader's server cert *and* as the follower's client cert. |
| `peer-key-file`     | Private key matching `peer-cert-file`.                                           |
| `peer-ca-file`      | CA bundle that signed every node's peer cert. Used to verify the peer on both sides. |

All three must be set together. The leader's gRPC server requires and verifies
the follower's client certificate (`tls.RequireAndVerifyClientCert`), and the
follower verifies the leader's server certificate against the same CA.

**Three-node cluster with mTLS:**

```
t4://my-bucket/k3s?data-dir=/var/lib/t4&service-name=kine.kube-system.svc.cluster.local&peer-cert-file=/etc/t4/peer.crt&peer-key-file=/etc/t4/peer.key&peer-ca-file=/etc/t4/peer-ca.crt
```

Certificate SANs must cover the address each node advertises via
`advertise-peer` (or the auto-generated `<hostname>.<service-name>` when
`service-name` is set). For Kubernetes headless services, issuing one
wildcard cert for `*.<service-name>` is the simplest path.

`peer-skip-verify=true` disables peer-cert verification on the client side and
downgrades the server to `tls.RequestClientCert` — useful for short-lived debug
clusters, but never enable it in production.

## S3 Credentials

Credentials are resolved from t4-specific environment variables:

- `T4_S3_ACCESS_KEY_ID`
- `T4_S3_SECRET_ACCESS_KEY`

To use the AWS default credential chain instead, set `T4_S3_PROFILE` to the
shared AWS profile name to load. Without either the t4 access key variables or
`T4_S3_PROFILE`, the driver fails before attempting the AWS credential chain.

No credential configuration is needed inside the DSN.

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
