Kine (Kine is not etcd)
=======================

Kine is an etcdshim that provides an etcd-compatible API on top of various backend datastores:
- SQL ([SQLite](/pkg/drivers/sqlite), [MySQL/MariaDB](/pkg/drivers/mysql), [Postgres](/pkg/drivers/pgsql))
- [Memory](/pkg/drivers/memory) (ephemeral btree)
- [NATS](/pkg/drivers/nats) (embedded or external JetStream)
- [T4](/pkg/drivers/t4) (standalone or with S3)

## Features
- Can be ran standalone so any k8s (not just K3s) can use Kine
- Implements the subset of the etcd V3 API required by the Kubernetes API server - not intendend to be usable as a generic etcd server
  - [Lease](https://pkg.go.dev/go.etcd.io/etcd/client/v3#Lease): Grant
  - [Watcher](https://pkg.go.dev/go.etcd.io/etcd/client/v3#Watcher): Watch, RequestProgress, Close
  - [KV](https://pkg.go.dev/go.etcd.io/etcd/client/v3#KV): Put, Get, GetStream, Delete, Compact, Txn
  - [Cluster](https://pkg.go.dev/go.etcd.io/etcd/client/v3#Cluster): MemberList
  - [Maintenance](https://pkg.go.dev/go.etcd.io/etcd/client/v3#Maintenance): Status

See an [example](/examples/minimal.md).

## Developer Documentation

A high level flow diagram and overview of code structure is available at [docs/flow.md](/docs/flow.md).
