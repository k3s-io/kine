Kine (Kine is not etcd)
=======================

Kine is an etcdshim that translates etcd API to:
- SQLite
- Postgres
- MySQL/MariaDB
- NATS

## Features
- Can be ran standalone so any k8s (not just K3s) can use Kine
- Implements a subset of etcdAPI (not usable at all for general purpose etcd)
- Translates etcdTX calls into the desired API (Create, Update, Delete)

See an [example](/examples/minimal.md).

## Developer Documentation

A high level flow diagram and overview of code structure is available at [docs/flow.md](/docs/flow.md).
