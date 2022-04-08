# Kine (Kine is not etcd)
==========================

_NOTE: this repository has been recently (2020-11-19) moved out of the github.com/rancher org to github.com/k3s-io
supporting the [acceptance of K3s as a CNCF sandbox project](https://github.com/cncf/toc/pull/447)_.

---

Kine is an etcdshim that translates etcd API to sqlite, Postgres, Mysql, and dqlite

## Features
- Can be ran standalone so any k8s (not just k3s) can use Kine
- Implements a subset of etcdAPI (not usable at all for general purpose etcd)
- Translates etcdTX calls into the desired API (Create, Update, Delete)
- Backend drivers for dqlite, sqlite, Postgres, MySQL and NATS JetStream
