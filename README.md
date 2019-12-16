## Kine (Kine is not etcd)

Kine is an etcdshim that translates etcdapito sqlite, Postgres, Mysql, dqlite

### Features
- Can be ran standalone so any k8s (not just k3s) can use Kine
- Implements a subset of etcdAPI (not usable at all for general purpose etcd)
- Translates etcdTX calls into the desired API (Create, Update,  Delete)
- Backend drivers for dqlite, sqlite, Postgres, MySQL
