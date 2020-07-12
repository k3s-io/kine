## Kine (Kine is not etcd)

Kine is an etcdshim that translates etcd API to sqlite, Postgres, Mysql, and dqlite

### Features
- Can be ran standalone so any k8s (not just k3s) can use Kine
- Implements a subset of etcdAPI (not usable at all for general purpose etcd)
- Translates etcdTX calls into the desired API (Create, Update, Delete)
- Backend drivers for dqlite, sqlite, Postgres, MySQL, Oracle (experimental)

### Using Oracle Backend (experimental)

The Oracle backend is backed by [godror/godror](https://github.com/godror/godror), that uses a C-based OCI wrapper,
so native OCI drivers are still required to start Kine.
However, as those drivers are proprietary and dynamically linked, you will have to manually download it yourself and 
point your LD_LIBRARY_PATH to the right direction. 

Oracle has outlined the procedure to use the drivers correctly. You can refer to their documentation 
[here](https://docs.oracle.com/en/database/oracle/oracle-database/19/lnoci/instant-client.html#GUID-7D65474A-8790-4E81-B535-409010791C2F).

To download the drivers, please go [here](https://www.oracle.com/hk/database/technologies/instant-client/downloads.html)
and carefully read their licensing terms. (unless you want Oracle to send you the papers)
