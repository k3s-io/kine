# SurrealDB Example for Kine

This example demonstrates how to use SurrealDB as a backend for Kine.

## Quick Start

### 1. Start SurrealDB

Using Docker:

```bash
docker run -d \
  --name surrealdb \
  -p 8000:8000 \
  surrealdb/surrealdb:latest \
  start \
  --log trace \
  --user root \
  --pass root \
  memory
```

Or using the SurrealDB CLI:

```bash
surreal start --log trace --user root --pass root memory
```

For persistent storage:

```bash
surreal start --log info --user root --pass root file://data/kine.db
```

### 2. Start Kine with SurrealDB

```bash
kine --endpoint=ws://localhost:8000/kine/kubernetes
```

With custom credentials:

```bash
export SURREALDB_USER=admin
export SURREALDB_PASS=mypassword
kine --endpoint=ws://localhost:8000/kine/kubernetes
```

### 3. Verify the Connection

Check Kine logs for successful connection:

```
INFO Configuring SurrealDB table schema and indexes, this may take a moment...
INFO SurrealDB tables and indexes are up to date
```

## Production Deployment

### High Availability Setup

For production, deploy SurrealDB in a distributed configuration:

```bash
# Node 1
surreal start \
  --log info \
  --user admin \
  --pass secure_password \
  --bind 0.0.0.0:8000 \
  tikv://pd1:2379,pd2:2379,pd3:2379

# Node 2
surreal start \
  --log info \
  --user admin \
  --pass secure_password \
  --bind 0.0.0.0:8000 \
  tikv://pd1:2379,pd2:2379,pd3:2379

# Node 3
surreal start \
  --log info \
  --user admin \
  --pass secure_password \
  --bind 0.0.0.0:8000 \
  tikv://pd1:2379,pd2:2379,pd3:2379
```

### Kine with Production SurrealDB

```bash
kine \
  --endpoint=wss://surrealdb.example.com:8000/production/kubernetes \
  --compact-interval=5m \
  --compact-batch-size=1000 \
  --compact-timeout=30s \
  --datastore-max-idle-connections=10 \
  --datastore-max-open-connections=50
```

## Using with K3s

### Single Node K3s

```bash
curl -sfL https://get.k3s.io | sh -s - server \
  --datastore-endpoint="ws://localhost:8000/kine/kubernetes"
```

### K3s with External SurrealDB

```bash
export SURREALDB_USER=k3s_user
export SURREALDB_PASS=k3s_password

curl -sfL https://get.k3s.io | sh -s - server \
  --datastore-endpoint="ws://surrealdb.internal:8000/production/k3s"
```

### K3s HA Cluster with SurrealDB

```bash
# First server
curl -sfL https://get.k3s.io | sh -s - server \
  --cluster-init \
  --datastore-endpoint="wss://surrealdb.example.com:8000/production/k3s"

# Additional servers
curl -sfL https://get.k3s.io | sh -s - server \
  --server https://first-server:6443 \
  --datastore-endpoint="wss://surrealdb.example.com:8000/production/k3s"
```

## Docker Compose Example

Create a `docker-compose.yml` file:

```yaml
version: '3.8'

services:
  surrealdb:
    image: surrealdb/surrealdb:latest
    container_name: surrealdb
    command: start --log info --user root --pass root file://data/kine.db
    ports:
      - "8000:8000"
    volumes:
      - surrealdb-data:/data
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 10s
      timeout: 5s
      retries: 5

  kine:
    image: rancher/kine:latest
    container_name: kine
    command: 
      - --endpoint=ws://surrealdb:8000/kine/kubernetes
      - --listen-address=0.0.0.0:2379
    ports:
      - "2379:2379"
    environment:
      - SURREALDB_USER=root
      - SURREALDB_PASS=root
    depends_on:
      surrealdb:
        condition: service_healthy
    restart: unless-stopped

volumes:
  surrealdb-data:
```

Start the stack:

```bash
docker-compose up -d
```

## Kubernetes Deployment

### SurrealDB StatefulSet

Create `surrealdb-statefulset.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: surrealdb
  namespace: kube-system
spec:
  ports:
  - port: 8000
    name: web
  clusterIP: None
  selector:
    app: surrealdb
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: surrealdb
  namespace: kube-system
spec:
  serviceName: surrealdb
  replicas: 3
  selector:
    matchLabels:
      app: surrealdb
  template:
    metadata:
      labels:
        app: surrealdb
    spec:
      containers:
      - name: surrealdb
        image: surrealdb/surrealdb:latest
        args:
        - start
        - --log
        - info
        - --user
        - root
        - --pass
        - root
        - file://data/kine.db
        ports:
        - containerPort: 8000
          name: web
        volumeMounts:
        - name: data
          mountPath: /data
        env:
        - name: SURREAL_USER
          valueFrom:
            secretKeyRef:
              name: surrealdb-secret
              key: username
        - name: SURREAL_PASS
          valueFrom:
            secretKeyRef:
              name: surrealdb-secret
              key: password
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: [ "ReadWriteOnce" ]
      resources:
        requests:
          storage: 10Gi
---
apiVersion: v1
kind: Secret
metadata:
  name: surrealdb-secret
  namespace: kube-system
type: Opaque
stringData:
  username: root
  password: change-me-in-production
```

### Kine Deployment

Create `kine-deployment.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kine-config
  namespace: kube-system
data:
  endpoint: "ws://surrealdb-0.surrealdb.kube-system.svc.cluster.local:8000/kine/kubernetes"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kine
  namespace: kube-system
spec:
  replicas: 2
  selector:
    matchLabels:
      app: kine
  template:
    metadata:
      labels:
        app: kine
    spec:
      containers:
      - name: kine
        image: rancher/kine:latest
        args:
        - --endpoint=$(DATASTORE_ENDPOINT)
        - --listen-address=0.0.0.0:2379
        env:
        - name: DATASTORE_ENDPOINT
          valueFrom:
            configMapKeyRef:
              name: kine-config
              key: endpoint
        - name: SURREALDB_USER
          valueFrom:
            secretKeyRef:
              name: surrealdb-secret
              key: username
        - name: SURREALDB_PASS
          valueFrom:
            secretKeyRef:
              name: surrealdb-secret
              key: password
        ports:
        - containerPort: 2379
          name: client
        livenessProbe:
          tcpSocket:
            port: 2379
          initialDelaySeconds: 15
          periodSeconds: 10
        readinessProbe:
          tcpSocket:
            port: 2379
          initialDelaySeconds: 5
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: kine
  namespace: kube-system
spec:
  selector:
    app: kine
  ports:
  - protocol: TCP
    port: 2379
    targetPort: 2379
  type: ClusterIP
```

Apply the configurations:

```bash
kubectl apply -f surrealdb-statefulset.yaml
kubectl apply -f kine-deployment.yaml
```

## Testing and Validation

### 1. Check SurrealDB is Running

```bash
curl http://localhost:8000/health
```

Expected output: `OK`

### 2. Verify Kine Connection

```bash
# Check Kine logs
docker logs kine

# Or if using kubectl
kubectl logs -n kube-system deployment/kine
```

### 3. Test etcd API through Kine

```bash
# Install etcdctl
ETCD_VER=v3.5.9
curl -L https://github.com/etcd-io/etcd/releases/download/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz -o /tmp/etcd.tar.gz
tar xzvf /tmp/etcd.tar.gz -C /tmp
sudo mv /tmp/etcd-${ETCD_VER}-linux-amd64/etcdctl /usr/local/bin/

# Test operations
etcdctl --endpoints=http://localhost:2379 put /test/key "test value"
etcdctl --endpoints=http://localhost:2379 get /test/key
etcdctl --endpoints=http://localhost:2379 del /test/key
```

### 4. Query SurrealDB Directly

```bash
# Install SurrealDB CLI
curl -sSf https://install.surrealdb.com | sh

# Connect to SurrealDB
surreal sql --endpoint ws://localhost:8000 --username root --password root --namespace kine --database kubernetes

# Run queries
> SELECT * FROM kine LIMIT 10;
> SELECT COUNT() FROM kine GROUP ALL;
```

## Performance Testing

### Load Test with etcdctl

```bash
# Write test
for i in {1..1000}; do
  etcdctl --endpoints=http://localhost:2379 put /test/key$i "value$i"
done

# Read test
for i in {1..1000}; do
  etcdctl --endpoints=http://localhost:2379 get /test/key$i
done

# Delete test
for i in {1..1000}; do
  etcdctl --endpoints=http://localhost:2379 del /test/key$i
done
```

### Benchmark with k6

Create `benchmark.js`:

```javascript
import http from 'k6/http';
import { check } from 'k6';

export let options = {
  vus: 10,
  duration: '30s',
};

export default function() {
  let key = `/test/key${Math.floor(Math.random() * 1000)}`;
  let value = `value${Math.random()}`;
  
  let putRes = http.put(`http://localhost:2379/v3/kv/put`, JSON.stringify({
    key: btoa(key),
    value: btoa(value)
  }));
  
  check(putRes, {
    'put successful': (r) => r.status === 200,
  });
  
  let getRes = http.post(`http://localhost:2379/v3/kv/range`, JSON.stringify({
    key: btoa(key)
  }));
  
  check(getRes, {
    'get successful': (r) => r.status === 200,
  });
}
```

Run the benchmark:

```bash
k6 run benchmark.js
```

## Backup and Restore

### Backup SurrealDB Data

```bash
# Export data
surreal export \
  --endpoint ws://localhost:8000 \
  --username root \
  --password root \
  --namespace kine \
  --database kubernetes \
  backup.surql

# Or backup the data file directly (if using file storage)
cp -r /path/to/data/kine.db /backup/location/
```

### Restore SurrealDB Data

```bash
# Import data
surreal import \
  --endpoint ws://localhost:8000 \
  --username root \
  --password root \
  --namespace kine \
  --database kubernetes \
  backup.surql
```

## Troubleshooting

### Common Issues

1. **Connection refused**
   - Ensure SurrealDB is running: `docker ps | grep surrealdb`
   - Check port binding: `netstat -an | grep 8000`

2. **Authentication failed**
   - Verify credentials in environment variables
   - Check SurrealDB logs: `docker logs surrealdb`

3. **Schema errors**
   - Drop and recreate the database:
     ```bash
     surreal sql --endpoint ws://localhost:8000 --username root --password root
     > REMOVE DATABASE kubernetes;
     > USE NS kine DB kubernetes;
     ```

4. **Performance issues**
   - Increase connection pool size
   - Adjust compaction settings
   - Monitor SurrealDB resource usage

### Enable Debug Logging

```bash
# Kine debug logging
kine --endpoint=ws://localhost:8000/kine/kubernetes --log-level=trace

# SurrealDB debug logging
surreal start --log trace --user root --pass root
```

## Next Steps

- Check the [Kine documentation](../docs/flow.md) for architecture details
- Explore [SurrealDB documentation](https://surrealdb.com/docs) for advanced features

## References

- [SurrealDB Installation](https://surrealdb.com/docs/installation)
- [SurrealDB Query Language](https://surrealdb.com/docs/surrealql)
- [Kine GitHub Repository](https://github.com/k3s-io/kine)
- [K3s Documentation](https://docs.k3s.io)
