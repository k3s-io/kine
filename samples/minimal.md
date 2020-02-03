## Minimal example of using kine
The following example uses kine with a mysql database for persistence.

A sample script is available to generate certs [here](generate-certs.sh)

We can run mysql on a host:

```
docker run --name kine-mysql -v $PWD:/etc/mysql/conf.d -p 3306:3306 -e MYSQL_DATABASE=kine -e MYSQL_ROOT_PASSWORD=$PASSWORD -d mysql:latest
```

This will start mysql db with ssl enabled for client connections.

A sample script is available to generate certs [here](generate-certs.sh)

Run kine on the same host as mysql database:
```
kine --endpoint "mysql://root:$PASSWORD@tcp(localhost:3306)/kine"  --ca-file ca.crt --cert-file server.crt --key-file server.key
```

This will expose the mysql db as an etcd endpoint.

### Using with RKE
Use the following RKE cluster.yml sample to boot up the cluster. 

RKE supports using an external etcd endpoint.

```
nodes:
    - address: 1.1.1.1
      user: ubuntu
      role:
        - controlplane
        - worker
    - address: 2.2.2.2
      user: ubuntu
      role:
        - controlplane
        - worker
cluster_name: "kine-demo"
network:
    plugin: canal
ignore_docker_version: true
services:
    etcd:
        path: /
        external_urls:
        - http://kine:2379
        ca_cert: |-
            -----BEGIN CERTIFICATE-----
            
            -----END CERTIFICATE-----
        cert: |-
            -----BEGIN CERTIFICATE-----
           Cert
            -----END CERTIFICATE-----
        key: |-
            -----BEGIN RSA PRIVATE KEY-----

            -----END RSA PRIVATE KEY-----
```

## Using with kubeadm

You can use the following sample kubeadm-master.cfg to launch a cluster with kine.

```
apiVersion: kubeadm.k8s.io/v1beta2
bootstrapTokens:
- groups:
  - system:bootstrappers:kubeadm:default-node-token
  token: abcdef.0123456789abcdef
  ttl: 24h0m0s
  usages:
  - signing
  - authentication
kind: InitConfiguration
localAPIEndpoint:
  advertiseAddress: 0.0.0.0
  bindPort: 6443
nodeRegistration:
  criSocket: /var/run/dockershim.sock
  name: kubeadm
  taints:
  - effect: NoSchedule
    key: node-role.kubernetes.io/master
---
apiServer:
  timeoutForControlPlane: 4m0s
apiVersion: kubeadm.k8s.io/v1beta2
certificatesDir: /etc/kubernetes/pki
clusterName: kubernetes
controllerManager: {}
dns:
  type: CoreDNS
imageRepository: k8s.gcr.io
kind: ClusterConfiguration
kubernetesVersion: v1.17.0
networking:
  dnsDomain: cluster.local
  serviceSubnet: 10.96.0.0/12
scheduler: {}
controlPlaneEndpoint: "0.0.0.0:6443"
etcd:
  external:
    endpoints:
    - http://k3s:2379
    caFile: ./ca.crt
    certFile: ./server.crt
    keyFile: ./server.key
```

The cluster can then be launched as 

`kubeadm init --config kubeadm-master.cfg --ignore-preflight-errors ExternalEtcdVersion`
