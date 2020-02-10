## Minimal example of using kine
The following example uses kine with a mysql database for persistence.

We can run mysql on a host:

```
docker run --name kine-mysql -v $PWD:/etc/mysql/conf.d -p 3306:3306 -e MYSQL_DATABASE=kine -e MYSQL_ROOT_PASSWORD=$PASSWORD -d mysql:latest
```

This will start mysql db with ssl enabled for client connections.

Run kine on the same host as mysql database:
```
kine --endpoint "mysql://root:$PASSWORD@tcp(localhost:3306)/kine"  --ca-file ca.pem --cert-file cert.pem --key-file cert.key
```

This will expose the mysql db as an etcd endpoint protected by the certs used to connect to the mysql database.


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
        - https://3.3.3.3:2379
        ca_cert: |-
            -----BEGIN CERTIFICATE-----
            MIIDVTCCAj2gAwIBAgIUZV9P6JhHOgjT5cRHdsX0rUp6q2AwDQYJKoZIhvcNAQEL
            BQAwOjELMAkGA1UEBhMCQVUxDDAKBgNVBAgMA1ZJQzEOMAwGA1UECgwFcmFuY2gx
            DTALBgNVBAMMBG15Q0EwHhcNMjAwMjAyMjI1NzAzWhcNMjUwMTMxMjI1NzAzWjA6
            MQswCQYDVQQGEwJBVTEMMAoGA1UECAwDVklDMQ4wDAYDVQQKDAVyYW5jaDENMAsG
            A1UEAwwEbXlDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAOWU/5KI
            3Es7fZeP7zpgl1t/EeIYtZHNQKDozTjuaNgdvaDYp5Ly1ARlrBDBJTJcUkVjUpbN
            33WeAsOew3YKWjNNPw1nC9x8r1BgdOwgJhPOFQ7paHHYVJMLPJ3B1qz31ZSkqjyB
            vCT7/qMijY8YJkM70FYIe/MokM9iLfFKP1RLlB/kRHgVOF3LT/uvknPX1Rg0tOmI
            8SrjChAkMwSC0LruBJnzAuAs1z03qLWl0R3uEGnRF3o9P6QzJnEsnXMs692IWwt2
            4ilxuC5yq9Km/gCuJMzhGsqkRpdRSutXgXnMWUSCk2LQQJK9qLTpEbxWY50LAF+Q
            Yctr7kmVC1H69skCAwEAAaNTMFEwHQYDVR0OBBYEFFf6jIEgiCAaA3zNllILqVg+
            fACnMB8GA1UdIwQYMBaAFFf6jIEgiCAaA3zNllILqVg+fACnMA8GA1UdEwEB/wQF
            MAMBAf8wDQYJKoZIhvcNAQELBQADggEBAJ3508ltZz0CkoEvMqq4Ux5dn+rE+J/f
            eq5kjjE/QB+HZnU1089OLwLilPPF7yMOGPhLt7sZw1/Ymqm7l6yrl2grL90brz2i
            DvDHw3N9fxRxnvjUeg61JOk6vOk/It6odJ2Lbht56L1PsgENEe2ih2kYy/i3NC6z
            yFcNtg3xR+iSO/2Gp1/UICDWVx7n4VLrbC34AKwuHF+WZDOLymk0MwtL6CV02U0W
            KOShfFMtkve95ZZtEojypGr+EhIHePLmaleTwxjgG15WXQLxKcPzDbg9I4je3FII
            U/nen9pF8UJ3+H6Mxotw4vOcIuafZrFLM7pXsyv00UUSzbvWiH9+X4I=
            -----END CERTIFICATE-----
        cert: |-
            -----BEGIN CERTIFICATE-----
            MIIDXTCCAkWgAwIBAgIUMakvO9NvNAeXHOaxOZdg+7e4evMwDQYJKoZIhvcNAQEL
            BQAwOjELMAkGA1UEBhMCQVUxDDAKBgNVBAgMA1ZJQzEOMAwGA1UECgwFcmFuY2gx
            DTALBgNVBAMMBG15Q0EwHhcNMjAwMjAyMjMzOTM1WhcNMjUwMTMxMjMzOTM1WjA/
            MQswCQYDVQQGEwJBVTEMMAoGA1UECAwDVklDMQ4wDAYDVQQKDAVyYW5jaDESMBAG
            A1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA
            oPOFYtnP4hXnDgGl0zN+Q8pwcXr5+fyS4/YVIciAgwG8z8gVSPgf9HakiPhJ2Z7Q
            eVbH5xikS0kzzPKS+fdu+nAJtbW6vTvP2XizkJx1xGcuC0jhmNYT3A8vCskzSuno
            YplVBGUvNyBBPaoW1PiUWzqLtO39wnleizg8ilcnoTje3owO/OQpxSCFVAezxJEv
            8gAQu0+wjVYPs2PAbmKKrXKo4aqGy6hVL/aIhEOXni4cMmxOl/UDCmV1SsmHKICk
            J6/v1kRcRyUdC2Gj7RxU+i0ipCUG9ZqQbnvNEhgQqWVzYdf+jIHu+wEVJEGNejU4
            CfKaC/za8jM98g/ui6bF3wIDAQABo1YwVDAfBgNVHSMEGDAWgBRX+oyBIIggGgN8
            zZZSC6lYPnwApzAJBgNVHRMEAjAAMAsGA1UdDwQEAwIE8DAZBgNVHREEEjAQggNr
            M3OCA2s4c4cEfwAAATANBgkqhkiG9w0BAQsFAAOCAQEAmQUrvASk70Kf1T23Dhng
            NFuq8mNrsm/IOPgxrVYwzG1nzhop9wa3060YHVQ39B2QSylVnkYwyfDWY0064RXx
            OrMOig6hsos+KMZKNhwHnVseZj33quz+FJZWIfByeAFenJ8z4j2Zx8QbxHznytOc
            lO+fKw+lbRId3ZyW4E/g4kChK6VXVCZ6+9VXEj/pmp77p8q8NYk5setS4HkQpJxN
            Y7x/tDvNyo8efz1VP3yjeNZ4WGANkYUx8O+rOzq31Qwopf6OBLlllumTXeDGU/n/
            fi2eFpSTPmAZZK0nlwu2T1YHbA4hxrX7HvVeqOVxypRmVZ1nLz2poUMUtphP6a+4
            ug==
            -----END CERTIFICATE-----
        key: |-
            -----BEGIN RSA PRIVATE KEY-----
            MIIEpAIBAAKCAQEAoPOFYtnP4hXnDgGl0zN+Q8pwcXr5+fyS4/YVIciAgwG8z8gV
            SPgf9HakiPhJ2Z7QeVbH5xikS0kzzPKS+fdu+nAJtbW6vTvP2XizkJx1xGcuC0jh
            mNYT3A8vCskzSunoYplVBGUvNyBBPaoW1PiUWzqLtO39wnleizg8ilcnoTje3owO
            /OQpxSCFVAezxJEv8gAQu0+wjVYPs2PAbmKKrXKo4aqGy6hVL/aIhEOXni4cMmxO
            l/UDCmV1SsmHKICkJ6/v1kRcRyUdC2Gj7RxU+i0ipCUG9ZqQbnvNEhgQqWVzYdf+
            jIHu+wEVJEGNejU4CfKaC/za8jM98g/ui6bF3wIDAQABAoIBAGkfxU5UP2BGt/xA
            /UMeDelPLvQfw2gRHQwBrbm8EJwApYb9A1H+pjhwyXSg3vNhtH6cPMLnKF/39vp0
            saTMhNLUHLNveAGjMFW1bWsVliHq1nsOZjMCGESSMkKUOYlDj8HerlXJlPYnfhU9
            o94EYjnX2moZS7YaubKqz3f4Bu1Yg4kHQWRRoD07pU0CXRjBZpg1ALhW3aAntuZo
            AL2TXa6dharA6wVi4tC64AbPvVoXQ9Pc96sEHs7yNxjwtfPnk2dZtN4XnKF6lcA7
            VnDVnVkcXEEsZ+YQYjfooOiIPHpG0Q0ymqaAydNoQ0xwS9V/0oFvWsPQ++ZwgOek
            mSLQctECgYEA0SLO9+tPaBF5t2fbX2TjlIU1/dnfLc7w2H7VHTV3QqYq3zYWRQZF
            bTEnBbWM81CHZPYTEmY+JToaUavHs2il1WfpfZQmYyhOUiXg9Jk8w3aYo5b0aVH2
            E/doNN6txuQ3c3xoJF3HzsOMLuinz3cYrnvGW0nQkUj0F5di5aMSRncCgYEAxQSS
            XjwIBgl8aR8WjbWJ7kI9Xk5Xx/aTP4FzUTmHSLXY4zxWFv7zRV3vHSzEEsKdxnYr
            mxtIRim1n6btNm6KCBqGOCDBHq2bWxuoFMODVZiMwVTYeqZcrOHlqh+vLcKNbJsO
            4jImvNwN8F2h9T0DBiXamL9pVWFt2bAZlY1TDdkCgYEAq0K6AkPsTuigqBSgjMnt
            pB5CTJMyNC0XBfM3SigSdb3ltcxxCC1OhVCPCBnYRxhXB9KLY7HeilW+X8swSjcU
            NmJVzsSXevPyz0q9oRArtlVUQgLIO8cmoMslxsXjwM/6qNPj5IP3r9Zq4a8cXMTG
            rXwmv3L/HTqEyRzrm+mieZkCgYEAxC6gPTvT1YcenmK5h5Ss61aEW2LxoAmVaJhT
            px786ldByEytgSqQPZOi5c9M00195ECJfWL2Xf9sfrSu4xPBWP5ohn1/MDg5ScjJ
            XxusrNBB4MXG5qLAB9rNYdE5E/z17J6efHjqAAezzZS/ED+XwkhxWsbHcaCZzTnA
            0B2xBUkCgYB0+bFGxDetRYSNEPBUiMxCUvzzwAR0bqXpFronTu8pTsQKGlnddJh3
            tNVseLswaQ9A8j1foxvTGv5/GYrPq7rpcKjPDkJs4CgLFTIBM0/t2v8wNwfvzJa5
            017peVJ5tbqdqphS8lrfdM0M509ao7ehqTMnu8STYWoLib54ZZG8Gw==
            -----END RSA PRIVATE KEY-----
```

