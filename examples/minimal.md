# Minimal example of using kine

The following example uses kine with a `mysql` database for persistence.

A sample script is available to generate certs [here](generate-certs.sh)
But you also will need to use `SAN` instead of `CN`
We have a example of the files you will need to generate the certs for the [server](server_openssl.cnf)

We can run `mysql` on a host using `docker` [here](Dockerfile)
you will also need the `cnf` file [here](mysql-ssl.cnf)

```bash
docker build -t mysql-kine .
```

```bash
docker run --name kine-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=$PASSWORD -d mysql-kine
```

this will start `mysql` db with everything you will need.

## Running kine standalone

Run kine on the same host as `mysql` database:

```bash
kine --endpoint "mysql://root:$PASSWORD@tcp(localhost:3306)/kine"
--ca-file ca.crt --cert-file server.crt --key-file server.key
```

This will expose the `mysql` db as an `etcd` endpoint.

## Using with k3s

You can use the following command to launch a `k3s` server with kine.

```bash
k3s server --datastore-endpoint "mysql://root:$PASSWORD@tcp(localhost:3306)/kine"
--datastore-cafile ca.crt --datastore-certfile server.crt --datastore-keyfile server.key
```

And that's it! You can now use `k3s` with `mysql` as a db.

## Using with AWS RDS IAM authentication for PostgresSQL

Kine supports IAM database authentication for PostgreSQL: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html

Create an IAM user in PostgresSQL with these grants:

```sql
CREATE USER iam_user_kine CREATEDB;
GRANT rds_iam TO iam_user_kine;
```

Specify the user in the connection string without a password e.g.

```bash
kine --endpoint "postgres://iam_user_kine@$RDS_HOST:5432/kine?sslmode=verify-full&sslrootcert=rds-global-bundle.pem"
```

By default, the AWS region used for authentication will match the environment where kine is being run (via IDMS lookup), however this can be overriden by setting environment variable `AWS_REGION`.