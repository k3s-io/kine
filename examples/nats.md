# Using the NATS backend

[NATS](https://nats.io) is a high-performance connective technology supporting a spectrum of messaging, streaming, and materialization capabilities.

One supported materialization a key-value store which is what is used in the implementation of the KINE backend.

There are two options for integrating NATS:

- As an external server or cluster KINE connects to
- As an embedded server within KINE itself

## Configuring KINE

This is done by specifying the `--endpoint` option having the following format where everything but the `nats://` scheme is optional.

```
nats://[[[<auth>@]<host>]:<port>][?<params>]`
```

The above tokens are defined as follows:

- `auth` - Inline username and password or standlaone token. Defaults is nothing.
- `host` - Hostname or IP address the NATS server is addressed to. Default is `127.0.0.1`.
- `port` - Port to bind the connection with the NATS server. Default is `4222`.
- `params` - Query parameters which are listed below with defaults.

The following query parameters are supported:

- `bucket` - Specifies the name of the NATS key-value bucket. Default is `kine`.
- `replicas` - Specifies the number of replicas for the bucket. Default is `1`.
- `revHistory` - Specifies the number of revisions to keep in history. Default is `10`.
- `slowMethod` - Specifies the duration of a method before it is considered slow. Default is `500ms`.
- `contextFile` - Specifies the path to a NATS context file. If this is provided, the `auth` and `host` should not be provided. See the available [options](https://docs.nats.io/using-nats/nats-tools/nats_cli#configuration-contexts). Default is nothing.
- `embedServer` - Specifies whether to embed a NATS server rather than relying on an external one. Default is `false`.
- `serverConfig` - Specifies the path to a NATS server configuration file if `embedServer` is `true`. Defaults to nothing.
- `dontListen` - Specifies whether the embedded NATS server should not rely on the TCP stack for a local client connection. Defaults to `false`
- `stdoutLogging` - Specifies whether to log to STDOUT if `embedServer` is `true`. Default is false.

## Examples

### External

- `nats://`
- `nats://localhost:4222?replicas=3`
- `nats://?contextFile=/path/to/context.json`
- `nats://user:pass@localhost:4222`
- `nats://token@localhost`

Multiple URLs can be passed in a comma separated format, however only the first URL
in the list will be evaluated for query parameters. For example:

```
nats://nats-1.example.com?bucket=k3s,nats://nats-2.example.com,nats://nats-3.example.com
```

However, if a `contextFile` is feasible to specify, this is recommended rather than an inline comma-separated list.

### Embedded

- `nats://?embedServer`
- `nats://?embedServer&serverConfig=/path/to/server.conf`
- `nats://?embedServer&dontListen`
