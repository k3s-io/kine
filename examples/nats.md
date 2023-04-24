# Using the NATS backend

[NATS](https://nats.io) is a high-performance connective technology supporting a spectrum of messaging, streaming, and state materialization capabilities.

The Key-Value (KV) capability is used in the KINE backend and supports:

- Optimized get, put, create, and update commands
  - create expects the key to not exist (or was previously deleted)
  - update requires a version number used for OCC against the key
- Historical and versioned values per key
- Subscription of changes for the entire KV store a subset of keys

There are two options for using NATS:

- As an external NATS deployment KINE connects to
- As an embedded server within KINE itself

## Configuring KINE

This is done by specifying the `--endpoint` option having the following format where everything but the `nats://` scheme is optional.

```
nats://[[[<auth>@]<host>]:<port>][?<params>]`
```

The above tokens are defined as follows:

- `host` - Hostname or IP address the NATS server is addressed to. Default is `0.0.0.0`.
- `port` - Port to bind the connection with the NATS server. Default is `4222`.
- `auth` - Inline username and password or standlaone token. Defaults is nothing.
- `params` - Query parameters which are listed below with defaults.

The following client query parameters are supported:

- `bucket` - Specifies the name of the NATS key-value bucket. Default is `kine`.
- `replicas` - Specifies the number of replicas for the bucket. Default is `1`.
- `revHistory` - Specifies the number of revisions to keep in history. Default is `10`.
- `slowMethod` - Specifies the duration of a method before it is considered slow. Default is `500ms`.
- `contextFile` - Specifies the path to a NATS context file. If this is provided, the `auth` and `host` should not be provided. See the available [options](https://docs.nats.io/using-nats/nats-tools/nats_cli#configuration-contexts). Default is nothing.

These query parameters are relevant when the server is embedded:

- `serverConfig` - Specifies the path to a NATS server configuration file in embedded mode. Defaults to nothing.
- `dontListen` - Specifies whether the embedded NATS server should not rely on the TCP stack for a local client connection. Defaults to `false`
- `noEmbed` - Specifies whether to explicitly _not_ run an embedded server and instead use an external server. Default is `false`.
- `stdoutLogging` - Specifies whether to log to STDOUT if running in embedded mode. Default is false.

### Examples

KINE can be built with our without embedding NATS.

If NATS is embedded, `nats://` defaults to running an embedded server using the default `host` and `port`.

For an embedded server, a path to a custom server configuration file can be provided.

```
nats://?serverConfig=/path/to/server.conf
```

If there is a need to disable the TCP listener, specify `dontListen` and the local client connection will talk to the server over a socket.

```
nats://?dontListen
```

If an external NATS server should be used, then `noEmbed` can be set to explicitly not run an embedded server.

```
nats://?noEmbed
```

Multiple URLs can be passed in a comma separated format, however only the first URL
in the list will be evaluated for query parameters. For example:

```
nats://nats-1.example.com,nats://nats-2.example.com,nats://nats-3.example.com
```
