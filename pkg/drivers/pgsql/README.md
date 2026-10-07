## Endpoint Format

```
postgres://username:password@hostname:port/database-name
```

If you only supply `postgres://` as the endpoint, kine will attempt to do the following:
* Connect to localhost using `postgres` as the username and password
* Create a database named `kubernetes`
