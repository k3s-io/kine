## Endpoint Format

```
mysql://username:password@tcp(hostname:3306)/database-name
```

If you only supply `mysql://` as the endpoint, kine will attempt to do the following:
* Connect to the MySQL socket at `/var/run/mysqld/mysqld.sock` using the root user and no password
* Create a database with the name `kubernetes`
