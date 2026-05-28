//go:build cgo

package sqlite

import (
	"fmt"

	"github.com/k3s-io/kine/pkg/query"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/mattn/go-sqlite3"
)

func newConnector(driverName, dsn string) (*sqliteConnector, error) {
	driver := &sqlite3.SQLiteDriver{}
	if driverName == "litestream" {
		driver.ConnectHook = func(conn *sqlite3.SQLiteConn) (err error) {
			return conn.SetFileControlInt("main", sqlite3.SQLITE_FCNTL_PERSIST_WAL, 1)
		}
	}
	return &sqliteConnector{dsn: dsn, driver: driver}, nil
}

func version() string {
	version, _, _ := sqlite3.Version()
	return fmt.Sprintf("github.com/mattn/go-sqlite3 version %s", version)
}

func postCompact() *query.Named {
	return query.New(`PRAGMA wal_checkpoint(FULL)`, "?", false, "PostCompactSQL")
}

func translateErr(err error) error {
	if err, ok := err.(sqlite3.Error); ok && err.ExtendedCode == sqlite3.ErrConstraintUnique {
		return server.ErrKeyExists
	}
	return err
}

func errCode(err error) string {
	if err == nil {
		return ""
	}
	if err, ok := err.(sqlite3.Error); ok {
		return fmt.Sprint(err.ExtendedCode)
	}
	return err.Error()
}
