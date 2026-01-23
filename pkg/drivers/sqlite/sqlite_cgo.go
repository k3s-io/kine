//go:build cgo

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/mattn/go-sqlite3"
)

func NewWithLitestream(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	backend, _, err := NewVariant(ctx, wg, "litestream", cfg, true)
	return false, backend, err
}


func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	backend, _, err := NewVariant(ctx, wg, "sqlite3", cfg, false)
	return false, backend, err
}

func postCompactSQL() string {
	return `PRAGMA wal_checkpoint(FULL)`
}

func translateError(err error) error {
	if err, ok := err.(sqlite3.Error); ok && err.ExtendedCode == sqlite3.ErrConstraintUnique {
		return server.ErrKeyExists
	}
	return err
}

func errorCode(err error) string {
	if err == nil {
		return ""
	}
	if err, ok := err.(sqlite3.Error); ok {
		return fmt.Sprint(err.ExtendedCode)
	}
	return err.Error()
}

func defaultDSNParams(queryString string) string {
	if len(queryString) == 0 {
		return "_journal=WAL&cache=shared&_busy_timeout=30000&_txlock=immediate"
	}
	return queryString
}

func init() {
	sql.Register("litestream", &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) (err error) {
			return conn.SetFileControlInt("main", sqlite3.SQLITE_FCNTL_PERSIST_WAL, 1)
		},
	})
}
