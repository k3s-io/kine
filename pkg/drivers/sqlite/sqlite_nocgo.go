//go:build !cgo

package sqlite

import (
	"context"

	"fmt"
	"net/url"
	"sync"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	backend, _, err := NewVariant(ctx, wg, "sqlite", cfg)
	return false, backend, err
}

func postCompactSQL() string {
	return `PRAGMA wal_checkpoint(PASSIVE)`
}

func translateError(err error) error {
	if err, ok := err.(*sqlite.Error); ok && err.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE {
		return server.ErrKeyExists
	}
	return err
}

func errorCode(err error) string {
	if err == nil {
		return ""
	}
	if err, ok := err.(*sqlite.Error); ok {
		return fmt.Sprint(sqlite.ErrorCodeString[err.Code()])
	}
	return err.Error()
}

func defaultDSNParams(queryString string) string {
	query, _ := url.ParseQuery(queryString)

	paramString := ""
	key := ""
	if _, ok := query["_journal"]; ok {
		key = "_journal"
	}
	if query.Has(key) {
		paramString += "_pragma=journal_mode(" + query.Get(key) + ")&"
		paramString += "_pragma=synchronous(NORMAL)&"
	}

	key = "cache"
	if query.Has(key) {
		paramString += "cache=" + query.Get(key) + "&"
	}

	key = ""
	if _, ok := query["_busy_timeout"]; ok {
		key = "_busy_timeout"
	}
	if query.Has(key) {
		paramString += "_pragma=busy_timeout(" + query.Get(key) + ")&"
	}

	key = "_txlock"
	if query.Has(key) {
		paramString += "_txlock=" + query.Get(key)
	}

	if len(paramString) == 0 && len(query) == 0 {
		paramString = "_pragma=journal_mode(WAL)&"
		paramString += "_pragma=synchronous(NORMAL)&"
		paramString += "cache=shared&"
		paramString += "_pragma=busy_timeout(30000)&"
		paramString += "_txlock=immediate"
		return paramString
	}
	if len(paramString) > 0 {
		return paramString
	}

	return queryString
}
