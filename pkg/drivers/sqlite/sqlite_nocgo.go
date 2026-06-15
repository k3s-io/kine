//go:build !cgo

package sqlite

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/k3s-io/kine/pkg/query"
	"github.com/k3s-io/kine/pkg/server"
	sqlite3 "modernc.org/sqlite"
	sqlite3lib "modernc.org/sqlite/lib"
)

func newConnector(driverName, dsn string) (*sqliteConnector, error) {
	var err error
	dsn, err = translateDSN(dsn)
	if err != nil {
		return nil, err
	}

	driver := &sqlite3.Driver{}
	if driverName == "litestream" {
		driver.RegisterConnectionHook(func(conn sqlite3.ExecQuerierContext, dsn string) error {
			var err error
			if ctrl, ok := conn.(sqlite3.FileControl); ok {
				_, err = ctrl.FileControlPersistWAL("main", 1)
			}
			return err
		})
	}
	return &sqliteConnector{dsn: dsn, driver: driver}, nil
}

// translateDSN translates a subset of mattn/go-sqlite3 DSN parameters to modernc/sqlite _pragmas
// only commonly used params are translated, others are passed through as-is
func translateDSN(dsn string) (string, error) {
	path, params, _ := strings.Cut(dsn, "?")
	old, err := url.ParseQuery(params)
	if err != nil {
		return "", err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", err
	}
	new := make(url.Values)
	addAll := func(key, format string, vals []string) {
		for _, val := range vals {
			new.Add(key, fmt.Sprintf(format, val))
		}
	}
	for key, vals := range old {
		switch key {
		case "_busy_timeout", "_timeout":
			addAll("_pragma", "busy_timeout(%s)", vals)
		case "_journal", "_journal_mode":
			addAll("_pragma", "journal_mode(%s)", vals)
		case "_synchronous", "_sync":
			addAll("_pragma", "synchronous(%s)", vals)
		case "_case_sensitive_like", "_cslike":
			addAll("_pragma", "case_sensitive_like(%s)", vals)
		case "cache":
			// shared cache mode is not supported
		default:
			addAll(key, "%s", vals)
		}
	}
	params, err = url.QueryUnescape(new.Encode())
	if err != nil {
		return "", err
	}
	return "file://" + path + "?" + params, nil
}

func version() string {
	return fmt.Sprintf("modernc.org/sqlite version %s", sqlite3lib.SQLITE_VERSION)
}

func postCompact() *query.Named {
	// wal_checkpoint(FULL) hangs in the busy handler, an issue unique to this sqlite variant
	return query.New(`PRAGMA wal_checkpoint(PASSIVE)`, "?", false, "PostCompactSQL")
}

func translateErr(err error) error {
	if err, ok := err.(*sqlite3.Error); ok && err.Code() == sqlite3lib.SQLITE_CONSTRAINT_UNIQUE {
		return server.ErrKeyExists
	}
	return err
}

func errCode(err error) string {
	if err == nil {
		return ""
	}
	if err, ok := err.(*sqlite3.Error); ok {
		return sqlite3.ErrorCodeString[err.Code()]
	}
	return err.Error()
}
