//go:build !cgo

package sqlite

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/k3s-io/kine/pkg/query"
	"github.com/k3s-io/kine/pkg/server"
	sqlite3lib "github.com/ncruces/go-sqlite3"
	sqlite3 "github.com/ncruces/go-sqlite3/driver"
)

func newConnector(driverName, dsn string) (*sqliteConnector, error) {
	var err error
	dsn, err = translateDSN(dsn)
	if err != nil {
		return nil, err
	}

	driver := &sqlite3.SQLite{}
	if driverName == "litestream" {
		sqlite3lib.AutoExtension(func(c *sqlite3lib.Conn) error {
			_, err := c.FileControl("main", sqlite3lib.FCNTL_PERSIST_WAL, 1)
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
	return "github.com/ncruces/go-sqlite3"
}

func postCompact() *query.Named {
	return query.New(`PRAGMA wal_checkpoint(FULL)`, "?", false, "PostCompactSQL")
}

func translateErr(err error) error {
	var sqlErr *sqlite3lib.Error
	if errors.As(err, &sqlErr) && sqlErr.ExtendedCode() == sqlite3lib.CONSTRAINT_UNIQUE {
		return server.ErrKeyExists
	}
	return err
}

func errCode(err error) string {
	if err == nil {
		return ""
	}
	var sqlErr *sqlite3lib.Error
	if errors.As(err, &sqlErr) {
		return sqlErr.ExtendedCode().Error()
	}
	return err.Error()
}
