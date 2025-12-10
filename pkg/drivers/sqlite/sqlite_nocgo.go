//go:build !cgo

package sqlite

import (
	"context"
	"fmt"
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

func translateErr(err error) error {
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
