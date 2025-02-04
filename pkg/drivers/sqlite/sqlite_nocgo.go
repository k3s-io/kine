//go:build !cgo
// +build !cgo

package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/server"
)

var errNoCgo = errors.New("this binary is built without CGO, sqlite is disabled")

func New(_ context.Context, _ *drivers.Config) (bool, server.Backend, error) {
	return false, nil, errNoCgo
}

func NewVariant(_ context.Context, _ string, cfg *drivers.Config) (server.Backend, *generic.Generic, error) {
	return nil, nil, errNoCgo
}

func setup(db *sql.DB) error {
	return errNoCgo
}

func init() {
	drivers.Register("sqlite", New)
	drivers.SetDefault("sqlite")
}
