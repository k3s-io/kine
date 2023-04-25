//go:build !nats
// +build !nats

package server

import (
	"errors"
)

const (
	Embedded = false
)

func New(*Config) (Server, error) {
	return nil, errors.New(`this binary is built without embedded NATS support, compile with "-tags nats"`)
}
