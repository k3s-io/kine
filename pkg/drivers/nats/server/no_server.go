//go:build !nats
// +build !nats

package server

import (
	"errors"
)

func New(_ string, _, _ bool) (Server, error) {
	return nil, errors.New(`this binary is built without embedded NATS support, compile with "-tags nats"`)
}
