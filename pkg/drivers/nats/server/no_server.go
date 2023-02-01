//go:build !nats
// +build !nats

package server

import (
	"errors"
)

func New(configFile string) (Server, error) {
	return nil, errors.New(`this binary is built without embedded nats support, compile with "-tags nats"`)
}
