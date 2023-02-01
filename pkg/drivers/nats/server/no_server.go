//go:build !nats
// +build !nats

package server

import (
	"errors"
)

func New(configFile string, stdoutLogging bool) (Server, error) {
	return nil, errors.New(`this binary is built without embedded NATS support, compile with "-tags nats"`)
}
