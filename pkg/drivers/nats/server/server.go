//go:build nats
// +build nats

package server

import (
	"fmt"

	"github.com/nats-io/nats-server/v2/server"
)

func New(configFile string, dontListen, stdoutLogging bool) (Server, error) {
	opts := &server.Options{}

	if configFile == "" {
		// TODO: Other defaults for easy single node config?
		opts.JetStream = true
	} else {
		// Parse the server config file as options.
		var err error
		opts, err = server.ProcessConfigFile(configFile)
		if err != nil {
			return nil, fmt.Errorf("failed to process NATS server config file: %w", err)
		}
	}

	opts.DontListen = dontListen
	opts.Port = 0

	srv, err := server.NewServer(opts)
	if stdoutLogging {
		srv.ConfigureLogger()
	}

	return srv, err
}
