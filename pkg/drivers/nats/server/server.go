//go:build nats
// +build nats

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/sirupsen/logrus"
)

const (
	Embedded = true
)

type responseWriter struct {
	code   int
	header http.Header
	body   *bytes.Buffer
}

func (w *responseWriter) Header() http.Header {
	return w.header
}

func (w *responseWriter) Write(b []byte) (int, error) {
	return w.body.Write(b)
}

func (w *responseWriter) WriteHeader(code int) {
	w.code = code
}

type embeddedServer struct {
	*server.Server
}

func (s *embeddedServer) Ready() bool {
	rw := responseWriter{
		header: http.Header{},
		body:   &bytes.Buffer{},
	}

	r := http.Request{
		Method: "GET",
		URL: &url.URL{
			Path: "/healthz",
		},
		Header: http.Header{},
	}

	s.Server.HandleHealthz(&rw, &r)

	var hs server.HealthStatus
	json.NewDecoder(rw.body).Decode(&hs)
	logrus.Debugf("embedded NATS server health: %#v", hs)

	return hs.Status == "ok"
}

func New(c *Config) (Server, error) {
	opts := &server.Options{}

	if c.ConfigFile != "" {
		// Parse the server config file as options.
		var err error
		opts, err = server.ProcessConfigFile(c.ConfigFile)
		if err != nil {
			return nil, fmt.Errorf("failed to process NATS server config file: %w", err)
		}
	}

	// TODO: Other defaults for embedded config?
	// Explicitly set JetStream to true since we need the KV store.
	opts.JetStream = true

	// Note, if don't listen is set, host and port will be ignored.
	opts.DontListen = c.DontListen

	// Only override host and port if set explicitly.
	if c.Host != "" {
		opts.Host = c.Host
	}
	if c.Port != 0 {
		opts.Port = c.Port
	}
	if c.DataDir != "" {
		opts.StoreDir = c.DataDir
	}

	srv, err := server.NewServer(opts)
	if err != nil {
		return nil, err
	}

	if c.StdoutLogging {
		srv.ConfigureLogger()
	}

	return &embeddedServer{Server: srv}, nil
}
