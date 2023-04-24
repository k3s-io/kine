package server

import (
	"net"
	"time"
)

type Server interface {
	Start()
	Shutdown()
	ClientURL() string
	ReadyForConnections(wait time.Duration) bool
	InProcessConn() (net.Conn, error)
}

type Config struct {
	Host          string
	Port          int
	ConfigFile    string
	DontListen    bool
	StdoutLogging bool
}
