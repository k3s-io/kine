package server

import (
	"net"
)

type Server interface {
	Start()
	Ready() bool
	Shutdown()
	ClientURL() string
	InProcessConn() (net.Conn, error)
}

type Config struct {
	Host          string
	Port          int
	ConfigFile    string
	DontListen    bool
	StdoutLogging bool
	DataDir       string
}
