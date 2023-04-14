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
