package server

import "time"

type Server interface {
	Start()
	Shutdown()
	ClientURL() string
	ReadyForConnections(wait time.Duration) bool
}
