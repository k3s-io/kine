package drivers

import (
	"context"

	"github.com/k3s-io/kine/pkg/server"
)

// Constructor is a function that takes a context and a config and returns a leaderElect bool, a server.Backend and an error
type Constructor func(ctx context.Context, cfg *Config) (leaderElect bool, backend server.Backend, err error)

var driverRegistry = map[string]Constructor{}
var defaultScheme string

// Register registers a constructor for the given scheme
func Register(scheme string, constructor Constructor) {
	driverRegistry[scheme] = constructor
}

// SetDefault sets the default driver scheme
// The default driver is used when an endpoint is not specified
func SetDefault(scheme string) {
	defaultScheme = scheme
}

// GetDefault returns the default driver
func GetDefault() Constructor {
	return driverRegistry[defaultScheme]
}

// Get returns the constructor for the given scheme
// The second return value is true if the scheme is registered, false otherwise
func Get(scheme string) (Constructor, bool) {
	constructor, ok := driverRegistry[scheme]
	return constructor, ok
}
