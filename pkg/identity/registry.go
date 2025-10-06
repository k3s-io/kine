package identity

import (
	"context"
)

type TokenSource func(ctx context.Context, config Config) (string, error)

// Provider is a function that takes a context, and credentials and sets a temporary generated password
type Provider func(ctx context.Context) (TokenSource, error)

var providerRegistry = map[string]Provider{}

// Register registers a provider for the given name
func Register(name string, provider Provider) {
	providerRegistry[name] = provider
}

// Get returns the provider for the given name
// The second return value is true if the name is registered, false otherwise
func Get(name string) (Provider, bool) {
	provider, ok := providerRegistry[name]
	return provider, ok
}
