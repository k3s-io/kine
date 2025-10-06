package identity

import (
	"context"
	"errors"
)

var ErrUnknownIdentityProvider = errors.New("unknown identity provider")

func New(ctx context.Context, providerName string) (TokenSource, error) {
	if providerName == "" {
		return nil, nil
	}

	provider, ok := Get(providerName)
	if !ok {
		return nil, ErrUnknownIdentityProvider
	}

	tokenSource, err := provider(ctx)
	if err != nil {
		return nil, err
	}
	return tokenSource, nil
}
