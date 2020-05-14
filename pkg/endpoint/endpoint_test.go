package endpoint

import (
	"context"
	"testing"
)

func TestListen(t *testing.T) {
	var config Config
	var ctx context.Context
	config.Listener = ""
	endpoints := [5]string{
		"postgres://123",
		"postgres://",
		"text",
		"mysql://",
		"mysql://123",
	}
	for i := range endpoints {
		config.Endpoint = endpoints[i]
		_, err := Listen(ctx, config)
		if err == nil {
			t.Error("unpassed error !! ")
		}
	}
}
