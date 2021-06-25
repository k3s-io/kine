package client

import (
	"context"
	"testing"

	"github.com/k3s-io/kine/pkg/endpoint"
)

var (
	config = endpoint.ETCDConfig{Endpoints: []string{"localhost:2379"}}
)

func TestList(t *testing.T) {
	c, err := New(config)
	if err != nil {
		t.Errorf("Unable to create new client: %v", err)
	}

	_, err = c.List(context.TODO(), "/bootstrap", 0)
	if err != nil {
		t.Errorf("Failed to list /bootstrap: %v", err)
	}
}

func TestCreateAndList(t *testing.T) {
	c, err := New(config)
	if err != nil {
		t.Errorf("Unable to create new client: %v", err)
	}

	err = c.Create(context.TODO(), "/bootstrap/test", []byte("test"))

	values, err := c.List(context.TODO(), "/bootstrap", 0)
	if err != nil {
		t.Errorf("Failed to list /bootstrap: %v", err)
	}

	for _, v := range values {
		t.Logf("Got value: %s=%s", v.Key, v.Data)
	}
}
