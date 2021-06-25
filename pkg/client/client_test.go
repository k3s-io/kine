package client

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/endpoint"
)

const (
	testTimeout = 1 * time.Second
)

func getClient(t *testing.T) (Client, error) {
	e := endpoint.ETCDConfig{Endpoints: []string{"localhost:2379"}}
	if str := os.Getenv("K3S_DATASTORE_ENDPOINT"); str != "" {
		// Strip off the scheme, if present
		parts := strings.SplitN(str, "://", 2)
		if len(parts) > 1 {
			str = parts[1]
		}
		e.Endpoints = strings.Split(str, ",")
	}
	t.Logf("Connecting to etcd at %v", e.Endpoints)
	return New(e)
}

func TestList(t *testing.T) {
	c, err := getClient(t)
	if err != nil {
		t.Errorf("Unable to create new client: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	values, err := c.List(ctx, "/bootstrap", 0)
	if err != nil {
		t.Errorf("Failed to list /bootstrap: %v", err)
	}
	if len(values) != 0 {
		t.Error("Expected 0 values in list response")
	}
}

func TestCreateAndList(t *testing.T) {
	c, err := getClient(t)
	if err != nil {
		t.Errorf("Unable to create new client: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	err = c.Create(ctx, "/bootstrap/test", []byte("test"))
	if err != nil {
		t.Errorf("Failed to create /bootstrap/test: %v", err)
	}

	values, err := c.List(ctx, "/bootstrap", 0)
	if err != nil {
		t.Errorf("Failed to list /bootstrap: %v", err)
	}

	if len(values) != 1 {
		t.Errorf("Expected 1 value in list response")
	}

	for _, v := range values {
		t.Logf("Got value: %s=%s", v.Key, v.Data)
	}
}
