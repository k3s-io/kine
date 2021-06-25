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

var (
	testClient Client
)

func getClient(t *testing.T) (Client, error) {
	e := endpoint.ETCDConfig{Endpoints: []string{"localhost:2379"}}
	if str := os.Getenv("K3S_DATASTORE_ENDPOINT"); str != "" {
		e.Endpoints = strings.Split(str, ",")
	}
	t.Logf("Connecting to etcd at %v", e.Endpoints)
	return New(e)
}

func TestGetCliient(t *testing.T) {
	c, err := getClient(t)
	if err != nil {
		t.Errorf("Unable to create new client: %v", err)
		return
	}
	testClient = c
}

func TestList(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	values, err := testClient.List(ctx, "/test", 0)
	if err != nil {
		t.Errorf("Failed to list /test: %v", err)
	}
	if len(values) != 0 {
		t.Error("Expected 0 values in list response")
	}
}

func TestCreateAndList(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	err := testClient.Create(ctx, "/test/x", []byte("test"))
	if err != nil {
		t.Errorf("Failed to create /test/x: %v", err)
	}

	values, err := testClient.List(ctx, "/test", 0)
	if err != nil {
		t.Errorf("Failed to list /test: %v", err)
	}

	if len(values) != 1 {
		t.Errorf("Expected 1 value in list response")
	}

	for _, v := range values {
		t.Logf("Got value: %s=%s", v.Key, v.Data)
	}
}
