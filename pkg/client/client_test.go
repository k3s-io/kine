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

func TestGetClient(t *testing.T) {
	c, err := getClient(t)
	if err != nil {
		t.Fatalf("Unable to create new client: %v", err)
		return
	}
	testClient = c
}

func TestListAll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	values, err := testClient.List(ctx, "", 0)
	if err != nil {
		t.Fatalf("Failed to list '': %v", err)
	}
	if len(values) != 0 {
		t.Fatalf("Expected 0 values in list response")
	}
}

func TestListPrefix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	values, err := testClient.List(ctx, "/test", 0)
	if err != nil {
		t.Fatalf("Failed to list '/test': %v", err)
	}
	if len(values) != 0 {
		t.Fatal("Expected 0 values in list response")
	}
}

func TestCreateAndListPrefix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	err := testClient.Create(ctx, "/test/x", []byte("test"))
	if err != nil {
		t.Fatalf("Failed to create '/test/x': %v", err)
	}

	values, err := testClient.List(ctx, "/test", 0)
	if err != nil {
		t.Fatalf("Failed to list '/test': %v", err)
	}

	if len(values) != 1 {
		t.Fatal("Expected 1 value in list response")
	}

	for _, v := range values {
		t.Logf("Got value: %s=%s", v.Key, v.Data)
	}
}

func TestDeleteAndListPrefix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	err := testClient.Delete(ctx, "/test/x")
	if err != nil {
		t.Fatalf("Failed to delete '/test/x': %v", err)
	}

	values, err := testClient.List(ctx, "/test", 0)
	if err != nil {
		t.Fatalf("Failed to list '/test': %v", err)
	}

	if len(values) != 1 {
		t.Fatal("Expected 0 values in list response")
	}

	for _, v := range values {
		t.Logf("Got value: %s=%s", v.Key, v.Data)
	}
}
