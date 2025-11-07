package nats

import (
	"errors"
	"testing"

	kserver "github.com/k3s-io/kine/pkg/server"
)

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func expErr(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error")
	}
}

func expEqualErr(t *testing.T, want, got error) {
	t.Helper()
	if !errors.Is(want, got) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func expEqual[T comparable](t *testing.T, want, got T) {
	t.Helper()
	if got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func expSortedKeys(t *testing.T, ents []*kserver.KeyValue) {
	t.Helper()
	var prev string
	for _, ent := range ents {
		if prev != "" {
			if prev > ent.Key {
				t.Fatalf("keys not sorted: %s > %s", prev, ent.Key)
			}
		}
		prev = ent.Key
	}
}

func expEqualKeys(t *testing.T, want []string, got []*kserver.KeyValue) {
	t.Helper()
	expEqual(t, len(want), len(got))
	for i, k := range want {
		expEqual(t, k, got[i].Key)
	}
}
