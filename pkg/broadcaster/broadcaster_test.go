package broadcaster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

func TestWatchConnectFailureClosesChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := &Broadcaster{}
	failing := func() (chan server.EventBatch, error) {
		return nil, errors.New("database is unavailable")
	}

	select {
	case _, ok := <-b.Watch(ctx, failing):
		if ok {
			t.Fatal("expected closed channel, got event")
		}
	case <-time.After(time.Second):
		t.Fatal("watch channel was not closed after connect failed")
	}

	// A later subscription must still be able to start the broadcaster.
	input := make(chan server.EventBatch, 1)
	working := func() (chan server.EventBatch, error) {
		return input, nil
	}
	readChan := b.Watch(ctx, working)
	input <- server.EventBatch{CurrentRev: 5}

	select {
	case batch, ok := <-readChan:
		if !ok {
			t.Fatal("expected event, got closed channel")
		}
		if batch.CurrentRev != 5 {
			t.Fatalf("expected revision 5, got %d", batch.CurrentRev)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive event after broadcaster started")
	}
}
