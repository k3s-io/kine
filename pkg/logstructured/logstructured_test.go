package logstructured

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/broadcaster"
	"github.com/k3s-io/kine/pkg/server"
)

// unavailableLog is a Log whose broadcaster cannot start, as happens when the
// datastore is unreachable while the first watch is created.
type unavailableLog struct {
	Log
	broadcaster broadcaster.Broadcaster
}

func (u *unavailableLog) CurrentRevision(context.Context) (int64, error) {
	return 1, nil
}

func (u *unavailableLog) Watch(ctx context.Context) <-chan server.EventBatch {
	return u.broadcaster.Watch(ctx, func() (chan server.EventBatch, error) {
		return nil, errors.New("database is unavailable")
	})
}

func TestWatchEndsWhenBroadcasterFailsToStart(t *testing.T) {
	l := New(&unavailableLog{})
	wr := l.Watch(t.Context(), 0)

	timeout := time.After(time.Second)
	for {
		select {
		case _, ok := <-wr.Eventc:
			if !ok {
				return
			}
		case <-timeout:
			t.Fatal("watch result channel was not closed after the broadcaster failed to start")
		}
	}
}
