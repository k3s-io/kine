package nats

import (
	"context"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

type expiredEntry struct {
	key string
	seq int64
}

func TestExpireWatcherExecutesCallbacks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := make(chan expiredEntry, 2)
	watcher := NewExpireWatcher(func(ctx context.Context, key string, seq int64) (int64, *server.KeyValue, bool, error) {
		events <- expiredEntry{key: key, seq: seq}
		return seq, nil, true, nil
	})

	go watcher.Start(ctx)

	now := time.Now()
	watcher.Add("/a", 1, now.Add(25*time.Millisecond))
	watcher.Add("/b", 2, now.Add(50*time.Millisecond))

	var got []expiredEntry
	for i := 0; i < 2; i++ {
		select {
		case evt := <-events:
			got = append(got, evt)
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for event %d", i+1)
		}
	}

	if len(got) != 2 {
		t.Fatalf("expected two events, got %d", len(got))
	}

	expEqual(t, "/a", got[0].key)
	expEqual(t, int64(1), got[0].seq)
	expEqual(t, "/b", got[1].key)
	expEqual(t, int64(2), got[1].seq)
}

func TestExpireWatcherReevaluatesOnEarlierEntry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := make(chan expiredEntry, 2)
	watcher := NewExpireWatcher(func(ctx context.Context, key string, seq int64) (int64, *server.KeyValue, bool, error) {
		events <- expiredEntry{key: key, seq: seq}
		return seq, nil, true, nil
	})

	go watcher.Start(ctx)

	now := time.Now()
	watcher.Add("late", 1, now.Add(500*time.Millisecond))
	time.Sleep(10 * time.Millisecond)
	watcher.Add("early", 2, time.Now().Add(50*time.Millisecond))

	select {
	case evt := <-events:
		expEqual(t, "early", evt.key)
		expEqual(t, int64(2), evt.seq)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected early event before timeout")
	}
}

func TestExpireWatcherRemoveKeyCancelsExpiry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan expiredEntry, 1)
	watcher := NewExpireWatcher(func(ctx context.Context, key string, seq int64) (int64, *server.KeyValue, bool, error) {
		done <- expiredEntry{key: key, seq: seq}
		return seq, nil, true, nil
	})

	go watcher.Start(ctx)

	watcher.Add("ephemeral", 1, time.Now().Add(30*time.Millisecond))
	removed := watcher.RemoveKey("ephemeral")
	if len(removed) != 1 {
		t.Fatalf("expected one removed entry, got %d", len(removed))
	}

	select {
	case <-done:
		t.Fatal("unexpected expiration after removal")
	case <-time.After(150 * time.Millisecond):
	}
}
