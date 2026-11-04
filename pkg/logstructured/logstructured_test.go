package logstructured

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

// fakeLog implements just the Log methods that Watch uses.
type fakeLog struct {
	Log
	events chan server.EventBatch
}

// Watch closes the channel when ctx is done, like the real broadcaster does.
func (f *fakeLog) Watch(ctx context.Context) <-chan server.EventBatch {
	go func() {
		<-ctx.Done()
		close(f.events)
	}()
	return f.events
}

func (f *fakeLog) After(ctx context.Context, key, end string, revision, limit int64) (server.EventBatch, error) {
	return server.EventBatch{CurrentRev: revision}, nil
}

func (f *fakeLog) CurrentRevision(ctx context.Context) (int64, error) {
	return 1, nil
}

// TestWatchForwarderExitsOnCancel checks that the goroutine forwarding events to the
// watch result channel stops when the context is cancelled, even if the consumer has
// stopped reading and the result channel is full.
func TestWatchForwarderExitsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fl := &fakeLog{events: make(chan server.EventBatch)}
	New(fl).Watch(ctx, 1)

	// Nobody reads wr.Eventc. Send more batches than the result buffer holds
	// (initial batch + 100 slots) so the forwarder ends up blocked on a send.
	sent := 0
	for ; sent < 150; sent++ {
		select {
		case fl.events <- server.EventBatch{CurrentRev: int64(sent + 2)}:
			continue
		case <-time.After(200 * time.Millisecond):
		}
		break
	}
	if sent == 150 {
		t.Fatal("forwarder never blocked; test did not reach the full-buffer state")
	}

	cancel()

	// Without draining wr.Eventc, the forwarder goroutine must still exit.
	deadline := time.Now().Add(5 * time.Second)
	for forwarderRunning() {
		if time.Now().After(deadline) {
			t.Fatal("forwarder goroutine still blocked after context was cancelled")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func forwarderRunning() bool {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	return strings.Contains(string(buf), "(*LogStructured).Watch.func1")
}
