package ttl

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

var errListFailed = errors.New("list failed")

// fakeBackend implements just enough of server.Backend for Run: List fails
// listFailures times before returning an empty list, Watch records that it
// was called and returns channels the test controls.
type fakeBackend struct {
	server.Backend

	listFailures int32
	listCalls    atomic.Int32
	watchRev     atomic.Int64
	watchStarted chan struct{}
	events       chan server.EventBatch
	errs         chan error
}

func (f *fakeBackend) List(ctx context.Context, key, end string, limit, revision int64, keysOnly bool) (int64, []*server.KeyValue, error) {
	f.listCalls.Add(1)
	if f.listFailures > 0 {
		f.listFailures--
		return 0, nil, errListFailed
	}
	return 0, nil, nil
}

func (f *fakeBackend) Watch(ctx context.Context, revision int64) server.WatchResult {
	f.watchRev.Store(revision)
	close(f.watchStarted)
	return server.WatchResult{Eventc: f.events, Errorc: f.errs}
}

func newFakeBackend(listFailures int32) *fakeBackend {
	return &fakeBackend{
		listFailures: listFailures,
		watchStarted: make(chan struct{}),
		events:       make(chan server.EventBatch),
		errs:         make(chan error),
	}
}

func shortenSeedRetries(t *testing.T, retries int, interval time.Duration) {
	t.Helper()
	oldRetries, oldInterval, oldMax := seedRetries, seedRetryInterval, seedRetryMaxInterval
	seedRetries, seedRetryInterval, seedRetryMaxInterval = retries, interval, interval
	t.Cleanup(func() {
		seedRetries, seedRetryInterval, seedRetryMaxInterval = oldRetries, oldInterval, oldMax
	})
}

// TestRunRetriesFailedSeed verifies that a transient failure of the initial
// list is retried instead of permanently shutting down the TTL queue.
func TestRunRetriesFailedSeed(t *testing.T) {
	shortenSeedRetries(t, 10, time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	b := newFakeBackend(2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, b)
	}()

	select {
	case <-b.watchStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("seed was not retried; watch never started")
	}
	if calls := b.listCalls.Load(); calls != 3 {
		t.Fatalf("expected 3 list calls (1 initial + 2 retries), got %d", calls)
	}
	if rev := b.watchRev.Load(); rev != 1 {
		t.Fatalf("watch started at revision %d, want 1", rev)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

// TestRunSeedGivesUp verifies that a seed that keeps failing eventually gives
// up and shuts the queue down rather than retrying forever.
func TestRunSeedGivesUp(t *testing.T) {
	shortenSeedRetries(t, 2, time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := newFakeBackend(100)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, b)
	}()

	select {
	case <-b.watchStarted:
		t.Fatal("watch started despite seed failing")
	case <-done:
	}
	if calls := b.listCalls.Load(); calls != 3 {
		t.Fatalf("expected 3 list calls (1 initial + 2 retries), got %d", calls)
	}
}

// TestRunSeedRespectsContextCancel verifies that a pending seed retry is
// aborted when the context is canceled, rather than waiting out the backoff.
func TestRunSeedRespectsContextCancel(t *testing.T) {
	shortenSeedRetries(t, 10, time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	b := newFakeBackend(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, b)
	}()

	// wait for the first attempt to fail, then cancel during the backoff wait
	for b.listCalls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()

	select {
	case <-b.watchStarted:
		t.Fatal("watch started despite context cancel")
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return promptly after context cancel")
	}
}
