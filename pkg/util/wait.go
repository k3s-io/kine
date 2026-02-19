package util

import (
	"context"
	"sync"
	"time"
)

// UntilWithContext loops until context is done, running f every period.
// If sliding is true, the period starts after f completes.
// if sliding is false, the period starts before f is called, so functions
// that take longer than period to execute may be re-run again immediately.
func UntilWithContext(ctx context.Context, interval time.Duration, f func(context.Context), sliding bool) {
	backoff := NewBackoffManager(interval)
	var t *time.Timer

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !sliding {
			t = backoff.Backoff()
		}

		f(ctx)

		if sliding {
			t = backoff.Backoff()
		}

		select {
		case <-ctx.Done():
			if !t.Stop() {
				<-t.C
			}
			return
		case <-t.C:
		}
	}
}

// BackoffManager is a simple interval manager with timer reuse, inpsired by
// k8s.io/apimachinery/util/wait.BackoffManager.
type BackoffManager interface {
	Backoff() *time.Timer
}

type backoffManager struct {
	lock  sync.Mutex
	step  time.Duration
	timer *time.Timer
}

// Backoff returns a timer with a channel that the caller may receive from
// in order to block for the configured interval.
func (b *backoffManager) Backoff() *time.Timer {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.timer == nil {
		b.timer = time.NewTimer(b.step)
	} else {
		b.timer.Reset(b.step)
	}
	return b.timer
}

// NewBackoffManager returns a simple fixed-step timer
func NewBackoffManager(step time.Duration) BackoffManager {
	return &backoffManager{step: step}
}
