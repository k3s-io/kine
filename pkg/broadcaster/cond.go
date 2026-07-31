package broadcaster

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Cond is a wrapper around sync.Cond that facilitates
// waiting for a broadcaster to signal that is has seen
// a specific revision.
type Cond struct {
	cond     *sync.Cond
	revision atomic.Int64
}

// NewCond createds a new Cond instance, using sync.Mutex as the lock primitive.
func NewCond() *Cond {
	return &Cond{cond: sync.NewCond(&sync.Mutex{})}
}

// Broadcast stores revision, and broadcasts
func (c *Cond) Broadcast(revision int64) {
	c.cond.L.Lock()
	c.revision.Store(revision)
	c.cond.Broadcast()
	c.cond.L.Unlock()
}

// Wait blocks until Broadcast is called, and revision is greater than or equal the specified revision.
// Context cancellation will break out of continued waiting on the channel, but may continue
// to block if Broadcast is not called again after the context is cancelled.
func (c *Cond) Wait(ctx context.Context, revision int64) {
	c.cond.L.Lock()
	for ctx.Err() == nil && c.revision.Load() < revision {
		c.cond.Wait()
	}
	c.cond.L.Unlock()
	select {
	case <-ctx.Done():
	case <-time.After(time.Millisecond):
	}
}
