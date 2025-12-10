package broadcaster

import (
	"context"
	"sync"

	"github.com/k3s-io/kine/pkg/server"
)

type ConnectFunc func() (chan server.Events, error)

type Broadcaster struct {
	sync.Mutex
	running bool
	subs    map[chan server.Events]struct{}
}

func (b *Broadcaster) Subscribe(ctx context.Context, connect ConnectFunc) (<-chan server.Events, error) {
	b.Lock()
	defer b.Unlock()

	if !b.running {
		if err := b.start(connect); err != nil {
			return nil, err
		}
	}

	sub := make(chan server.Events, 100)
	if b.subs == nil {
		b.subs = map[chan server.Events]struct{}{}
	}
	b.subs[sub] = struct{}{}
	go func() {
		<-ctx.Done()
		b.unsub(sub, true)
	}()

	return sub, nil
}

func (b *Broadcaster) unsub(sub chan server.Events, lock bool) {
	if lock {
		b.Lock()
	}
	if _, ok := b.subs[sub]; ok {
		close(sub)
		delete(b.subs, sub)
	}
	if lock {
		b.Unlock()
	}
}

func (b *Broadcaster) start(connect ConnectFunc) error {
	c, err := connect()
	if err != nil {
		return err
	}

	go b.stream(c)
	b.running = true
	return nil
}

func (b *Broadcaster) stream(input chan server.Events) {
	for item := range input {
		b.Lock()
		for sub := range b.subs {
			select {
			case sub <- item:
			default:
				// Slow consumer, drop
				go b.unsub(sub, true)
			}
		}
		b.Unlock()
	}

	b.Lock()
	for sub := range b.subs {
		b.unsub(sub, false)
	}
	b.running = false
	b.Unlock()
}
