package util

import (
	"context"
	"time"
)

type FuncToExecute func(context.Context) (bool, error)

func PollWithContext(ctx context.Context, interval time.Duration, f FuncToExecute) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f(ctx)
		}
	}
}
