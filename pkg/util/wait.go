package util

import (
	"context"
	"time"
)

type ConditionWithContextFunc func(context.Context) (done bool, err error)

func PollWithContext(ctx context.Context, interval time.Duration, condition ConditionWithContextFunc) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			done, err := condition(ctx)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		}
	}
}
