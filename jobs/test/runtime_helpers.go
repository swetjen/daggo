package jobs

import (
	"context"
	crand "crypto/rand"
	"math/big"
	"time"
)

func sleepOrCancel(ctx context.Context, delay time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
		return nil
	}
}

func randomMinuteDuration(minMinutes, maxMinutes int) time.Duration {
	if minMinutes <= 0 {
		minMinutes = 1
	}
	if maxMinutes < minMinutes {
		maxMinutes = minMinutes
	}
	span := int64(maxMinutes - minMinutes + 1)
	pick, err := crand.Int(crand.Reader, big.NewInt(span))
	if err != nil {
		return time.Duration(minMinutes) * time.Minute
	}
	return time.Duration(minMinutes+int(pick.Int64())) * time.Minute
}
