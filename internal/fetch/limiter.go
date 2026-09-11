package fetch

import (
	"context"
	"sync"
	"time"
)

// limiter is a token bucket; the standard library has no rate limiter and the
// behaviour needed here is small enough not to justify a dependency.
type limiter struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newLimiter(perSecond float64, burst int) *limiter {
	if burst < 1 {
		burst = 1
	}
	return &limiter{rate: perSecond, burst: float64(burst), tokens: float64(burst), last: time.Now()}
}

func (l *limiter) wait(ctx context.Context) error {
	for {
		l.mu.Lock()
		now := time.Now()
		l.tokens = min(l.burst, l.tokens+now.Sub(l.last).Seconds()*l.rate)
		l.last = now
		if l.tokens >= 1 {
			l.tokens--
			l.mu.Unlock()
			return nil
		}
		need := time.Duration((1 - l.tokens) / l.rate * float64(time.Second))
		l.mu.Unlock()

		if err := sleep(ctx, need); err != nil {
			return err
		}
	}
}
