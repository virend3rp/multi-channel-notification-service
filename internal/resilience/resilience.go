// Package resilience wraps providers with a per-provider circuit breaker and holds the
// per-channel rate limiters whose limits are reloaded from the rate_limit_config table.
package resilience

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/sony/gobreaker/v2"
	"golang.org/x/time/rate"

	"github.com/virend3rp/multi-channel-notification-service/internal/domain"
	"github.com/virend3rp/multi-channel-notification-service/internal/messaging"
	"github.com/virend3rp/multi-channel-notification-service/internal/metrics"
	"github.com/virend3rp/multi-channel-notification-service/internal/provider"
)

// ErrCircuitOpen is returned instead of calling a provider whose breaker is open.
var ErrCircuitOpen = provider.Retryable("CIRCUIT_OPEN", "circuit breaker open; provider not called")

type BreakerSettings struct {
	// Trip after this many consecutive retryable failures...
	ConsecutiveFailures uint32
	// ...or when at least MinRequests were made in the window and this share failed.
	FailureRatio float64
	MinRequests  uint32
	Window       time.Duration // rolling window for the counts above while closed
	OpenTimeout  time.Duration // how long to stay open before letting a probe through
	HalfOpenMax  uint32        // probes allowed while half-open
}

var DefaultBreakerSettings = BreakerSettings{
	ConsecutiveFailures: 5, FailureRatio: 0.5, MinRequests: 10,
	Window: 30 * time.Second, OpenTimeout: 15 * time.Second, HalfOpenMax: 2,
}

// Breaker wraps a provider so failures stop reaching a provider that is already down.
type Breaker struct {
	provider.Provider
	cb *gobreaker.CircuitBreaker[string]
}

func NewBreaker(p provider.Provider, s BreakerSettings) *Breaker {
	cb := gobreaker.NewCircuitBreaker[string](gobreaker.Settings{
		Name:        p.Name(),
		MaxRequests: s.HalfOpenMax,
		Interval:    s.Window,
		Timeout:     s.OpenTimeout,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.ConsecutiveFailures >= s.ConsecutiveFailures ||
				(c.Requests >= s.MinRequests && float64(c.TotalFailures)/float64(c.Requests) >= s.FailureRatio)
		},
		// A 4xx (bad number, unknown device) says nothing about provider health, so it
		// is excluded from the counts instead of tripping the breaker.
		IsExcluded: func(err error) bool {
			pe := provider.Classify(err)
			return pe != nil && !pe.Retryable
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			slog.Warn("circuit breaker state change", "provider", name, "from", from.String(), "to", to.String())
			metrics.CircuitState.WithLabelValues(name).Set(float64(stateValue(to)))
		},
	})
	metrics.CircuitState.WithLabelValues(p.Name()).Set(0)
	return &Breaker{Provider: p, cb: cb}
}

func (b *Breaker) Send(ctx context.Context, env messaging.Envelope) (string, error) {
	ref, err := b.cb.Execute(func() (string, error) { return b.Provider.Send(ctx, env) })
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return "", ErrCircuitOpen
	}
	return ref, err
}

func (b *Breaker) State() gobreaker.State { return b.cb.State() }

func stateValue(s gobreaker.State) int {
	switch s {
	case gobreaker.StateHalfOpen:
		return 1
	case gobreaker.StateOpen:
		return 2
	}
	return 0
}

// RateLimiters holds one token bucket per channel. Limits can change at runtime.
type RateLimiters struct {
	mu       sync.RWMutex
	limiters map[domain.Channel]*rate.Limiter
}

func NewRateLimiters(limits []domain.RateLimit) *RateLimiters {
	r := &RateLimiters{limiters: map[domain.Channel]*rate.Limiter{}}
	r.Apply(limits)
	return r
}

// Apply updates limits in place, so goroutines already waiting on a limiter see the change.
func (r *RateLimiters) Apply(limits []domain.RateLimit) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, l := range limits {
		burst := l.Burst
		if burst < 1 {
			burst = 1
		}
		if lim, ok := r.limiters[l.Channel]; ok {
			if lim.Limit() != rate.Limit(l.PermitsPerSec) || lim.Burst() != burst {
				lim.SetLimit(rate.Limit(l.PermitsPerSec))
				lim.SetBurst(burst)
				slog.Info("rate limit updated", "channel", l.Channel, "permitsPerSec", l.PermitsPerSec, "burst", burst)
			}
			continue
		}
		r.limiters[l.Channel] = rate.NewLimiter(rate.Limit(l.PermitsPerSec), burst)
	}
}

// Wait blocks until the channel has a free permit. Channels without config are unlimited.
func (r *RateLimiters) Wait(ctx context.Context, c domain.Channel) error {
	r.mu.RLock()
	lim := r.limiters[c]
	r.mu.RUnlock()
	if lim == nil {
		return nil
	}
	return lim.Wait(ctx)
}

// Watch reloads limits from load every interval until ctx is cancelled.
func (r *RateLimiters) Watch(ctx context.Context, interval time.Duration, load func(context.Context) ([]domain.RateLimit, error)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			limits, err := load(ctx)
			if err != nil {
				slog.Warn("reload rate limits failed", "err", err)
				continue
			}
			r.Apply(limits)
		}
	}
}
