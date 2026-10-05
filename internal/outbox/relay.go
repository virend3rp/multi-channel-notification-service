// Package outbox relays rows from the transactional outbox table to Kafka.
package outbox

import (
	"context"
	"log/slog"
	"time"

	"mcns/internal/messaging"
	"mcns/internal/metrics"
	"mcns/internal/store"
)

// Relay polls the outbox and publishes rows in seq order. It is at-least-once: a crash
// after publishing but before marking rows published re-sends them, which the workers'
// idempotent Claim absorbs. Run exactly one relay so per-recipient order is preserved.
type Relay struct {
	Store     *store.Store
	Publisher messaging.Publisher
	Batch     int
	Interval  time.Duration // idle poll interval; a full batch polls again immediately
	Retention time.Duration // how long published rows are kept for debugging
}

func (r *Relay) Run(ctx context.Context) {
	if r.Batch == 0 {
		r.Batch = 200
	}
	if r.Interval == 0 {
		r.Interval = 100 * time.Millisecond
	}
	if r.Retention == 0 {
		r.Retention = 24 * time.Hour
	}
	purge := time.NewTicker(time.Hour)
	defer purge.Stop()
	slog.Info("outbox relay started", "batch", r.Batch, "interval", r.Interval.String())

	for {
		n, err := r.relayOnce(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Error("outbox relay failed", "err", err)
		}
		wait := r.Interval
		if err != nil {
			wait = 2 * time.Second
		} else if n == r.Batch {
			wait = 0 // backlog: keep draining
		}
		select {
		case <-ctx.Done():
			return
		case <-purge.C:
			if n, err := r.Store.PurgeOutbox(ctx, time.Now().Add(-r.Retention)); err != nil {
				slog.Warn("outbox purge failed", "err", err)
			} else if n > 0 {
				slog.Info("outbox purged", "rows", n)
			}
		case <-time.After(wait):
		}
	}
}

func (r *Relay) relayOnce(ctx context.Context) (int, error) {
	rows, err := r.Store.UnpublishedOutbox(ctx, r.Batch)
	if err != nil {
		return 0, err
	}
	metrics.OutboxPending.Set(float64(len(rows)))
	if len(rows) == 0 {
		return 0, nil
	}
	msgs := make([]messaging.Message, len(rows))
	seqs := make([]int64, len(rows))
	for i, row := range rows {
		msgs[i], seqs[i] = row.Message, row.Seq
	}
	if err := r.Publisher.Publish(ctx, msgs...); err != nil {
		return 0, err
	}
	return len(rows), r.Store.MarkOutboxPublished(ctx, seqs)
}
