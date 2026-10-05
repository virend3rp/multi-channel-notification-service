package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/virend3rp/multi-channel-notification-service/internal/messaging"
	"github.com/virend3rp/multi-channel-notification-service/internal/metrics"
)

// Consumer drives one kafka.Reader. Messages from a partition are handled one at a time
// and committed only after Handle succeeds, which gives at-least-once delivery while
// preserving per-recipient order (the recipient is the partition key).
type Consumer struct {
	Name    string
	Reader  *kafka.Reader
	Handler func(context.Context, messaging.Message) error
}

func (c *Consumer) Run(ctx context.Context) error {
	log := slog.With("consumer", c.Name, "topic", c.Reader.Config().Topic)
	go c.reportLag(ctx)
	log.Info("consumer started")
	for {
		km, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Error("fetch failed", "err", err)
			if !sleep(ctx, time.Second) {
				return nil
			}
			continue
		}
		msg := messaging.FromKafka(km)

		// Retry topics hold messages in due-time order (one fixed delay per topic), so
		// waiting here for the head message never delays one that is already due.
		if due := msg.NotBefore(); !due.IsZero() {
			if !sleep(ctx, time.Until(due)) {
				return nil
			}
		}

		backoff := 500 * time.Millisecond
		for {
			err := c.Handler(ctx, msg)
			if err == nil {
				break
			}
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return nil // uncommitted; the next owner of this partition redelivers it
			}
			log.Error("handler failed; retrying same message", "err", err, "offset", km.Offset,
				"message_id", msg.Headers[messaging.HeaderMessageID], "backoff", backoff.String())
			if !sleep(ctx, backoff) {
				return nil
			}
			backoff = min(backoff*2, 30*time.Second)
		}

		if err := c.Reader.CommitMessages(ctx, km); err != nil && ctx.Err() == nil {
			// Not fatal: the message is redelivered and the idempotent Claim drops it.
			log.Warn("commit failed", "err", err, "offset", km.Offset)
		}
	}
}

func (c *Consumer) reportLag(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	topic := c.Reader.Config().Topic
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			metrics.ConsumerLag.WithLabelValues(topic, c.Name).Set(float64(c.Reader.Stats().Lag))
		}
	}
}

// sleep waits d or until ctx is done; it reports whether the full duration elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
