// Package worker consumes notification topics, sends through providers and routes
// failures to retry topics or the dead-letter queue.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"

	"mcns/internal/domain"
	"mcns/internal/messaging"
	"mcns/internal/metrics"
	"mcns/internal/provider"
	"mcns/internal/store"
)

// Store is the slice of persistence the worker needs; *store.Store implements it.
type Store interface {
	Claim(ctx context.Context, id string, attempt int) (bool, domain.Status, error)
	RecordAttempt(ctx context.Context, a domain.DeliveryAttempt) error
	MarkSent(ctx context.Context, id, providerRef string) error
	MarkRetrying(ctx context.Context, id, lastError string) error
	MarkFailed(ctx context.Context, id, lastError string) error
	DeadLetter(ctx context.Context, id string, channel domain.Channel, reason string, payload []byte) error
}

// Limiter blocks until a channel may send again.
type Limiter interface {
	Wait(ctx context.Context, c domain.Channel) error
}

type Processor struct {
	Store       Store
	Publisher   messaging.Publisher
	Providers   map[domain.Channel]provider.Provider
	Limiter     Limiter
	Tiers       []messaging.RetryTier
	SendTimeout time.Duration
	Now         func() time.Time
}

// MaxAttempts is the first try plus one per retry tier.
func (p *Processor) MaxAttempts() int { return len(p.Tiers) + 1 }

func (p *Processor) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

// Handle processes one message. A nil return means the message is finished (sent,
// rescheduled, failed or dead-lettered) and its offset may be committed. A non-nil error
// means infrastructure trouble (database or broker unavailable); the caller must not
// commit and should retry the same message, which is safe because Claim is re-entrant.
func (p *Processor) Handle(ctx context.Context, msg messaging.Message) error {
	env, err := messaging.UnmarshalEnvelope(msg.Value)
	if err != nil {
		// A poison message can never succeed; park it on the DLQ topic and move on.
		slog.Error("undecodable message sent to DLQ topic", "topic", msg.Topic, "err", err)
		return p.Publisher.Publish(ctx, p.dlqMessage(msg, "DECODE_ERROR: "+err.Error()))
	}
	attempt := msg.Attempt()
	log := slog.With("message_id", env.MessageID, "notification_id", env.NotificationID,
		"channel", env.Channel, "attempt", attempt)
	ch := string(env.Channel)

	claimed, status, err := p.Store.Claim(ctx, env.NotificationID, attempt)
	if errors.Is(err, store.ErrNotFound) {
		log.Error("message references unknown notification; dropping")
		return nil
	}
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	if !claimed {
		metrics.DuplicatesSkipped.WithLabelValues(ch).Inc()
		log.Info("skipping duplicate delivery", "current_status", status)
		return nil
	}

	prov, ok := p.Providers[env.Channel]
	if !ok {
		return p.fail(ctx, log, env, attempt, "none", provider.NonRetryable("NO_PROVIDER", "no provider configured for channel"), 0)
	}

	if err := p.Limiter.Wait(ctx, env.Channel); err != nil {
		return err // shutting down; message will be redelivered and re-claimed
	}

	timeout := p.SendTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	sendCtx, cancel := context.WithTimeout(ctx, timeout)
	start := time.Now()
	ref, sendErr := prov.Send(sendCtx, env)
	latency := time.Since(start)
	cancel()
	if ctx.Err() != nil {
		return ctx.Err() // interrupted by shutdown, not a provider verdict
	}
	metrics.ProviderLatency.WithLabelValues(ch, prov.Name()).Observe(latency.Seconds())

	if sendErr != nil {
		return p.fail(ctx, log, env, attempt, prov.Name(), provider.Classify(sendErr), latency)
	}

	if err := p.Store.RecordAttempt(ctx, p.attempt(env, attempt, prov.Name(), domain.OutcomeSuccess, nil, latency)); err != nil {
		return fmt.Errorf("record attempt: %w", err)
	}
	if err := p.Store.MarkSent(ctx, env.NotificationID, ref); err != nil {
		return fmt.Errorf("mark sent: %w", err)
	}
	metrics.DeliveryAttempts.WithLabelValues(ch, string(domain.OutcomeSuccess)).Inc()
	metrics.NotificationsSent.WithLabelValues(ch, string(domain.StatusSent)).Inc()
	if !env.CreatedAt.IsZero() {
		metrics.EndToEndLatency.WithLabelValues(ch).Observe(p.now().Sub(env.CreatedAt).Seconds())
	}
	log.Info("sent", "provider", prov.Name(), "provider_ref", ref, "latency_ms", latency.Milliseconds())
	return nil
}

func (p *Processor) fail(ctx context.Context, log *slog.Logger, env messaging.Envelope, attempt int,
	provName string, perr *provider.Error, latency time.Duration) error {
	ch := string(env.Channel)
	outcome := domain.OutcomeRetryable
	switch {
	case perr.Code == "CIRCUIT_OPEN":
		outcome = domain.OutcomeCircuitOpen
	case !perr.Retryable:
		outcome = domain.OutcomeNonRetryable
	}
	if err := p.Store.RecordAttempt(ctx, p.attempt(env, attempt, provName, outcome, perr, latency)); err != nil {
		return fmt.Errorf("record attempt: %w", err)
	}
	metrics.DeliveryAttempts.WithLabelValues(ch, string(outcome)).Inc()
	log = log.With("error_code", perr.Code, "error", perr.Msg)

	if !perr.Retryable {
		if err := p.Store.MarkFailed(ctx, env.NotificationID, perr.Error()); err != nil {
			return fmt.Errorf("mark failed: %w", err)
		}
		metrics.NotificationsSent.WithLabelValues(ch, string(domain.StatusFailed)).Inc()
		log.Warn("permanent failure; not retrying")
		return nil
	}

	if attempt >= p.MaxAttempts() {
		reason := fmt.Sprintf("retries exhausted after %d attempts; last error %s", attempt, perr.Error())
		// The dlq_message row is the source of truth for replay; the DLQ topic is a feed for
		// alerting and audit consumers. Write the row first so a crash cannot lose it.
		if err := p.Store.DeadLetter(ctx, env.NotificationID, env.Channel, reason, env.Marshal()); err != nil {
			return fmt.Errorf("dead-letter: %w", err)
		}
		dlq := p.dlqMessage(messaging.NewMainMessage(env), reason)
		dlq.Headers[messaging.HeaderAttempt] = strconv.Itoa(attempt)
		if err := p.Publisher.Publish(ctx, dlq); err != nil {
			return fmt.Errorf("publish dlq: %w", err)
		}
		metrics.NotificationsSent.WithLabelValues(ch, string(domain.StatusDeadLettered)).Inc()
		log.Error("dead-lettered")
		return nil
	}

	// Publish the retry before updating the row: if we crash in between, the retry message
	// still exists and Claim accepts it, whereas the reverse order could strand the
	// notification in RETRYING with nothing left in Kafka to pick it up.
	tier := p.Tiers[attempt-1]
	retry := messaging.Message{
		Topic: messaging.RetryTopic(env.Channel, tier),
		Key:   []byte(env.Recipient),
		Value: env.Marshal(),
		Headers: map[string]string{
			messaging.HeaderMessageID:     env.MessageID,
			messaging.HeaderAttempt:       strconv.Itoa(attempt + 1),
			messaging.HeaderNotBefore:     strconv.FormatInt(p.now().Add(tier.Delay).UnixMilli(), 10),
			messaging.HeaderLastError:     perr.Code,
			messaging.HeaderOriginalTopic: messaging.MainTopic(env.Channel),
		},
	}
	if err := p.Publisher.Publish(ctx, retry); err != nil {
		return fmt.Errorf("publish retry: %w", err)
	}
	if err := p.Store.MarkRetrying(ctx, env.NotificationID, perr.Error()); err != nil {
		return fmt.Errorf("mark retrying: %w", err)
	}
	metrics.Retries.WithLabelValues(ch, tier.Name).Inc()
	log.Warn("scheduled retry", "tier", tier.Name, "next_attempt", attempt+1)
	return nil
}

func (p *Processor) attempt(env messaging.Envelope, n int, prov string, outcome domain.Outcome,
	perr *provider.Error, latency time.Duration) domain.DeliveryAttempt {
	a := domain.DeliveryAttempt{
		ID: uuid.NewString(), NotificationID: env.NotificationID, AttemptNo: n, Provider: prov,
		Outcome: outcome, LatencyMs: latency.Milliseconds(), AttemptedAt: p.now().UTC(),
	}
	if perr != nil {
		a.ErrorCode, a.ErrorMessage = perr.Code, perr.Msg
	}
	return a
}

func (p *Processor) dlqMessage(m messaging.Message, reason string) messaging.Message {
	h := map[string]string{messaging.HeaderLastError: reason, messaging.HeaderOriginalTopic: m.Topic}
	for k, v := range m.Headers {
		if _, set := h[k]; !set {
			h[k] = v
		}
	}
	return messaging.Message{Topic: messaging.DLQTopic, Key: m.Key, Value: m.Value, Headers: h}
}
