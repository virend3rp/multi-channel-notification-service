package worker

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/virend3rp/multi-channel-notification-service/internal/domain"
	"github.com/virend3rp/multi-channel-notification-service/internal/messaging"
	"github.com/virend3rp/multi-channel-notification-service/internal/provider"
	"github.com/virend3rp/multi-channel-notification-service/internal/resilience"
	"github.com/virend3rp/multi-channel-notification-service/internal/store"
)

// ------------------------------------------------------------------ fakes

type fakeRow struct {
	status   domain.Status
	attempts int
	lastErr  string
	ref      string
}

// memStore mirrors the SQL semantics of store.Store closely enough to exercise the
// processor's idempotency and ordering rules.
type memStore struct {
	mu        sync.Mutex
	rows      map[string]*fakeRow
	attempts  []domain.DeliveryAttempt
	dlq       []string
	failNext  map[string]int // method name → number of calls to fail
	claimErrs int
}

func newMemStore(ids ...string) *memStore {
	s := &memStore{rows: map[string]*fakeRow{}, failNext: map[string]int{}}
	for _, id := range ids {
		s.rows[id] = &fakeRow{status: domain.StatusQueued}
	}
	return s
}

func (s *memStore) injected(method string) error {
	if s.failNext[method] > 0 {
		s.failNext[method]--
		return errors.New("db unavailable")
	}
	return nil
}

func (s *memStore) Claim(_ context.Context, id string, attempt int) (bool, domain.Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.injected("Claim"); err != nil {
		return false, "", err
	}
	r, ok := s.rows[id]
	if !ok {
		return false, "", store.ErrNotFound
	}
	inFlight := r.status == domain.StatusQueued || r.status == domain.StatusRetrying || r.status == domain.StatusSending
	if (r.attempts < attempt && inFlight) || (r.attempts == attempt && r.status == domain.StatusSending) {
		r.status, r.attempts = domain.StatusSending, attempt
		return true, r.status, nil
	}
	return false, r.status, nil
}

func (s *memStore) RecordAttempt(_ context.Context, a domain.DeliveryAttempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts = append(s.attempts, a)
	return nil
}

func (s *memStore) set(id string, to domain.Status, lastErr, ref string) {
	if r := s.rows[id]; r != nil && r.status == domain.StatusSending {
		r.status = to
		if lastErr != "" {
			r.lastErr = lastErr
		}
		if ref != "" {
			r.ref = ref
		}
	}
}

func (s *memStore) MarkSent(_ context.Context, id, ref string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set(id, domain.StatusSent, "", ref)
	return nil
}

func (s *memStore) MarkRetrying(_ context.Context, id, lastErr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.injected("MarkRetrying"); err != nil {
		return err
	}
	s.set(id, domain.StatusRetrying, lastErr, "")
	return nil
}

func (s *memStore) MarkFailed(_ context.Context, id, lastErr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set(id, domain.StatusFailed, lastErr, "")
	return nil
}

func (s *memStore) DeadLetter(_ context.Context, id string, _ domain.Channel, reason string, _ []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.rows[id]; r != nil && r.status == domain.StatusSending {
		r.status, r.lastErr = domain.StatusDeadLettered, reason
		s.dlq = append(s.dlq, id)
	}
	return nil
}

func (s *memStore) row(id string) fakeRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.rows[id]
}

type memPublisher struct {
	mu   sync.Mutex
	msgs []messaging.Message
}

func (p *memPublisher) Publish(_ context.Context, msgs ...messaging.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.msgs = append(p.msgs, msgs...)
	return nil
}

func (p *memPublisher) take() []messaging.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.msgs
	p.msgs = nil
	return out
}

// scriptedProvider returns errors from script in order, then succeeds.
type scriptedProvider struct {
	mu     sync.Mutex
	script []error
	calls  int
}

func (p *scriptedProvider) Name() string { return "scripted" }

func (p *scriptedProvider) Send(context.Context, messaging.Envelope) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if len(p.script) > 0 {
		err := p.script[0]
		p.script = p.script[1:]
		return "", err
	}
	return "prov-" + strconv.Itoa(p.calls), nil
}

type noLimit struct{}

func (noLimit) Wait(context.Context, domain.Channel) error { return nil }

// ---------------------------------------------------------------- helpers

var fixedNow = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func setup(prov provider.Provider, ids ...string) (*Processor, *memStore, *memPublisher) {
	st, pub := newMemStore(ids...), &memPublisher{}
	return &Processor{
		Store: st, Publisher: pub, Limiter: noLimit{},
		Providers: map[domain.Channel]provider.Provider{domain.ChannelSMS: prov},
		Tiers:     messaging.DefaultRetryTiers,
		Now:       func() time.Time { return fixedNow },
	}, st, pub
}

func smsMessage(id string) messaging.Message {
	return messaging.NewMainMessage(messaging.Envelope{
		NotificationID: id, MessageID: "key-" + id, Channel: domain.ChannelSMS,
		Recipient: "+15550001111", Body: "code 1234", Priority: domain.PriorityNormal,
	})
}

func outage() error { return provider.Retryable("HTTP_503", "service unavailable") }

// --------------------------------------------------------------------- tests

func TestHappyPathMarksSent(t *testing.T) {
	prov := &scriptedProvider{}
	p, st, pub := setup(prov, "n1")

	if err := p.Handle(context.Background(), smsMessage("n1")); err != nil {
		t.Fatal(err)
	}
	if r := st.row("n1"); r.status != domain.StatusSent || r.ref != "prov-1" || r.attempts != 1 {
		t.Errorf("row = %+v", r)
	}
	if len(pub.take()) != 0 {
		t.Error("happy path should publish nothing")
	}
	if len(st.attempts) != 1 || st.attempts[0].Outcome != domain.OutcomeSuccess {
		t.Errorf("attempts = %+v", st.attempts)
	}
}

func TestRetryableFailureGoesToFirstRetryTier(t *testing.T) {
	p, st, pub := setup(&scriptedProvider{script: []error{outage()}}, "n1")

	if err := p.Handle(context.Background(), smsMessage("n1")); err != nil {
		t.Fatal(err)
	}
	if r := st.row("n1"); r.status != domain.StatusRetrying {
		t.Errorf("status = %s, want RETRYING", r.status)
	}
	msgs := pub.take()
	if len(msgs) != 1 {
		t.Fatalf("published %d messages, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Topic != "notif.sms.retry.1s" || m.Attempt() != 2 || string(m.Key) != "+15550001111" {
		t.Errorf("retry message: topic=%s attempt=%d key=%s", m.Topic, m.Attempt(), m.Key)
	}
	if want := fixedNow.Add(time.Second); !m.NotBefore().Equal(want) {
		t.Errorf("not-before = %v, want %v", m.NotBefore(), want)
	}
}

// Given the SMS provider fails on every attempt, the message walks 1s → 5s → 30s and
// then lands in the DLQ (DB row and DLQ topic) after MaxAttempts tries.
func TestExhaustedRetriesDeadLetter(t *testing.T) {
	prov := &scriptedProvider{script: []error{outage(), outage(), outage(), outage()}}
	p, st, pub := setup(prov, "n1")

	msg := smsMessage("n1")
	var topics []string
	for i := 0; i < p.MaxAttempts(); i++ {
		if err := p.Handle(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
		out := pub.take()
		if len(out) != 1 {
			t.Fatalf("attempt %d published %d messages", i+1, len(out))
		}
		msg = out[0]
		topics = append(topics, msg.Topic)
	}

	want := []string{"notif.sms.retry.1s", "notif.sms.retry.5s", "notif.sms.retry.30s", messaging.DLQTopic}
	for i := range want {
		if topics[i] != want[i] {
			t.Errorf("hop %d: topic %s, want %s", i, topics[i], want[i])
		}
	}
	if r := st.row("n1"); r.status != domain.StatusDeadLettered || r.attempts != 4 {
		t.Errorf("row = %+v", r)
	}
	if len(st.dlq) != 1 || prov.calls != 4 || len(st.attempts) != 4 {
		t.Errorf("dlq=%v calls=%d attempts=%d", st.dlq, prov.calls, len(st.attempts))
	}
}

func TestNonRetryableFailsImmediately(t *testing.T) {
	prov := &scriptedProvider{script: []error{provider.NonRetryable("HTTP_400", "invalid number")}}
	p, st, pub := setup(prov, "n1")

	if err := p.Handle(context.Background(), smsMessage("n1")); err != nil {
		t.Fatal(err)
	}
	if r := st.row("n1"); r.status != domain.StatusFailed {
		t.Errorf("status = %s, want FAILED", r.status)
	}
	if n := len(pub.take()); n != 0 {
		t.Errorf("published %d messages, want none", n)
	}
	if st.attempts[0].Outcome != domain.OutcomeNonRetryable {
		t.Errorf("outcome = %s", st.attempts[0].Outcome)
	}
}

// A rebalance can redeliver a message whose offset was never committed even though the
// send succeeded. The second delivery must not reach the provider.
func TestRedeliveryAfterSuccessIsSkipped(t *testing.T) {
	prov := &scriptedProvider{}
	p, st, _ := setup(prov, "n1")
	msg := smsMessage("n1")

	for i := 0; i < 3; i++ {
		if err := p.Handle(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
	}
	if prov.calls != 1 {
		t.Errorf("provider called %d times, want 1", prov.calls)
	}
	if st.row("n1").status != domain.StatusSent {
		t.Error("want SENT")
	}
}

// Duplicate copies of an older retry message must not trigger extra sends once a newer
// attempt has been scheduled.
func TestStaleRetryDuplicateIsSkipped(t *testing.T) {
	prov := &scriptedProvider{script: []error{outage(), outage()}}
	p, _, pub := setup(prov, "n1")

	_ = p.Handle(context.Background(), smsMessage("n1"))
	retry := pub.take()[0]                    // attempt 2
	_ = p.Handle(context.Background(), retry) // attempt 2 fails → attempt 3 scheduled
	_ = p.Handle(context.Background(), retry) // duplicate of attempt 2
	if prov.calls != 2 {
		t.Errorf("provider called %d times, want 2", prov.calls)
	}
}

// The worker publishes the retry before marking RETRYING. If the DB update fails (or the
// process dies) between the two, the published retry must still be processable.
func TestCrashBetweenRetryPublishAndStatusUpdate(t *testing.T) {
	prov := &scriptedProvider{script: []error{outage()}}
	p, st, pub := setup(prov, "n1")
	st.failNext["MarkRetrying"] = 1

	if err := p.Handle(context.Background(), smsMessage("n1")); err == nil {
		t.Fatal("expected error from failed status update")
	}
	if r := st.row("n1"); r.status != domain.StatusSending {
		t.Fatalf("status = %s, want SENDING (update failed)", r.status)
	}
	retry := pub.take()
	if len(retry) != 1 {
		t.Fatalf("retry not published")
	}
	if err := p.Handle(context.Background(), retry[0]); err != nil {
		t.Fatal(err)
	}
	if r := st.row("n1"); r.status != domain.StatusSent || r.attempts != 2 {
		t.Errorf("row = %+v, want SENT on attempt 2", r)
	}
}

func TestInfrastructureErrorIsReturned(t *testing.T) {
	p, st, _ := setup(&scriptedProvider{}, "n1")
	st.failNext["Claim"] = 1
	if err := p.Handle(context.Background(), smsMessage("n1")); err == nil {
		t.Fatal("claim failure must be returned so the offset is not committed")
	}
	if err := p.Handle(context.Background(), smsMessage("n1")); err != nil {
		t.Fatal(err)
	}
	if st.row("n1").status != domain.StatusSent {
		t.Error("second try should send")
	}
}

func TestPoisonMessageGoesToDLQTopic(t *testing.T) {
	p, _, pub := setup(&scriptedProvider{}, "n1")
	err := p.Handle(context.Background(), messaging.Message{Topic: "notif.sms", Value: []byte("{not json")})
	if err != nil {
		t.Fatal(err)
	}
	out := pub.take()
	if len(out) != 1 || out[0].Topic != messaging.DLQTopic {
		t.Fatalf("got %+v", out)
	}
}

// Once the breaker opens, the provider is no longer called and attempts are recorded
// as CIRCUIT_OPEN and rescheduled like any retryable failure.
func TestCircuitBreakerStopsCallingDeadProvider(t *testing.T) {
	script := make([]error, 100)
	for i := range script {
		script[i] = outage()
	}
	prov := &scriptedProvider{script: script}
	settings := resilience.DefaultBreakerSettings
	settings.ConsecutiveFailures = 3

	ids := []string{"a", "b", "c", "d", "e"}
	p, st, _ := setup(resilience.NewBreaker(prov, settings), ids...)
	for _, id := range ids {
		if err := p.Handle(context.Background(), smsMessage(id)); err != nil {
			t.Fatal(err)
		}
	}
	if prov.calls != 3 {
		t.Errorf("provider called %d times, want 3 before the breaker opened", prov.calls)
	}
	last := st.attempts[len(st.attempts)-1]
	if last.Outcome != domain.OutcomeCircuitOpen {
		t.Errorf("last outcome = %s, want CIRCUIT_OPEN", last.Outcome)
	}
	if st.row("e").status != domain.StatusRetrying {
		t.Errorf("circuit-open message should be rescheduled, got %s", st.row("e").status)
	}
}

func TestStatusTransitions(t *testing.T) {
	ok := [][2]domain.Status{
		{domain.StatusQueued, domain.StatusSending},
		{domain.StatusSending, domain.StatusSent},
		{domain.StatusSent, domain.StatusDelivered},
		{domain.StatusSending, domain.StatusRetrying},
		{domain.StatusRetrying, domain.StatusSending},
		{domain.StatusSending, domain.StatusDeadLettered},
		{domain.StatusDeadLettered, domain.StatusQueued},
	}
	for _, tr := range ok {
		if !domain.CanTransition(tr[0], tr[1]) {
			t.Errorf("%s → %s should be allowed", tr[0], tr[1])
		}
	}
	bad := [][2]domain.Status{
		{domain.StatusQueued, domain.StatusSent},
		{domain.StatusDelivered, domain.StatusQueued},
		{domain.StatusSent, domain.StatusSending},
		{domain.StatusDeadLettered, domain.StatusSending},
	}
	for _, tr := range bad {
		if domain.CanTransition(tr[0], tr[1]) {
			t.Errorf("%s → %s should be rejected", tr[0], tr[1])
		}
	}
}
