// Package domain holds the core notification types and the status state machine.
package domain

import (
	"fmt"
	"strings"
	"time"
)

type Channel string

const (
	ChannelEmail Channel = "EMAIL"
	ChannelSMS   Channel = "SMS"
	ChannelPush  Channel = "PUSH"
)

var AllChannels = []Channel{ChannelEmail, ChannelSMS, ChannelPush}

func ParseChannel(s string) (Channel, error) {
	c := Channel(strings.ToUpper(strings.TrimSpace(s)))
	switch c {
	case ChannelEmail, ChannelSMS, ChannelPush:
		return c, nil
	}
	return "", fmt.Errorf("unknown channel %q", s)
}

// Lower is the lowercase form used in topic names and URLs.
func (c Channel) Lower() string { return strings.ToLower(string(c)) }

type Status string

const (
	StatusQueued       Status = "QUEUED"
	StatusSending      Status = "SENDING"
	StatusRetrying     Status = "RETRYING"
	StatusSent         Status = "SENT"
	StatusDelivered    Status = "DELIVERED"
	StatusFailed       Status = "FAILED"
	StatusDeadLettered Status = "DEAD_LETTERED"
)

// transitions is the status lifecycle:
//
//	QUEUED → SENDING → SENT → DELIVERED
//	            │  ↑      └──→ FAILED        (provider callback reports a bounce)
//	            ↓  │
//	         RETRYING ──→ DEAD_LETTERED      (retries exhausted)
//	SENDING → FAILED                         (non-retryable error)
//	DEAD_LETTERED / FAILED → QUEUED          (manual replay from the admin console)
var transitions = map[Status][]Status{
	StatusQueued:       {StatusSending},
	StatusSending:      {StatusSent, StatusRetrying, StatusFailed, StatusDeadLettered},
	StatusRetrying:     {StatusSending},
	StatusSent:         {StatusDelivered, StatusFailed},
	StatusDelivered:    {},
	StatusFailed:       {StatusQueued},
	StatusDeadLettered: {StatusQueued},
}

// CanTransition reports whether a notification may move from one status to another.
func CanTransition(from, to Status) bool {
	for _, s := range transitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Sendable reports whether a worker may still attempt delivery in this status.
// Anything past SENT has already reached the provider, so redelivered Kafka
// messages for it are dropped instead of sent twice.
func (s Status) Sendable() bool {
	return s == StatusQueued || s == StatusRetrying || s == StatusSending
}

type Priority string

const (
	PriorityHigh   Priority = "HIGH"
	PriorityNormal Priority = "NORMAL"
	PriorityLow    Priority = "LOW"
)

func ParsePriority(s string) (Priority, error) {
	if s == "" {
		return PriorityNormal, nil
	}
	p := Priority(strings.ToUpper(s))
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return p, nil
	}
	return "", fmt.Errorf("unknown priority %q", s)
}

type Template struct {
	ID        string    `json:"id"`
	Code      string    `json:"code"`
	Channel   Channel   `json:"channel"`
	Subject   string    `json:"subject,omitempty"`
	Body      string    `json:"body"`
	Version   int       `json:"version"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"createdAt"`
}

type Notification struct {
	ID           string            `json:"id"`
	MessageID    string            `json:"messageId"`
	RequestHash  string            `json:"-"`
	Channel      Channel           `json:"channel"`
	Recipient    string            `json:"recipient"`
	TemplateCode string            `json:"templateCode"`
	TemplateVer  int               `json:"templateVersion"`
	PayloadJSON  string            `json:"-"`
	Payload      map[string]any    `json:"payload,omitempty"`
	Subject      string            `json:"subject,omitempty"`
	Body         string            `json:"body"`
	Status       Status            `json:"status"`
	Priority     Priority          `json:"priority"`
	Attempts     int               `json:"attempts"`
	LastError    string            `json:"lastError,omitempty"`
	ProviderRef  string            `json:"providerRef,omitempty"`
	CreatedAt    time.Time         `json:"createdAt"`
	UpdatedAt    time.Time         `json:"updatedAt"`
	History      []DeliveryAttempt `json:"attemptsHistory,omitempty"`
}

type Outcome string

const (
	OutcomeSuccess      Outcome = "SUCCESS"
	OutcomeRetryable    Outcome = "RETRYABLE_ERROR"
	OutcomeNonRetryable Outcome = "PERMANENT_ERROR"
	OutcomeCircuitOpen  Outcome = "CIRCUIT_OPEN"
)

type DeliveryAttempt struct {
	ID             string    `json:"id"`
	NotificationID string    `json:"notificationId"`
	AttemptNo      int       `json:"attemptNo"`
	Provider       string    `json:"provider"`
	Outcome        Outcome   `json:"outcome"`
	ErrorCode      string    `json:"errorCode,omitempty"`
	ErrorMessage   string    `json:"errorMessage,omitempty"`
	LatencyMs      int64     `json:"latencyMs"`
	AttemptedAt    time.Time `json:"attemptedAt"`
}

type DLQMessage struct {
	ID             string     `json:"id"`
	NotificationID string     `json:"notificationId"`
	Channel        Channel    `json:"channel"`
	Reason         string     `json:"reason"`
	Payload        string     `json:"payload"`
	CreatedAt      time.Time  `json:"createdAt"`
	ReplayedAt     *time.Time `json:"replayedAt,omitempty"`
}

type RateLimit struct {
	Channel       Channel `json:"channel"`
	PermitsPerSec float64 `json:"permitsPerSec"`
	Burst         int     `json:"burst"`
}

// ChannelStats is one row of the admin dashboard.
type ChannelStats struct {
	Channel      Channel          `json:"channel"`
	ByStatus     map[Status]int64 `json:"byStatus"`
	Attempts     int64            `json:"attempts"`
	FailedTries  int64            `json:"failedAttempts"`
	P95LatencyMs float64          `json:"p95LatencyMs"`
	AvgLatencyMs float64          `json:"avgLatencyMs"`
	DLQPending   int64            `json:"dlqPending"`
}
