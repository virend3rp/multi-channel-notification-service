// Package messaging defines the Kafka topic layout, the message envelope and headers
// shared by the API (producer) and the workers (consumers).
package messaging

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/virend3rp/multi-channel-notification-service/internal/domain"
)

const (
	HeaderMessageID     = "x-message-id"
	HeaderAttempt       = "x-attempt"        // attempt number this message will make (1-based)
	HeaderNotBefore     = "x-not-before"     // unix millis; retry consumers wait until then
	HeaderLastError     = "x-last-error"     // error code of the previous attempt
	HeaderOriginalTopic = "x-original-topic" // main topic, kept for DLQ replay

	DLQTopic = "notif.dlq"
)

// RetryTier is one retry topic with a fixed delay. Using a topic per delay (rather than
// one retry topic with per-message delays) keeps every topic FIFO in due-time order, so a
// consumer never waits on a 30s message while a 1s message behind it is already due.
type RetryTier struct {
	Name  string
	Delay time.Duration
}

// DefaultRetryTiers gives exponential-ish backoff of 1s, 5s, 30s: four attempts in total.
var DefaultRetryTiers = []RetryTier{
	{Name: "1s", Delay: 1 * time.Second},
	{Name: "5s", Delay: 5 * time.Second},
	{Name: "30s", Delay: 30 * time.Second},
}

// MainTopic is notif.email, notif.sms or notif.push.
func MainTopic(c domain.Channel) string { return "notif." + c.Lower() }

// RetryTopic is notif.<channel>.retry.<tier>.
func RetryTopic(c domain.Channel, t RetryTier) string {
	return fmt.Sprintf("notif.%s.retry.%s", c.Lower(), t.Name)
}

// AllTopics lists every topic the system uses, for creation at startup.
func AllTopics(tiers []RetryTier) []string {
	topics := []string{DLQTopic}
	for _, c := range domain.AllChannels {
		topics = append(topics, MainTopic(c))
		for _, t := range tiers {
			topics = append(topics, RetryTopic(c, t))
		}
	}
	return topics
}

// Envelope is the JSON value of every notification message. The content is rendered
// by the API, so workers never need template access and retries send identical content.
type Envelope struct {
	NotificationID string          `json:"notificationId"`
	MessageID      string          `json:"messageId"`
	Channel        domain.Channel  `json:"channel"`
	Recipient      string          `json:"recipient"`
	Subject        string          `json:"subject,omitempty"`
	Body           string          `json:"body"`
	Priority       domain.Priority `json:"priority"`
	CreatedAt      time.Time       `json:"createdAt"`
}

func (e Envelope) Marshal() []byte {
	b, _ := json.Marshal(e) // only plain fields; cannot fail
	return b
}

func UnmarshalEnvelope(b []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return e, fmt.Errorf("decode envelope: %w", err)
	}
	if e.NotificationID == "" || e.Channel == "" {
		return e, fmt.Errorf("envelope missing notificationId or channel")
	}
	return e, nil
}

// Message is a broker-agnostic record. The Kafka adapter converts it to kafka.Message.
type Message struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// Attempt reads the attempt header, defaulting to 1 for first delivery.
func (m Message) Attempt() int {
	if n, err := strconv.Atoi(m.Headers[HeaderAttempt]); err == nil && n > 0 {
		return n
	}
	return 1
}

// NotBefore reads the retry due time, or the zero time when absent.
func (m Message) NotBefore() time.Time {
	if ms, err := strconv.ParseInt(m.Headers[HeaderNotBefore], 10, 64); err == nil {
		return time.UnixMilli(ms)
	}
	return time.Time{}
}

// NewMainMessage builds the first-attempt message for a notification.
// The key is the recipient so all messages for one user land on one partition, in order.
func NewMainMessage(e Envelope) Message {
	return Message{
		Topic: MainTopic(e.Channel),
		Key:   []byte(e.Recipient),
		Value: e.Marshal(),
		Headers: map[string]string{
			HeaderMessageID: e.MessageID,
			HeaderAttempt:   "1",
		},
	}
}
