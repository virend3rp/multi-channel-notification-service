package messaging

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// Publisher sends messages to the broker. Publish returns only after the broker has
// acknowledged every message, so callers can safely commit their own progress afterwards.
type Publisher interface {
	Publish(ctx context.Context, msgs ...Message) error
}

type KafkaPublisher struct {
	w *kafka.Writer
}

func NewKafkaPublisher(brokers []string) *KafkaPublisher {
	return &KafkaPublisher{w: &kafka.Writer{
		Addr: kafka.TCP(brokers...),
		// Murmur2 matches the Java client's partitioner, so key → partition mapping
		// is the same as any JVM producer writing to these topics.
		Balancer:     &kafka.Murmur2Balancer{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 5 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
	}}
}

func (p *KafkaPublisher) Publish(ctx context.Context, msgs ...Message) error {
	if len(msgs) == 0 {
		return nil
	}
	out := make([]kafka.Message, len(msgs))
	for i, m := range msgs {
		out[i] = toKafka(m)
	}
	return p.w.WriteMessages(ctx, out...)
}

func (p *KafkaPublisher) Close() error { return p.w.Close() }

func toKafka(m Message) kafka.Message {
	km := kafka.Message{Topic: m.Topic, Key: m.Key, Value: m.Value}
	for k, v := range m.Headers {
		km.Headers = append(km.Headers, kafka.Header{Key: k, Value: []byte(v)})
	}
	return km
}

// FromKafka converts a consumed record into a Message.
func FromKafka(km kafka.Message) Message {
	m := Message{Topic: km.Topic, Key: km.Key, Value: km.Value, Headers: make(map[string]string, len(km.Headers))}
	for _, h := range km.Headers {
		m.Headers[h.Key] = string(h.Value)
	}
	return m
}

// EnsureTopics creates any missing topics. Existing topics are left untouched.
func EnsureTopics(ctx context.Context, brokers []string, topics []string, partitions, replication int) error {
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		if lastErr = createTopics(brokers[0], topics, partitions, replication); lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	return fmt.Errorf("create topics: %w", lastErr)
}

func createTopics(broker string, topics []string, partitions, replication int) error {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	cc, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer cc.Close()
	cfgs := make([]kafka.TopicConfig, len(topics))
	for i, t := range topics {
		cfgs[i] = kafka.TopicConfig{Topic: t, NumPartitions: partitions, ReplicationFactor: replication}
	}
	return cc.CreateTopics(cfgs...)
}

// NewReader creates a consumer-group reader with manual commits: offsets are committed
// only after a message has been fully handled (sent, retried or dead-lettered).
func NewReader(brokers []string, groupID, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		MaxWait:        250 * time.Millisecond,
		CommitInterval: 0, // synchronous commits
		StartOffset:    kafka.FirstOffset,
	})
}
