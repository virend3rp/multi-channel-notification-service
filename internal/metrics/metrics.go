// Package metrics declares the Prometheus series exported by the API and the workers.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	NotificationsAccepted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notifications_accepted_total",
		Help: "Notifications accepted by the API, by channel and whether the request was an idempotent replay.",
	}, []string{"channel", "duplicate"})

	NotificationsSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notifications_sent_total",
		Help: "Terminal outcomes reached by workers, by channel and status (SENT, FAILED, DEAD_LETTERED).",
	}, []string{"channel", "status"})

	DeliveryAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notification_attempts_total",
		Help: "Provider send attempts by channel and outcome.",
	}, []string{"channel", "outcome"})

	Retries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notification_retries_total",
		Help: "Messages routed to a retry topic, by channel and tier.",
	}, []string{"channel", "tier"})

	DuplicatesSkipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notification_duplicates_skipped_total",
		Help: "Kafka deliveries skipped because the notification was already handled (consumer idempotency).",
	}, []string{"channel"})

	ProviderLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "provider_send_duration_seconds",
		Help:    "Provider send latency.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"channel", "provider"})

	EndToEndLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "notification_end_to_end_seconds",
		Help:    "Time from API acceptance to successful provider hand-off.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
	}, []string{"channel"})

	CircuitState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "circuit_breaker_state",
		Help: "Circuit breaker state per provider: 0 closed, 1 half-open, 2 open.",
	}, []string{"provider"})

	ConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag",
		Help: "Messages behind the partition high-water mark, per topic and consumer.",
	}, []string{"topic", "consumer"})

	OutboxPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_pending",
		Help: "Rows read from the outbox in the last relay poll (a sustained high value means Kafka is lagging).",
	})

	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency by route pattern, method and status.",
		Buckets: prometheus.DefBuckets,
	}, []string{"route", "method", "status"})
)

func Handler() http.Handler { return promhttp.Handler() }

// ObserveHTTP records one request; route must be the pattern, not the raw path, to keep cardinality bounded.
func ObserveHTTP(route, method string, status int, d time.Duration) {
	httpDuration.WithLabelValues(route, method, strconv.Itoa(status)).Observe(d.Seconds())
}
