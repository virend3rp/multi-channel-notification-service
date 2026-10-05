// Package app wires the API and worker processes from a Config. The cmd/ binaries fill
// the Config from environment variables; the end-to-end tests fill it from Testcontainers.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/virend3rp/multi-channel-notification-service/internal/api"
	"github.com/virend3rp/multi-channel-notification-service/internal/config"
	"github.com/virend3rp/multi-channel-notification-service/internal/db"
	"github.com/virend3rp/multi-channel-notification-service/internal/domain"
	"github.com/virend3rp/multi-channel-notification-service/internal/messaging"
	"github.com/virend3rp/multi-channel-notification-service/internal/metrics"
	"github.com/virend3rp/multi-channel-notification-service/internal/outbox"
	"github.com/virend3rp/multi-channel-notification-service/internal/provider"
	"github.com/virend3rp/multi-channel-notification-service/internal/resilience"
	"github.com/virend3rp/multi-channel-notification-service/internal/store"
	"github.com/virend3rp/multi-channel-notification-service/internal/worker"
)

type Config struct {
	OracleURL     string
	DBWait        time.Duration
	Brokers       []string
	Partitions    int
	Replication   int
	RetryTiers    []messaging.RetryTier
	WebhookSecret string

	// API
	HTTPAddr       string
	CORSOrigin     string
	Migrate        bool
	OutboxRelay    bool
	OutboxInterval time.Duration

	// Worker
	OpsAddr         string
	Channels        []string
	Concurrency     int
	SMTPAddr        string
	SMTPFrom        string
	StubsURL        string
	WebhookBaseURL  string
	ProviderTimeout time.Duration
	SendTimeout     time.Duration
	Breaker         resilience.BreakerSettings
	RateLimitReload time.Duration
}

// FromEnv reads the configuration shared by both binaries.
func FromEnv() Config {
	return Config{
		OracleURL:       config.String("ORACLE_URL", "oracle://app:app@localhost:1521/FREEPDB1"),
		DBWait:          config.Duration("DB_WAIT", 5*time.Minute),
		Brokers:         config.List("KAFKA_BROKERS", "localhost:9092"),
		Partitions:      config.Int("KAFKA_PARTITIONS", 6),
		Replication:     config.Int("KAFKA_REPLICATION", 1),
		RetryTiers:      messaging.DefaultRetryTiers,
		WebhookSecret:   config.String("WEBHOOK_SECRET", ""),
		HTTPAddr:        config.String("HTTP_ADDR", ":8080"),
		CORSOrigin:      config.String("CORS_ORIGIN", ""),
		Migrate:         config.Bool("MIGRATE", true),
		OutboxRelay:     config.Bool("OUTBOX_RELAY", true),
		OutboxInterval:  config.Duration("OUTBOX_INTERVAL", 100*time.Millisecond),
		OpsAddr:         config.String("OPS_ADDR", ":9091"),
		Channels:        config.List("WORKER_CHANNELS", "email,sms,push"),
		Concurrency:     config.Int("WORKER_CONCURRENCY", 3),
		SMTPAddr:        config.String("SMTP_ADDR", "localhost:1025"),
		SMTPFrom:        config.String("SMTP_FROM", "Acme Notifications <no-reply@acme.test>"),
		StubsURL:        config.String("STUBS_URL", "http://localhost:8090"),
		WebhookBaseURL:  config.String("WEBHOOK_BASE_URL", "http://localhost:8080/api/v1/webhooks"),
		ProviderTimeout: config.Duration("PROVIDER_TIMEOUT", 5*time.Second),
		SendTimeout:     config.Duration("SEND_TIMEOUT", 10*time.Second),
		Breaker:         breakerFromEnv(),
		RateLimitReload: config.Duration("RATE_LIMIT_RELOAD", 15*time.Second),
	}
}

func breakerFromEnv() resilience.BreakerSettings {
	d := resilience.DefaultBreakerSettings
	d.ConsecutiveFailures = uint32(config.Int("BREAKER_CONSECUTIVE_FAILURES", int(d.ConsecutiveFailures)))
	d.MinRequests = uint32(config.Int("BREAKER_MIN_REQUESTS", int(d.MinRequests)))
	if v := config.Int("BREAKER_FAILURE_PERCENT", 0); v > 0 {
		d.FailureRatio = float64(v) / 100
	}
	d.OpenTimeout = config.Duration("BREAKER_OPEN_TIMEOUT", d.OpenTimeout)
	return d
}

// RunAPI serves HTTP and relays the outbox until ctx is cancelled. If ready is non-nil it
// is called with the bound listener address once the server accepts connections.
func RunAPI(ctx context.Context, cfg Config, ready func(addr string)) error {
	conn, err := db.Open(ctx, cfg.OracleURL, cfg.DBWait)
	if err != nil {
		return err
	}
	defer conn.Close()
	if cfg.Migrate {
		if err := db.Migrate(ctx, conn); err != nil {
			return err
		}
	}
	if err := messaging.EnsureTopics(ctx, cfg.Brokers, messaging.AllTopics(cfg.RetryTiers), cfg.Partitions, cfg.Replication); err != nil {
		return err
	}

	st := store.New(conn)
	pub := messaging.NewKafkaPublisher(cfg.Brokers)
	defer pub.Close()

	var wg sync.WaitGroup
	if cfg.OutboxRelay {
		wg.Add(1)
		go func() {
			defer wg.Done()
			(&outbox.Relay{Store: st, Publisher: pub, Interval: cfg.OutboxInterval}).Run(ctx)
		}()
	}

	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler: (&api.Server{
			Store: st, WebhookSecret: cfg.WebhookSecret, CORSOrigin: cfg.CORSOrigin, Ready: conn.PingContext,
		}).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	slog.Info("http listening", "addr", ln.Addr().String())
	if ready != nil {
		ready(ln.Addr().String())
	}

	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	wg.Wait()
	return err
}

// RunWorker consumes and delivers until ctx is cancelled, then waits for in-flight messages.
func RunWorker(ctx context.Context, cfg Config) error {
	conn, err := db.Open(ctx, cfg.OracleURL, cfg.DBWait)
	if err != nil {
		return err
	}
	defer conn.Close()
	st := store.New(conn)

	if err := messaging.EnsureTopics(ctx, cfg.Brokers, messaging.AllTopics(cfg.RetryTiers), cfg.Partitions, cfg.Replication); err != nil {
		return err
	}
	limits, err := waitForRateLimits(ctx, st)
	if err != nil {
		return err
	}
	limiter := resilience.NewRateLimiters(limits)
	go limiter.Watch(ctx, cfg.RateLimitReload, st.RateLimits)

	pub := messaging.NewKafkaPublisher(cfg.Brokers)
	defer pub.Close()
	proc := &worker.Processor{
		Store: st, Publisher: pub, Providers: Providers(cfg), Limiter: limiter,
		Tiers: cfg.RetryTiers, SendTimeout: cfg.SendTimeout,
	}

	// One consumer group per (channel, topic). The main topic gets several readers so its
	// partitions are processed in parallel; each partition is still handled in order.
	var consumers []*worker.Consumer
	for _, name := range cfg.Channels {
		ch, err := domain.ParseChannel(name)
		if err != nil {
			return err
		}
		group := "worker." + ch.Lower()
		for i := 0; i < max(cfg.Concurrency, 1); i++ {
			consumers = append(consumers, consumer(cfg.Brokers, group+".main", messaging.MainTopic(ch), i, proc))
		}
		for _, t := range cfg.RetryTiers {
			consumers = append(consumers, consumer(cfg.Brokers, group+".retry."+t.Name, messaging.RetryTopic(ch, t), 0, proc))
		}
	}

	if cfg.OpsAddr != "" {
		go serveOps(ctx, cfg.OpsAddr)
	}

	var wg sync.WaitGroup
	for _, c := range consumers {
		wg.Add(1)
		go func(c *worker.Consumer) {
			defer wg.Done()
			_ = c.Run(ctx)
			_ = c.Reader.Close()
		}(c)
	}
	slog.Info("worker started", "consumers", len(consumers), "channels", cfg.Channels)
	<-ctx.Done()
	slog.Info("shutting down; waiting for in-flight messages")
	wg.Wait()
	return nil
}

// Providers builds every channel's provider, each behind its own circuit breaker.
func Providers(cfg Config) map[domain.Channel]provider.Provider {
	client := &http.Client{Timeout: cfg.ProviderTimeout}
	breaker := cfg.Breaker
	if breaker.ConsecutiveFailures == 0 {
		breaker = resilience.DefaultBreakerSettings
	}
	return map[domain.Channel]provider.Provider{
		domain.ChannelEmail: resilience.NewBreaker(&provider.SMTP{Addr: cfg.SMTPAddr, From: cfg.SMTPFrom}, breaker),
		domain.ChannelSMS: resilience.NewBreaker(&provider.HTTP{
			ProviderName: "sms-stub", Endpoint: cfg.StubsURL + "/sms/send",
			CallbackURL: cfg.WebhookBaseURL + "/sms-stub", Client: client,
		}, breaker),
		domain.ChannelPush: resilience.NewBreaker(&provider.HTTP{
			ProviderName: "push-stub", Endpoint: cfg.StubsURL + "/push/send",
			CallbackURL: cfg.WebhookBaseURL + "/push-stub", Client: client,
		}, breaker),
	}
}

func consumer(brokers []string, group, topic string, i int, proc *worker.Processor) *worker.Consumer {
	return &worker.Consumer{
		Name:    fmt.Sprintf("%s#%d", group, i),
		Reader:  messaging.NewReader(brokers, group, topic),
		Handler: proc.Handle,
	}
}

// waitForRateLimits blocks until the API has migrated the schema and limits can be read.
func waitForRateLimits(ctx context.Context, st *store.Store) ([]domain.RateLimit, error) {
	for {
		limits, err := st.RateLimits(ctx)
		if err == nil {
			return limits, nil
		}
		slog.Info("waiting for schema (rate_limit_config)", "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

func serveOps(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"status":"UP"}`)) })
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("ops server stopped", "err", err)
	}
}
