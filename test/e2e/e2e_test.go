//go:build e2e

// Package e2e runs the Cucumber feature files against the whole system.
//
//	go test -tags e2e ./test/e2e/                     # in-process stack on Testcontainers (needs Docker)
//	E2E_API_URL=http://localhost:8080 \
//	E2E_STUBS_URL=http://localhost:8090 \
//	go test -tags e2e ./test/e2e/                     # against `docker compose up`
package e2e

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/cucumber/godog"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/wait"

	"mcns/internal/app"
	"mcns/internal/messaging"
	"mcns/internal/resilience"
	"mcns/internal/stubs"
)

const webhookSecret = "e2e-secret"

func TestFeatures(t *testing.T) {
	tgt := target{APIURL: os.Getenv("E2E_API_URL"), StubsURL: os.Getenv("E2E_STUBS_URL")}
	if tgt.APIURL == "" {
		tgt = startStack(t)
	}

	suite := godog.TestSuite{
		Name:                "notifications",
		ScenarioInitializer: func(sc *godog.ScenarioContext) { register(sc, tgt) },
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"features"},
			Tags:     os.Getenv("E2E_TAGS"),
			Strict:   true,
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("feature tests failed")
	}
}

// startStack boots Oracle and Kafka on Testcontainers and runs the API, worker and
// provider stubs in-process against them. Retry delays are shortened so the DLQ
// scenarios finish in seconds, and the circuit breaker is relaxed because these
// scenarios fail the provider on purpose (the breaker has its own unit test).
func startStack(t *testing.T) target {
	t.Helper()
	ctx := context.Background()

	oracle, err := testcontainers.Run(ctx, "gvenzl/oracle-free:23-slim-faststart",
		testcontainers.WithEnv(map[string]string{
			"ORACLE_PASSWORD": "oracle", "APP_USER": "app", "APP_USER_PASSWORD": "app",
		}),
		testcontainers.WithExposedPorts("1521/tcp"),
		testcontainers.WithWaitStrategyAndDeadline(6*time.Minute, wait.ForLog("DATABASE IS READY TO USE!")),
	)
	testcontainers.CleanupContainer(t, oracle)
	if err != nil {
		t.Fatalf("start oracle: %v", err)
	}
	oracleAddr, err := oracle.PortEndpoint(ctx, "1521/tcp", "")
	if err != nil {
		t.Fatal(err)
	}

	kafka, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0", tckafka.WithClusterID("e2e"))
	testcontainers.CleanupContainer(t, kafka)
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	brokers, err := kafka.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}

	stubSrv := httptest.NewServer(stubs.New(webhookSecret, map[string]stubs.Behavior{
		"sms": {LatencyMs: 5}, "push": {LatencyMs: 5},
	}).Routes())
	t.Cleanup(stubSrv.Close)

	cfg := app.Config{
		OracleURL:      fmt.Sprintf("oracle://app:app@%s/FREEPDB1", oracleAddr),
		DBWait:         2 * time.Minute,
		Brokers:        brokers,
		Partitions:     3,
		Replication:    1,
		WebhookSecret:  webhookSecret,
		HTTPAddr:       "127.0.0.1:0",
		Migrate:        true,
		OutboxRelay:    true,
		OutboxInterval: 50 * time.Millisecond,
		RetryTiers: []messaging.RetryTier{
			{Name: "1s", Delay: 300 * time.Millisecond},
			{Name: "5s", Delay: 600 * time.Millisecond},
			{Name: "30s", Delay: time.Second},
		},
		Channels:        []string{"email", "sms", "push"},
		Concurrency:     2,
		SMTPAddr:        "127.0.0.1:1", // email is not exercised by the features
		StubsURL:        stubSrv.URL,
		ProviderTimeout: 5 * time.Second,
		SendTimeout:     10 * time.Second,
		RateLimitReload: time.Second,
		Breaker: resilience.BreakerSettings{
			ConsecutiveFailures: 1000, FailureRatio: 1, MinRequests: 1000,
			Window: time.Minute, OpenTimeout: time.Second, HalfOpenMax: 1,
		},
	}

	runCtx, cancel := context.WithCancel(ctx)
	apiErr, workerErr := make(chan error, 1), make(chan error, 1)
	addrCh := make(chan string, 1)
	go func() { apiErr <- app.RunAPI(runCtx, cfg, func(addr string) { addrCh <- addr }) }()

	var apiURL string
	select {
	case addr := <-addrCh:
		apiURL = "http://" + addr
	case err := <-apiErr:
		cancel()
		t.Fatalf("api failed to start: %v", err)
	case <-time.After(3 * time.Minute):
		cancel()
		t.Fatal("api did not start in time")
	}

	wcfg := cfg
	wcfg.WebhookBaseURL = apiURL + "/api/v1/webhooks"
	go func() { workerErr <- app.RunWorker(runCtx, wcfg) }()

	t.Cleanup(func() {
		cancel()
		for _, ch := range []chan error{apiErr, workerErr} {
			select {
			case err := <-ch:
				if err != nil {
					t.Logf("shutdown: %v", err)
				}
			case <-time.After(20 * time.Second):
				t.Log("shutdown timed out")
			}
		}
	})

	// Consumer groups take a few seconds to join; give them a head start so the first
	// scenario's timing reflects delivery rather than group rebalancing.
	time.Sleep(5 * time.Second)
	return target{APIURL: apiURL, StubsURL: stubSrv.URL}
}
