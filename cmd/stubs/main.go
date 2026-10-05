// Command stubs runs fake SMS and push providers with configurable latency and failure
// injection. Change behavior at runtime with PUT /control/{sms|push}.
package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"mcns/internal/config"
	"mcns/internal/stubs"
)

func main() {
	config.SetupLogging("provider-stubs")
	srv := stubs.New(config.String("WEBHOOK_SECRET", ""), map[string]stubs.Behavior{
		"sms":  stubs.ParseBehavior(config.String("SMS_BEHAVIOR", "latencyMs=40,jitterMs=60")),
		"push": stubs.ParseBehavior(config.String("PUSH_BEHAVIOR", "latencyMs=20,jitterMs=30")),
	})
	addr := config.String("HTTP_ADDR", ":8090")
	slog.Info("provider stubs listening", "addr", addr)
	hs := &http.Server{Addr: addr, Handler: srv.Routes(), ReadHeaderTimeout: 5 * time.Second}
	if err := hs.ListenAndServe(); err != nil {
		slog.Error("stopped", "err", err)
		os.Exit(1)
	}
}
