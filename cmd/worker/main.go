// Command worker consumes the notification topics for the configured channels and
// delivers through the email, SMS and push providers with retries, a dead-letter queue,
// per-provider circuit breakers and per-channel rate limits.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"mcns/internal/app"
	"mcns/internal/config"
)

func main() {
	config.SetupLogging("notification-worker")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.RunWorker(ctx, app.FromEnv()); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}
