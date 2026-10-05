// Command api serves the notification REST API and the admin endpoints, applies database
// migrations, and runs the outbox relay that publishes accepted notifications to Kafka.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/virend3rp/multi-channel-notification-service/internal/app"
	"github.com/virend3rp/multi-channel-notification-service/internal/config"
)

func main() {
	config.SetupLogging("notification-api")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.RunAPI(ctx, app.FromEnv(), nil); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}
