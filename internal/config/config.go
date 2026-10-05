// Package config reads settings from environment variables with development defaults.
package config

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

func String(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func Int(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		slog.Warn("ignoring invalid integer env var", "key", key, "value", v)
	}
	return def
}

func Duration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		slog.Warn("ignoring invalid duration env var", "key", key, "value", v)
	}
	return def
}

func List(key, def string) []string {
	var out []string
	for _, s := range strings.Split(String(key, def), ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func Bool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

// SetupLogging installs a JSON slog handler as the default logger, tagged with the service name.
func SetupLogging(service string) {
	level := slog.LevelInfo
	if strings.EqualFold(os.Getenv("LOG_LEVEL"), "debug") {
		level = slog.LevelDebug
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(h).With("service", service))
}
