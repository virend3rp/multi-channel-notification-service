// Package api is the HTTP layer: the public notification API, provider webhooks and
// the admin endpoints used by the Angular console.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/virend3rp/multi-channel-notification-service/internal/metrics"
	"github.com/virend3rp/multi-channel-notification-service/internal/store"
)

type Server struct {
	Store         *store.Store
	WebhookSecret string                          // HMAC key for provider callbacks; empty disables verification
	Ready         func(ctx context.Context) error // readiness probe (DB ping)
	CORSOrigin    string                          // allowed origin for the admin UI in dev, e.g. http://localhost:4200
}

func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, s.observe, middleware.Recoverer, s.cors)

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "UP"}) })
	r.Get("/readyz", s.readyz)
	r.Handle("/metrics", metrics.Handler())

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/notifications", s.sendOne)
		r.Post("/notifications/bulk", s.sendBulk)
		r.Get("/notifications/{id}", s.getNotification)
		r.Post("/webhooks/{provider}", s.webhook)

		r.Route("/admin", func(r chi.Router) {
			r.Get("/templates", s.listTemplates)
			r.Post("/templates", s.createTemplate)
			r.Post("/templates/preview", s.previewTemplate)
			r.Get("/templates/{code}", s.templateVersions)
			r.Put("/templates/{code}", s.updateTemplate)
			r.Post("/templates/{code}/versions/{version}/activate", s.activateTemplate)

			r.Get("/notifications", s.listNotifications)

			r.Get("/dlq", s.listDLQ)
			r.Post("/dlq/replay", s.replayDLQBulk)
			r.Post("/dlq/{id}/replay", s.replayDLQ)

			r.Get("/stats", s.stats)
			r.Get("/rate-limits", s.listRateLimits)
			r.Put("/rate-limits/{channel}", s.putRateLimit)
		})
	})
	return r
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if s.Ready != nil {
		if err := s.Ready(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "DOWN", "error": err.Error()})
			return
		}
	}
	writeJSON(w, 200, map[string]string{"status": "UP"})
}

// observe logs each request as structured JSON and records latency by route pattern.
// The Idempotency-Key doubles as the message id, so it is logged to correlate with workers.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)

		route := "unmatched"
		if rc := chi.RouteContext(r.Context()); rc != nil && rc.RoutePattern() != "" {
			route = rc.RoutePattern()
		}
		status := ww.Status()
		if status == 0 {
			status = 200
		}
		d := time.Since(start)
		metrics.ObserveHTTP(route, r.Method, status, d)
		if route == "/metrics" || route == "/healthz" || route == "/readyz" {
			return
		}
		attrs := []any{"method", r.Method, "route", route, "path", r.URL.Path, "status", status,
			"duration_ms", d.Milliseconds(), "request_id", middleware.GetReqID(r.Context())}
		if key := r.Header.Get("Idempotency-Key"); key != "" {
			attrs = append(attrs, "message_id", key)
		}
		slog.Info("http request", attrs...)
	})
}

func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.CORSOrigin != "" {
			w.Header().Set("Access-Control-Allow-Origin", s.CORSOrigin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Idempotency-Key")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// ------------------------------------------------------------------ helpers

type apiError struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, apiError{Error: code, Message: msg})
}

// writeStoreErr maps store sentinel errors to HTTP statuses and hides internal errors.
func writeStoreErr(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "resource not found")
	case errors.Is(err, store.ErrConflict):
		writeErr(w, http.StatusConflict, "CONFLICT", "resource was modified or is in a conflicting state")
	default:
		slog.Error("request failed", "path", r.URL.Path, "err", err, "request_id", middleware.GetReqID(r.Context()))
		writeErr(w, http.StatusInternalServerError, "INTERNAL", "internal error")
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_JSON", strings.TrimPrefix(err.Error(), "json: "))
		return false
	}
	return true
}
