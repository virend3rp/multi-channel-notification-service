package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/mail"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/virend3rp/multi-channel-notification-service/internal/domain"
	"github.com/virend3rp/multi-channel-notification-service/internal/messaging"
	"github.com/virend3rp/multi-channel-notification-service/internal/metrics"
	"github.com/virend3rp/multi-channel-notification-service/internal/render"
	"github.com/virend3rp/multi-channel-notification-service/internal/sig"
	"github.com/virend3rp/multi-channel-notification-service/internal/store"
)

type SendRequest struct {
	Channel        string         `json:"channel,omitempty"` // optional: defaults to the template's channel
	Recipient      string         `json:"recipient"`
	TemplateCode   string         `json:"templateCode"`
	Data           map[string]any `json:"data,omitempty"`
	Priority       string         `json:"priority,omitempty"`
	IdempotencyKey string         `json:"idempotencyKey,omitempty"` // bulk items only; single sends use the header
}

type SendResponse struct {
	ID        string        `json:"id"`
	MessageID string        `json:"messageId"`
	Status    domain.Status `json:"status"`
	Duplicate bool          `json:"duplicate,omitempty"`
}

// sendError carries the HTTP status for a rejected send.
type sendError struct {
	status int
	apiError
}

func (e *sendError) Error() string { return e.Message }

func reject(status int, code, field, format string, args ...any) *sendError {
	return &sendError{status: status, apiError: apiError{Error: code, Field: field, Message: fmt.Sprintf(format, args...)}}
}

// POST /api/v1/notifications
//
// Returns 202 for a new notification. Repeating a request with the same Idempotency-Key
// returns 200 with the original notification instead of sending again; reusing a key
// with a different body returns 422.
func (s *Server) sendOne(w http.ResponseWriter, r *http.Request) {
	var req SendRequest
	if !decode(w, r, &req) {
		return
	}
	if key := strings.TrimSpace(r.Header.Get("Idempotency-Key")); key != "" {
		req.IdempotencyKey = key
	}
	resp, err := s.accept(r.Context(), req)
	if err != nil {
		s.writeSendErr(w, r, err)
		return
	}
	status := http.StatusAccepted
	if resp.Duplicate {
		status = http.StatusOK
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.Header().Set("Location", "/api/v1/notifications/"+resp.ID)
	writeJSON(w, status, resp)
}

type BulkRequest struct {
	Notifications []SendRequest `json:"notifications"`
}

type BulkItemResult struct {
	Index int `json:"index"`
	*SendResponse
	Error *apiError `json:"error,omitempty"`
}

const maxBulk = 1000

// POST /api/v1/notifications/bulk: each item is accepted or rejected on its own,
// with its own idempotencyKey, and the response reports per-item results.
func (s *Server) sendBulk(w http.ResponseWriter, r *http.Request) {
	var req BulkRequest
	if !decode(w, r, &req) {
		return
	}
	if len(req.Notifications) == 0 || len(req.Notifications) > maxBulk {
		writeErr(w, http.StatusBadRequest, "INVALID_BATCH", fmt.Sprintf("batch must contain 1..%d notifications", maxBulk))
		return
	}
	results := make([]BulkItemResult, len(req.Notifications))
	accepted := 0
	for i, item := range req.Notifications {
		results[i].Index = i
		resp, err := s.accept(r.Context(), item)
		if err != nil {
			var se *sendError
			if !errors.As(err, &se) {
				s.writeSendErr(w, r, err) // infrastructure failure: fail the whole call
				return
			}
			results[i].Error = &se.apiError
			continue
		}
		results[i].SendResponse = &resp
		accepted++
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"accepted": accepted, "rejected": len(results) - accepted, "results": results,
	})
}

func (s *Server) writeSendErr(w http.ResponseWriter, r *http.Request, err error) {
	var se *sendError
	if errors.As(err, &se) {
		writeJSON(w, se.status, se.apiError)
		return
	}
	writeStoreErr(w, r, err)
}

// accept validates, renders and persists one notification together with its outbox row.
func (s *Server) accept(ctx context.Context, req SendRequest) (SendResponse, error) {
	req.Recipient = strings.TrimSpace(req.Recipient)
	if req.TemplateCode == "" {
		return SendResponse{}, reject(400, "VALIDATION", "templateCode", "templateCode is required")
	}
	if req.Recipient == "" {
		return SendResponse{}, reject(400, "VALIDATION", "recipient", "recipient is required")
	}
	if len(req.IdempotencyKey) > 200 {
		return SendResponse{}, reject(400, "VALIDATION", "idempotencyKey", "idempotency key longer than 200 characters")
	}
	priority, err := domain.ParsePriority(req.Priority)
	if err != nil {
		return SendResponse{}, reject(400, "VALIDATION", "priority", "%s", err.Error())
	}

	tmpl, err := s.Store.ActiveTemplate(ctx, req.TemplateCode)
	if errors.Is(err, store.ErrNotFound) {
		return SendResponse{}, reject(422, "UNKNOWN_TEMPLATE", "templateCode", "no active template %q", req.TemplateCode)
	}
	if err != nil {
		return SendResponse{}, err
	}
	channel := tmpl.Channel
	if req.Channel != "" {
		if channel, err = domain.ParseChannel(req.Channel); err != nil {
			return SendResponse{}, reject(400, "VALIDATION", "channel", "%s", err.Error())
		}
		if channel != tmpl.Channel {
			return SendResponse{}, reject(422, "CHANNEL_MISMATCH", "channel",
				"template %q is for %s, not %s", tmpl.Code, tmpl.Channel, channel)
		}
	}
	if err := validateRecipient(channel, req.Recipient); err != nil {
		return SendResponse{}, reject(400, "INVALID_RECIPIENT", "recipient", "%s", err.Error())
	}

	rendered, err := render.Render(tmpl, req.Data)
	if err != nil {
		return SendResponse{}, reject(422, "TEMPLATE_RENDER", "data", "%s", err.Error())
	}

	hash := requestHash(channel, req.Recipient, req.TemplateCode, req.Data, priority)
	messageID := req.IdempotencyKey
	if messageID != "" {
		// Fast path for retried requests: no insert attempt, no unique-violation round trip.
		if existing, err := s.Store.NotificationByMessageID(ctx, messageID); err == nil {
			return replay(existing, hash)
		} else if !errors.Is(err, store.ErrNotFound) {
			return SendResponse{}, err
		}
	} else {
		messageID = uuid.NewString()
	}

	payloadJSON, _ := json.Marshal(req.Data)
	if len(payloadJSON) > 4000 {
		return SendResponse{}, reject(400, "VALIDATION", "data", "data payload exceeds 4000 bytes")
	}
	now := time.Now().UTC()
	n := domain.Notification{
		ID: uuid.NewString(), MessageID: messageID, RequestHash: hash, Channel: channel,
		Recipient: req.Recipient, TemplateCode: tmpl.Code, TemplateVer: tmpl.Version,
		PayloadJSON: string(payloadJSON), Subject: rendered.Subject, Body: rendered.Body,
		Status: domain.StatusQueued, Priority: priority, CreatedAt: now, UpdatedAt: now,
	}
	env := messaging.Envelope{
		NotificationID: n.ID, MessageID: n.MessageID, Channel: channel, Recipient: n.Recipient,
		Subject: n.Subject, Body: n.Body, Priority: priority, CreatedAt: now,
	}

	err = s.Store.CreateNotification(ctx, n, messaging.NewMainMessage(env))
	if errors.Is(err, store.ErrDuplicate) {
		// Lost a race with a concurrent request carrying the same key.
		existing, lookupErr := s.Store.NotificationByMessageID(ctx, messageID)
		if lookupErr != nil {
			return SendResponse{}, lookupErr
		}
		return replay(existing, hash)
	}
	if err != nil {
		return SendResponse{}, err
	}
	metrics.NotificationsAccepted.WithLabelValues(string(channel), "false").Inc()
	slog.Info("notification accepted", "message_id", messageID, "notification_id", n.ID, "channel", channel)
	return SendResponse{ID: n.ID, MessageID: messageID, Status: n.Status}, nil
}

func replay(existing domain.Notification, hash string) (SendResponse, error) {
	if existing.RequestHash != hash {
		return SendResponse{}, reject(422, "IDEMPOTENCY_KEY_REUSED", "Idempotency-Key",
			"idempotency key was already used for a different request")
	}
	metrics.NotificationsAccepted.WithLabelValues(string(existing.Channel), "true").Inc()
	return SendResponse{ID: existing.ID, MessageID: existing.MessageID, Status: existing.Status, Duplicate: true}, nil
}

// requestHash fingerprints the semantic content of a request so a reused idempotency key
// with a different body can be detected. json.Marshal sorts map keys, so it is canonical.
func requestHash(c domain.Channel, recipient, template string, data map[string]any, p domain.Priority) string {
	b, _ := json.Marshal(data)
	sum := sha256.Sum256([]byte(strings.Join([]string{string(c), recipient, template, string(p), string(b)}, "\x00")))
	return hex.EncodeToString(sum[:])
}

func validateRecipient(c domain.Channel, recipient string) error {
	switch c {
	case domain.ChannelEmail:
		if _, err := mail.ParseAddress(recipient); err != nil {
			return fmt.Errorf("not a valid email address")
		}
	case domain.ChannelSMS:
		if !strings.HasPrefix(recipient, "+") || len(recipient) < 8 || len(recipient) > 16 {
			return fmt.Errorf("phone number must be in E.164 format, e.g. +15551234567")
		}
	case domain.ChannelPush:
		if len(recipient) > 320 {
			return fmt.Errorf("device token too long")
		}
	}
	return nil
}

// GET /api/v1/notifications/{id}
func (s *Server) getNotification(w http.ResponseWriter, r *http.Request) {
	n, err := s.Store.Notification(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeStoreErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, n)
}

type DeliveryReceipt struct {
	MessageID         string `json:"messageId"`
	ProviderMessageID string `json:"providerMessageId,omitempty"`
	Status            string `json:"status"` // DELIVERED or FAILED
	ErrorCode         string `json:"errorCode,omitempty"`
}

// POST /api/v1/webhooks/{provider}: delivery receipts. When a secret is configured the
// body must carry X-Signature: sha256=<hex HMAC of the raw body>.
func (s *Server) webhook(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_BODY", err.Error())
		return
	}
	if s.WebhookSecret != "" && !sig.Verify(s.WebhookSecret, raw, r.Header.Get("X-Signature")) {
		writeErr(w, http.StatusUnauthorized, "BAD_SIGNATURE", "webhook signature mismatch")
		return
	}
	var rc DeliveryReceipt
	if err := json.Unmarshal(raw, &rc); err != nil || rc.MessageID == "" {
		writeErr(w, http.StatusBadRequest, "BAD_RECEIPT", "messageId and status are required")
		return
	}
	var to domain.Status
	switch strings.ToUpper(rc.Status) {
	case "DELIVERED":
		to = domain.StatusDelivered
	case "FAILED", "UNDELIVERED", "BOUNCED":
		to = domain.StatusFailed
	default:
		writeErr(w, http.StatusBadRequest, "BAD_RECEIPT", "status must be DELIVERED or FAILED")
		return
	}
	applied, err := s.Store.ApplyDeliveryReceipt(r.Context(), rc.MessageID, to, rc.ErrorCode)
	if err != nil {
		writeStoreErr(w, r, err)
		return
	}
	slog.Info("delivery receipt", "provider", chi.URLParam(r, "provider"), "message_id", rc.MessageID,
		"status", to, "applied", applied)
	// Always 200 for a well-formed receipt: a 4xx would make the provider retry forever
	// for receipts we intentionally ignore (duplicates, or ones that arrive out of order).
	writeJSON(w, http.StatusOK, map[string]bool{"applied": applied})
}
