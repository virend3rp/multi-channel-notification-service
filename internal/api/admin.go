package api

import (
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/virend3rp/multi-channel-notification-service/internal/domain"
	"github.com/virend3rp/multi-channel-notification-service/internal/render"
	"github.com/virend3rp/multi-channel-notification-service/internal/store"
)

var templateCode = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,98}[a-z0-9]$`)

// ---------------------------------------------------------------- templates

func (s *Server) listTemplates(w http.ResponseWriter, r *http.Request) {
	ts, err := s.Store.ListTemplates(r.Context())
	if err != nil {
		writeStoreErr(w, r, err)
		return
	}
	if ts == nil {
		ts = []domain.Template{}
	}
	writeJSON(w, http.StatusOK, ts)
}

func (s *Server) templateVersions(w http.ResponseWriter, r *http.Request) {
	ts, err := s.Store.TemplateVersions(r.Context(), chi.URLParam(r, "code"))
	if err != nil {
		writeStoreErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, ts)
}

type templateRequest struct {
	Code    string `json:"code,omitempty"`
	Channel string `json:"channel,omitempty"`
	Subject string `json:"subject,omitempty"`
	Body    string `json:"body"`
}

func (s *Server) createTemplate(w http.ResponseWriter, r *http.Request) {
	var req templateRequest
	if !decode(w, r, &req) {
		return
	}
	if !templateCode.MatchString(req.Code) {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "code must be 3-100 chars of lowercase letters, digits and dashes")
		return
	}
	ch, err := domain.ParseChannel(req.Channel)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", err.Error())
		return
	}
	if !s.validTemplate(w, ch, req.Subject, req.Body) {
		return
	}
	t := domain.Template{ID: uuid.NewString(), Code: req.Code, Channel: ch, Subject: req.Subject, Body: req.Body,
		Version: 1, Active: true, CreatedAt: time.Now().UTC()}
	if err := s.Store.CreateTemplate(r.Context(), t); err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeErr(w, http.StatusConflict, "TEMPLATE_EXISTS", "a template with this code exists; PUT to add a version")
			return
		}
		writeStoreErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, t)
}

// PUT /admin/templates/{code} adds a new version and makes it active.
func (s *Server) updateTemplate(w http.ResponseWriter, r *http.Request) {
	var req templateRequest
	if !decode(w, r, &req) {
		return
	}
	code := chi.URLParam(r, "code")
	cur, err := s.Store.ActiveTemplate(r.Context(), code)
	if err != nil {
		writeStoreErr(w, r, err)
		return
	}
	if !s.validTemplate(w, cur.Channel, req.Subject, req.Body) {
		return
	}
	t, err := s.Store.AddTemplateVersion(r.Context(), uuid.NewString(), code, req.Subject, req.Body)
	if err != nil {
		writeStoreErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) activateTemplate(w http.ResponseWriter, r *http.Request) {
	version, err := strconv.Atoi(chi.URLParam(r, "version"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "version must be a number")
		return
	}
	if err := s.Store.ActivateTemplateVersion(r.Context(), chi.URLParam(r, "code"), version); err != nil {
		writeStoreErr(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) validTemplate(w http.ResponseWriter, ch domain.Channel, subject, body string) bool {
	if body == "" {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "body is required")
		return false
	}
	if ch == domain.ChannelEmail && subject == "" {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "email templates need a subject")
		return false
	}
	if err := render.Validate(subject, body); err != nil {
		writeErr(w, http.StatusUnprocessableEntity, "TEMPLATE_SYNTAX", err.Error())
		return false
	}
	return true
}

type previewRequest struct {
	Channel string         `json:"channel"`
	Subject string         `json:"subject,omitempty"`
	Body    string         `json:"body"`
	Data    map[string]any `json:"data,omitempty"`
}

// POST /admin/templates/preview renders an unsaved template, for the live editor preview.
func (s *Server) previewTemplate(w http.ResponseWriter, r *http.Request) {
	var req previewRequest
	if !decode(w, r, &req) {
		return
	}
	ch, err := domain.ParseChannel(req.Channel)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", err.Error())
		return
	}
	out, err := render.Render(domain.Template{Channel: ch, Subject: req.Subject, Body: req.Body}, req.Data)
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, "TEMPLATE_RENDER", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// ------------------------------------------------------------ delivery logs

func (s *Server) listNotifications(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.NotificationFilter{Recipient: q.Get("recipient")}
	var err error
	if v := q.Get("channel"); v != "" {
		if f.Channel, err = domain.ParseChannel(v); err != nil {
			writeErr(w, http.StatusBadRequest, "VALIDATION", err.Error())
			return
		}
	}
	if v := q.Get("status"); v != "" {
		f.Status = domain.Status(v)
	}
	for name, dst := range map[string]*time.Time{"from": &f.From, "to": &f.To} {
		if v := q.Get(name); v != "" {
			if *dst, err = time.Parse(time.RFC3339, v); err != nil {
				writeErr(w, http.StatusBadRequest, "VALIDATION", name+" must be RFC 3339")
				return
			}
		}
	}
	f.Limit, f.Offset = intParam(q.Get("limit"), 50), intParam(q.Get("offset"), 0)
	page, err := s.Store.ListNotifications(r.Context(), f)
	if err != nil {
		writeStoreErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

// ---------------------------------------------------------------------- DLQ

func (s *Server) listDLQ(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page, err := s.Store.ListDLQ(r.Context(), q.Get("includeReplayed") == "true",
		intParam(q.Get("limit"), 50), intParam(q.Get("offset"), 0))
	if err != nil {
		writeStoreErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) replayDLQ(w http.ResponseWriter, r *http.Request) {
	id, err := s.Store.ReplayDLQ(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeErr(w, http.StatusConflict, "ALREADY_REPLAYED", "message was already replayed")
			return
		}
		writeStoreErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"notificationId": id, "status": string(domain.StatusQueued)})
}

func (s *Server) replayDLQBulk(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs []string `json:"ids"`
	}
	if !decode(w, r, &req) {
		return
	}
	if len(req.IDs) == 0 || len(req.IDs) > 500 {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "ids must contain 1..500 entries")
		return
	}
	type result struct {
		ID    string `json:"id"`
		OK    bool   `json:"ok"`
		Error string `json:"error,omitempty"`
	}
	results := make([]result, 0, len(req.IDs))
	for _, id := range req.IDs {
		_, err := s.Store.ReplayDLQ(r.Context(), id)
		res := result{ID: id, OK: err == nil}
		switch {
		case errors.Is(err, store.ErrNotFound):
			res.Error = "not found"
		case errors.Is(err, store.ErrConflict):
			res.Error = "already replayed"
		case err != nil:
			writeStoreErr(w, r, err)
			return
		}
		results = append(results, res)
	}
	writeJSON(w, http.StatusAccepted, results)
}

// -------------------------------------------------------------------- stats

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	window := 24 * time.Hour
	if v := r.URL.Query().Get("window"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 || d > 90*24*time.Hour {
			writeErr(w, http.StatusBadRequest, "VALIDATION", "window must be a duration like 1h or 24h")
			return
		}
		window = d
	}
	st, err := s.Store.Stats(r.Context(), time.Now().Add(-window))
	if err != nil {
		writeStoreErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"window": window.String(), "channels": st})
}

// -------------------------------------------------------------- rate limits

func (s *Server) listRateLimits(w http.ResponseWriter, r *http.Request) {
	rl, err := s.Store.RateLimits(r.Context())
	if err != nil {
		writeStoreErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, rl)
}

// PUT /admin/rate-limits/{channel}; workers pick the change up within their reload interval.
func (s *Server) putRateLimit(w http.ResponseWriter, r *http.Request) {
	ch, err := domain.ParseChannel(chi.URLParam(r, "channel"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "VALIDATION", err.Error())
		return
	}
	var req domain.RateLimit
	if !decode(w, r, &req) {
		return
	}
	if req.PermitsPerSec <= 0 || req.Burst < 1 {
		writeErr(w, http.StatusBadRequest, "VALIDATION", "permitsPerSec must be > 0 and burst >= 1")
		return
	}
	req.Channel = ch
	if err := s.Store.SetRateLimit(r.Context(), req); err != nil {
		writeStoreErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, req)
}

func intParam(v string, def int) int {
	if n, err := strconv.Atoi(v); err == nil && n >= 0 {
		return n
	}
	return def
}
