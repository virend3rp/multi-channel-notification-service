// Package stubs is a fake SMS/push gateway with failure injection. It mimics the shape
// of a Twilio/FCM-style API: accepts a send, returns a provider message id, and later
// posts a signed delivery receipt to the callback URL.
package stubs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/virend3rp/multi-channel-notification-service/internal/sig"
)

// Behavior controls one channel's simulated provider.
type Behavior struct {
	LatencyMs           int     `json:"latencyMs"`
	JitterMs            int     `json:"jitterMs"`
	FailureRate         float64 `json:"failureRate"`         // share of sends answered with 503
	RejectRate          float64 `json:"rejectRate"`          // share answered with 400 (permanent)
	FailNext            int     `json:"failNext"`            // force the next N sends to 503
	DeliveryFailureRate float64 `json:"deliveryFailureRate"` // share of receipts reporting FAILED
	CallbackDelayMs     int     `json:"callbackDelayMs"`
	Down                bool    `json:"down"` // every send gets 503 (simulated outage)
}

// Received is one message as the provider saw it.
type Received struct {
	MessageID         string    `json:"messageId"`
	ProviderMessageID string    `json:"providerMessageId"`
	To                string    `json:"to"`
	Title             string    `json:"title,omitempty"`
	Body              string    `json:"body"`
	Accepted          int       `json:"accepted"`   // accepted sends (1 unless the client ignored idempotency)
	Duplicates        int       `json:"duplicates"` // repeat sends deduplicated by Idempotency-Key
	Rejected          int       `json:"rejected"`   // sends answered with an injected error
	FirstSeen         time.Time `json:"firstSeen"`
}

type channelState struct {
	behavior Behavior
	messages map[string]*Received
}

type Server struct {
	WebhookSecret string
	Client        *http.Client

	mu       sync.Mutex
	channels map[string]*channelState
	defaults map[string]Behavior
}

func New(webhookSecret string, defaults map[string]Behavior) *Server {
	s := &Server{WebhookSecret: webhookSecret, Client: &http.Client{Timeout: 5 * time.Second}, defaults: defaults}
	s.reset()
	return s
}

func (s *Server) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.channels = map[string]*channelState{}
	for _, ch := range []string{"sms", "push"} {
		s.channels[ch] = &channelState{behavior: s.defaults[ch], messages: map[string]*Received{}}
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /{channel}/send", s.send)
	mux.HandleFunc("GET /control/{channel}", s.getControl)
	mux.HandleFunc("PUT /control/{channel}", s.putControl)
	mux.HandleFunc("POST /control/reset", func(w http.ResponseWriter, _ *http.Request) {
		s.reset()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /messages/{channel}", s.listMessages)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"status":"UP"}`)) })
	return mux
}

type sendRequest struct {
	MessageID   string `json:"messageId"`
	To          string `json:"to"`
	Title       string `json:"title"`
	Body        string `json:"body"`
	CallbackURL string `json:"callbackUrl"`
}

func (s *Server) send(w http.ResponseWriter, r *http.Request) {
	ch := r.PathValue("channel")
	var req sendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.To == "" {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		key = req.MessageID
	}

	s.mu.Lock()
	state, ok := s.channels[ch]
	if !ok {
		s.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	b := state.behavior
	rec := state.messages[key]
	if rec == nil {
		rec = &Received{MessageID: key, To: req.To, Title: req.Title, Body: req.Body, FirstSeen: time.Now().UTC()}
		state.messages[key] = rec
	}
	// Decide the outcome while holding the lock so FailNext counts down exactly.
	status := http.StatusAccepted
	switch {
	case b.Down:
		status = http.StatusServiceUnavailable
	case state.behavior.FailNext > 0:
		state.behavior.FailNext--
		status = http.StatusServiceUnavailable
	case rand.Float64() < b.RejectRate:
		status = http.StatusBadRequest
	case rand.Float64() < b.FailureRate:
		status = http.StatusServiceUnavailable
	}
	duplicate := status == http.StatusAccepted && rec.Accepted > 0
	switch {
	case status != http.StatusAccepted:
		rec.Rejected++
	case duplicate:
		rec.Duplicates++
	default:
		rec.Accepted++
		rec.ProviderMessageID = ch + "-" + uuid.NewString()[:8]
	}
	providerID := rec.ProviderMessageID
	s.mu.Unlock()

	if d := b.LatencyMs + jitter(b.JitterMs); d > 0 {
		time.Sleep(time.Duration(d) * time.Millisecond)
	}

	w.Header().Set("Content-Type", "application/json")
	if status != http.StatusAccepted {
		w.WriteHeader(status)
		fmt.Fprintf(w, `{"error":"injected failure","status":%d}`, status)
		return
	}
	if duplicate {
		// Same Idempotency-Key: return the original result and do not deliver again.
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"providerMessageId": providerID, "status": "duplicate"})
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"providerMessageId": providerID, "status": "queued"})

	if req.CallbackURL != "" {
		go s.callback(req.CallbackURL, key, providerID, b)
	}
}

func (s *Server) callback(url, messageID, providerID string, b Behavior) {
	delay := b.CallbackDelayMs
	if delay == 0 {
		delay = 300 + rand.IntN(700)
	}
	time.Sleep(time.Duration(delay) * time.Millisecond)

	receipt := map[string]string{"messageId": messageID, "providerMessageId": providerID, "status": "DELIVERED"}
	if rand.Float64() < b.DeliveryFailureRate {
		receipt["status"], receipt["errorCode"] = "FAILED", "UNREACHABLE_HANDSET"
	}
	body, _ := json.Marshal(receipt)
	for attempt := 1; attempt <= 3; attempt++ {
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if s.WebhookSecret != "" {
			req.Header.Set("X-Signature", sig.Sign(s.WebhookSecret, body))
		}
		resp, err := s.Client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 300 {
				return
			}
			err = fmt.Errorf("status %d", resp.StatusCode)
		}
		slog.Warn("delivery receipt failed", "message_id", messageID, "attempt", attempt, "err", err)
		time.Sleep(time.Duration(attempt) * time.Second)
	}
}

func (s *Server) getControl(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	state, ok := s.channels[r.PathValue("channel")]
	var b Behavior
	if ok {
		b = state.behavior
	}
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, b)
}

// PUT /control/{channel} replaces the channel's behavior (send the full object).
func (s *Server) putControl(w http.ResponseWriter, r *http.Request) {
	var b Behavior
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	state, ok := s.channels[r.PathValue("channel")]
	if ok {
		state.behavior = b
	}
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	slog.Info("behavior updated", "channel", r.PathValue("channel"), "behavior", b)
	writeJSON(w, b)
}

func (s *Server) listMessages(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	state, ok := s.channels[r.PathValue("channel")]
	var out []Received
	if ok {
		filter := r.URL.Query().Get("messageId")
		for _, m := range state.messages {
			if filter == "" || m.MessageID == filter {
				out = append(out, *m)
			}
		}
	}
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FirstSeen.Before(out[j].FirstSeen) })
	if out == nil {
		out = []Received{}
	}
	writeJSON(w, out)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func jitter(max int) int {
	if max <= 0 {
		return 0
	}
	return rand.IntN(max)
}

// ParseBehavior reads "latencyMs=50,failureRate=0.05" style settings from env strings.
func ParseBehavior(spec string) Behavior {
	var b Behavior
	for _, kv := range strings.Split(spec, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(kv), "=")
		if !ok {
			continue
		}
		switch k {
		case "latencyMs":
			fmt.Sscan(v, &b.LatencyMs)
		case "jitterMs":
			fmt.Sscan(v, &b.JitterMs)
		case "failureRate":
			fmt.Sscan(v, &b.FailureRate)
		case "rejectRate":
			fmt.Sscan(v, &b.RejectRate)
		case "deliveryFailureRate":
			fmt.Sscan(v, &b.DeliveryFailureRate)
		case "callbackDelayMs":
			fmt.Sscan(v, &b.CallbackDelayMs)
		}
	}
	return b
}
