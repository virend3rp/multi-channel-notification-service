package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"mcns/internal/domain"
	"mcns/internal/sig"
)

func TestRequestHashIsOrderInsensitive(t *testing.T) {
	a := requestHash(domain.ChannelSMS, "+1555", "otp", map[string]any{"a": 1, "b": "x"}, domain.PriorityNormal)
	b := requestHash(domain.ChannelSMS, "+1555", "otp", map[string]any{"b": "x", "a": 1}, domain.PriorityNormal)
	c := requestHash(domain.ChannelSMS, "+1555", "otp", map[string]any{"a": 2, "b": "x"}, domain.PriorityNormal)
	if a != b {
		t.Error("map key order changed the hash")
	}
	if a == c {
		t.Error("different data produced the same hash")
	}
}

func TestValidateRecipient(t *testing.T) {
	cases := []struct {
		ch domain.Channel
		r  string
		ok bool
	}{
		{domain.ChannelEmail, "a@b.co", true},
		{domain.ChannelEmail, "not-an-email", false},
		{domain.ChannelSMS, "+15551234567", true},
		{domain.ChannelSMS, "5551234567", false},
		{domain.ChannelPush, "fcm-token-abc", true},
	}
	for _, c := range cases {
		if err := validateRecipient(c.ch, c.r); (err == nil) != c.ok {
			t.Errorf("%s %q: err=%v want ok=%v", c.ch, c.r, err, c.ok)
		}
	}
}

func TestWebhookRejectsBadSignature(t *testing.T) {
	s := &Server{WebhookSecret: "secret"}
	h := s.Routes()
	body := `{"messageId":"m1","status":"DELIVERED"}`

	req := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/sms-stub", strings.NewReader(body))
	req.Header.Set("X-Signature", sig.Sign("wrong", []byte(body)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

func TestSendValidationHappensBeforeDB(t *testing.T) {
	h := (&Server{}).Routes() // nil store: any DB access would panic → 500
	for _, body := range []string{
		`{"recipient":"+15551234567"}`, // missing templateCode
		`{"templateCode":"otp-sms"}`,   // missing recipient
		`{"templateCode":"x","recipient":"y","priority":"URGENT"}`,
		`{"templateCode":"x","recipient":"y","unknownField":1}`,
		`not json`,
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/notifications", strings.NewReader(body)))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400 (%s)", body, rec.Code, rec.Body.String())
		}
	}
}

func TestPreviewRendersWithoutSaving(t *testing.T) {
	h := (&Server{}).Routes()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/admin/templates/preview",
		strings.NewReader(`{"channel":"SMS","body":"Code {{code}}","data":{"code":"9876"}}`)))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "Code 9876") {
		t.Errorf("status %d body %s", rec.Code, rec.Body.String())
	}
}
