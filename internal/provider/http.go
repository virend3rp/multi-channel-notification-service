package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"time"

	"mcns/internal/domain"
	"mcns/internal/messaging"
)

// HTTP talks to an SMS or push gateway with a JSON API. In development that is the
// provider-stubs service, which mimics a Twilio/FCM-style API with failure injection.
type HTTP struct {
	ProviderName string
	Endpoint     string // e.g. http://stubs:8090/sms/send
	CallbackURL  string // where the provider posts delivery receipts
	Client       *http.Client
}

var e164 = regexp.MustCompile(`^\+[1-9]\d{6,14}$`)

func (p *HTTP) Name() string { return p.ProviderName }

type sendRequest struct {
	MessageID   string `json:"messageId"`
	To          string `json:"to"`
	Title       string `json:"title,omitempty"`
	Body        string `json:"body"`
	CallbackURL string `json:"callbackUrl,omitempty"`
}

type sendResponse struct {
	ProviderMessageID string `json:"providerMessageId"`
}

func (p *HTTP) Send(ctx context.Context, env messaging.Envelope) (string, error) {
	if env.Channel == domain.ChannelSMS && !e164.MatchString(env.Recipient) {
		return "", NonRetryable("INVALID_NUMBER", "recipient is not an E.164 phone number")
	}
	body, _ := json.Marshal(sendRequest{
		MessageID: env.MessageID, To: env.Recipient, Title: env.Subject, Body: env.Body, CallbackURL: p.CallbackURL,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.Endpoint, bytes.NewReader(body))
	if err != nil {
		return "", NonRetryable("BAD_REQUEST", err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", env.MessageID)

	client := p.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err // Classify turns timeouts and connection errors into retryable ones
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if perr := ClassifyHTTP(resp.StatusCode, string(raw)); perr != nil {
		return "", perr
	}
	var out sendResponse
	_ = json.Unmarshal(raw, &out)
	return out.ProviderMessageID, nil
}
