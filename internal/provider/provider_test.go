package provider

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"testing"
	"time"

	"mcns/internal/domain"
	"mcns/internal/messaging"
)

func TestClassifyHTTP(t *testing.T) {
	cases := []struct {
		status    int
		retryable bool
		isErr     bool
	}{
		{200, false, false}, {202, false, false},
		{400, false, true}, {401, false, true}, {404, false, true}, {422, false, true},
		{408, true, true}, {429, true, true},
		{500, true, true}, {502, true, true}, {503, true, true},
	}
	for _, c := range cases {
		got := ClassifyHTTP(c.status, "")
		if (got != nil) != c.isErr {
			t.Errorf("%d: error = %v, want error %v", c.status, got, c.isErr)
			continue
		}
		if got != nil && got.Retryable != c.retryable {
			t.Errorf("%d: retryable = %v, want %v", c.status, got.Retryable, c.retryable)
		}
	}
}

func TestClassifyGenericErrors(t *testing.T) {
	if e := Classify(context.DeadlineExceeded); !e.Retryable || e.Code != "TIMEOUT" {
		t.Errorf("deadline: %+v", e)
	}
	if e := Classify(&textproto.Error{Code: 550, Msg: "no such user"}); e.Retryable {
		t.Errorf("SMTP 550 should be permanent: %+v", e)
	}
	if e := Classify(&textproto.Error{Code: 451, Msg: "try later"}); !e.Retryable {
		t.Errorf("SMTP 451 should be retryable: %+v", e)
	}
	wrapped := fmt.Errorf("send: %w", NonRetryable("INVALID_NUMBER", "x"))
	if e := Classify(wrapped); e.Retryable || e.Code != "INVALID_NUMBER" {
		t.Errorf("wrapped classified error lost: %+v", e)
	}
	if e := Classify(errors.New("mystery")); !e.Retryable {
		t.Errorf("unknown errors default to retryable: %+v", e)
	}
}

func TestHTTPProviderSendsIdempotencyKey(t *testing.T) {
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprint(w, `{"providerMessageId":"prov-1"}`)
	}))
	defer srv.Close()

	p := &HTTP{ProviderName: "sms", Endpoint: srv.URL}
	ref, err := p.Send(context.Background(), messaging.Envelope{
		NotificationID: "n1", MessageID: "key-1", Channel: domain.ChannelSMS, Recipient: "+15550001111", Body: "hi",
	})
	if err != nil {
		t.Fatal(err)
	}
	if ref != "prov-1" || gotKey != "key-1" {
		t.Errorf("ref=%q key=%q", ref, gotKey)
	}
}

func TestHTTPProviderClassifiesFailures(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()
	p := &HTTP{ProviderName: "push", Endpoint: srv.URL, Client: &http.Client{Timeout: 50 * time.Millisecond}}
	_, err := p.Send(context.Background(), messaging.Envelope{Channel: domain.ChannelPush, Recipient: "device-token", Body: "x"})
	if e := Classify(err); !e.Retryable || e.Code != "TIMEOUT" {
		t.Errorf("timeout classified as %+v", e)
	}

	_, err = p.Send(context.Background(), messaging.Envelope{Channel: domain.ChannelSMS, Recipient: "not-a-number", Body: "x"})
	if e := Classify(err); e.Retryable || e.Code != "INVALID_NUMBER" {
		t.Errorf("invalid number classified as %+v", e)
	}
}
