package stubs

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"mcns/internal/sig"
)

func post(t *testing.T, url, key string, body any) *http.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	req.Header.Set("Idempotency-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestFailNextThenAcceptThenDedupe(t *testing.T) {
	s := New("", nil)
	srv := httptest.NewServer(s.Routes())
	defer srv.Close()

	b, _ := json.Marshal(Behavior{FailNext: 2})
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/control/sms", bytes.NewReader(b))
	if _, err := http.DefaultClient.Do(req); err != nil {
		t.Fatal(err)
	}

	want := []int{503, 503, 202, 200}
	for i, code := range want {
		resp := post(t, srv.URL+"/sms/send", "k1", map[string]string{"to": "+15550001111", "body": "x"})
		resp.Body.Close()
		if resp.StatusCode != code {
			t.Errorf("send %d: status %d, want %d", i+1, resp.StatusCode, code)
		}
	}

	resp, _ := http.Get(srv.URL + "/messages/sms?messageId=k1")
	var got []Received
	_ = json.NewDecoder(resp.Body).Decode(&got)
	resp.Body.Close()
	if len(got) != 1 || got[0].Accepted != 1 || got[0].Rejected != 2 || got[0].Duplicates != 1 {
		t.Errorf("record = %+v", got)
	}
}

func TestCallbackIsSigned(t *testing.T) {
	var mu sync.Mutex
	var gotBody []byte
	var gotSig string
	done := make(chan struct{})
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotBody, _ = io.ReadAll(r.Body)
		gotSig = r.Header.Get("X-Signature")
		mu.Unlock()
		close(done)
	}))
	defer hook.Close()

	s := New("secret", map[string]Behavior{"push": {CallbackDelayMs: 1}})
	srv := httptest.NewServer(s.Routes())
	defer srv.Close()

	resp := post(t, srv.URL+"/push/send", "k2", map[string]string{"to": "token", "body": "x", "callbackUrl": hook.URL})
	resp.Body.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("no callback")
	}
	mu.Lock()
	defer mu.Unlock()
	if !sig.Verify("secret", gotBody, gotSig) {
		t.Errorf("bad signature %q for %s", gotSig, gotBody)
	}
}

func TestParseBehavior(t *testing.T) {
	b := ParseBehavior("latencyMs=40, failureRate=0.25,unknown=1,deliveryFailureRate=0.1")
	if b.LatencyMs != 40 || b.FailureRate != 0.25 || b.DeliveryFailureRate != 0.1 {
		t.Errorf("%+v", b)
	}
}
