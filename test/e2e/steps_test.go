//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cucumber/godog"
	"github.com/google/uuid"
)

// target is the system under test: either the Docker Compose stack or the in-process
// stack started on Testcontainers by TestMain.
type target struct {
	APIURL   string
	StubsURL string
}

type scenario struct {
	t      target
	client *http.Client

	idemKey     string
	lastReq     sendRequest
	lastStatus  int
	lastBody    map[string]any
	responseIDs []string

	notificationID string
	messageID      string
}

type sendRequest struct {
	TemplateCode string         `json:"templateCode"`
	Recipient    string         `json:"recipient"`
	Data         map[string]any `json:"data"`
	idemKey      string
}

func register(sc *godog.ScenarioContext, t target) {
	s := &scenario{t: t, client: &http.Client{Timeout: 15 * time.Second}}

	sc.Step(`^the provider stubs are reset$`, s.resetStubs)
	sc.Step(`^the "(sms|push)" provider fails the next (\d+) requests?$`, s.providerFailsNext)
	sc.Step(`^the "(sms|push)" provider rejects every request$`, s.providerRejectsAll)
	sc.Step(`^a new idempotency key$`, s.newIdempotencyKey)

	sc.Step(`^I send an? "([^"]+)" notification to "([^"]+)" with data:$`, s.sendWithData)
	sc.Step(`^I send an? "([^"]+)" notification to "([^"]+)" with the idempotency key and data:$`, s.sendWithKeyAndData)
	sc.Step(`^I send the same request again$`, s.sendAgain)
	sc.Step(`^I replay the notification from the DLQ$`, s.replayFromDLQ)

	sc.Step(`^the response status is (\d+)$`, s.responseStatusIs)
	sc.Step(`^the error code is "([^"]+)"$`, s.errorCodeIs)
	sc.Step(`^both responses refer to the same notification$`, s.bothResponsesSame)
	sc.Step(`^within (\d+) seconds the notification status is "([A-Z_]+)"$`, s.eventuallyStatus)
	sc.Step(`^the notification has (\d+) delivery attempts?$`, s.attemptCount)
	sc.Step(`^attempt (\d+) has outcome "([A-Z_]+)" and error code "([^"]+)"$`, s.attemptOutcome)
	sc.Step(`^the "(sms|push)" provider received the body "([^"]*)"$`, s.providerReceivedBody)
	sc.Step(`^the "(sms|push)" provider accepted exactly (\d+) messages? for the notification$`, s.providerAccepted)
	sc.Step(`^the DLQ contains the notification$`, s.dlqContains)
	sc.Step(`^the DLQ entry is marked as replayed$`, s.dlqReplayed)
}

// ------------------------------------------------------------------- HTTP

func (s *scenario) do(method, url string, body any, headers map[string]string) (int, []byte, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, b, err
}

func (s *scenario) getJSON(url string, out any) error {
	status, b, err := s.do(http.MethodGet, url, nil, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("GET %s: status %d: %s", url, status, b)
	}
	return json.Unmarshal(b, out)
}

// ------------------------------------------------------------------ stubs

func (s *scenario) resetStubs() error {
	status, b, err := s.do(http.MethodPost, s.t.StubsURL+"/control/reset", nil, nil)
	if err == nil && status != http.StatusNoContent {
		err = fmt.Errorf("reset stubs: %d %s", status, b)
	}
	return err
}

func (s *scenario) setBehavior(channel string, behavior map[string]any) error {
	status, b, err := s.do(http.MethodPut, s.t.StubsURL+"/control/"+channel, behavior, nil)
	if err == nil && status != http.StatusOK {
		err = fmt.Errorf("set behavior: %d %s", status, b)
	}
	return err
}

func (s *scenario) providerFailsNext(channel string, n int) error {
	return s.setBehavior(channel, map[string]any{"failNext": n, "callbackDelayMs": 100})
}

func (s *scenario) providerRejectsAll(channel string) error {
	return s.setBehavior(channel, map[string]any{"rejectRate": 1})
}

// -------------------------------------------------------------- sending

func (s *scenario) newIdempotencyKey() error {
	s.idemKey = "e2e-" + uuid.NewString()
	return nil
}

func tableData(tbl *godog.Table) map[string]any {
	data := map[string]any{}
	for _, row := range tbl.Rows {
		if len(row.Cells) >= 2 {
			data[row.Cells[0].Value] = row.Cells[1].Value
		}
	}
	return data
}

func (s *scenario) sendWithData(template, recipient string, tbl *godog.Table) error {
	return s.send(sendRequest{TemplateCode: template, Recipient: recipient, Data: tableData(tbl)})
}

func (s *scenario) sendWithKeyAndData(template, recipient string, tbl *godog.Table) error {
	return s.send(sendRequest{TemplateCode: template, Recipient: recipient, Data: tableData(tbl), idemKey: s.idemKey})
}

func (s *scenario) sendAgain() error { return s.send(s.lastReq) }

func (s *scenario) send(req sendRequest) error {
	headers := map[string]string{}
	if req.idemKey != "" {
		headers["Idempotency-Key"] = req.idemKey
	}
	status, b, err := s.do(http.MethodPost, s.t.APIURL+"/api/v1/notifications", req, headers)
	if err != nil {
		return err
	}
	s.lastReq, s.lastStatus, s.lastBody = req, status, map[string]any{}
	_ = json.Unmarshal(b, &s.lastBody)
	if id, ok := s.lastBody["id"].(string); ok {
		s.responseIDs = append(s.responseIDs, id)
		if s.notificationID == "" {
			s.notificationID = id
			s.messageID, _ = s.lastBody["messageId"].(string)
		}
	}
	return nil
}

func (s *scenario) responseStatusIs(want int) error {
	if s.lastStatus != want {
		return fmt.Errorf("response status %d, want %d (body %v)", s.lastStatus, want, s.lastBody)
	}
	return nil
}

func (s *scenario) errorCodeIs(want string) error {
	if got := s.lastBody["error"]; got != want {
		return fmt.Errorf("error code %v, want %s (body %v)", got, want, s.lastBody)
	}
	return nil
}

func (s *scenario) bothResponsesSame() error {
	if len(s.responseIDs) != 2 || s.responseIDs[0] != s.responseIDs[1] {
		return fmt.Errorf("response ids %v, want two identical ids", s.responseIDs)
	}
	return nil
}

// --------------------------------------------------------- notification

type notification struct {
	ID        string `json:"id"`
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	History   []struct {
		AttemptNo int    `json:"attemptNo"`
		Outcome   string `json:"outcome"`
		ErrorCode string `json:"errorCode"`
	} `json:"attemptsHistory"`
}

func (s *scenario) fetch() (notification, error) {
	var n notification
	if s.notificationID == "" {
		return n, fmt.Errorf("no notification was created in this scenario (last response %d %v)", s.lastStatus, s.lastBody)
	}
	err := s.getJSON(s.t.APIURL+"/api/v1/notifications/"+s.notificationID, &n)
	return n, err
}

func (s *scenario) eventuallyStatus(seconds int, want string) error {
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	var last notification
	for time.Now().Before(deadline) {
		n, err := s.fetch()
		if err != nil {
			return err
		}
		if n.Status == want {
			return nil
		}
		last = n
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("status still %q after %ds, want %q (attempts: %+v)", last.Status, seconds, want, last.History)
}

func (s *scenario) attemptCount(want int) error {
	n, err := s.fetch()
	if err != nil {
		return err
	}
	if len(n.History) != want {
		return fmt.Errorf("%d delivery attempts, want %d: %+v", len(n.History), want, n.History)
	}
	return nil
}

func (s *scenario) attemptOutcome(no int, outcome, code string) error {
	n, err := s.fetch()
	if err != nil {
		return err
	}
	for _, a := range n.History {
		if a.AttemptNo == no {
			if a.Outcome != outcome || a.ErrorCode != code {
				return fmt.Errorf("attempt %d: outcome %s code %s, want %s %s", no, a.Outcome, a.ErrorCode, outcome, code)
			}
			return nil
		}
	}
	return fmt.Errorf("no attempt %d in %+v", no, n.History)
}

// ------------------------------------------------------------ provider

type received struct {
	Body     string `json:"body"`
	Accepted int    `json:"accepted"`
}

func (s *scenario) received(channel string) (received, error) {
	var out []received
	if err := s.getJSON(s.t.StubsURL+"/messages/"+channel+"?messageId="+s.messageID, &out); err != nil {
		return received{}, err
	}
	if len(out) != 1 {
		return received{}, fmt.Errorf("provider has %d records for message %s", len(out), s.messageID)
	}
	return out[0], nil
}

func (s *scenario) providerReceivedBody(channel, body string) error {
	r, err := s.received(channel)
	if err != nil {
		return err
	}
	if r.Body != body {
		return fmt.Errorf("provider got body %q, want %q", r.Body, body)
	}
	return nil
}

func (s *scenario) providerAccepted(channel string, want int) error {
	r, err := s.received(channel)
	if err != nil {
		return err
	}
	if r.Accepted != want {
		return fmt.Errorf("provider accepted %d sends, want %d", r.Accepted, want)
	}
	return nil
}

// ----------------------------------------------------------------- DLQ

type dlqPage struct {
	Items []struct {
		ID             string  `json:"id"`
		NotificationID string  `json:"notificationId"`
		ReplayedAt     *string `json:"replayedAt"`
	} `json:"items"`
}

func (s *scenario) dlqEntry(includeReplayed bool) (string, *string, error) {
	var page dlqPage
	url := fmt.Sprintf("%s/api/v1/admin/dlq?limit=500&includeReplayed=%t", s.t.APIURL, includeReplayed)
	if err := s.getJSON(url, &page); err != nil {
		return "", nil, err
	}
	for _, it := range page.Items {
		if it.NotificationID == s.notificationID {
			return it.ID, it.ReplayedAt, nil
		}
	}
	return "", nil, fmt.Errorf("notification %s not in DLQ", s.notificationID)
}

func (s *scenario) dlqContains() error {
	_, _, err := s.dlqEntry(false)
	return err
}

func (s *scenario) replayFromDLQ() error {
	id, _, err := s.dlqEntry(false)
	if err != nil {
		return err
	}
	status, b, err := s.do(http.MethodPost, s.t.APIURL+"/api/v1/admin/dlq/"+id+"/replay", nil, nil)
	if err == nil && status != http.StatusAccepted {
		err = fmt.Errorf("replay: %d %s", status, b)
	}
	return err
}

func (s *scenario) dlqReplayed() error {
	_, replayedAt, err := s.dlqEntry(true)
	if err != nil {
		return err
	}
	if replayedAt == nil {
		return fmt.Errorf("DLQ entry has no replayedAt")
	}
	return nil
}
