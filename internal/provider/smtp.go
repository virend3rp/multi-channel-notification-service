package provider

import (
	"context"
	"fmt"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"strings"
	"time"

	"mcns/internal/messaging"
)

// SMTP delivers email over plain SMTP. In development it points at Mailpit.
type SMTP struct {
	Addr    string // host:port
	From    string
	Timeout time.Duration
}

func (p *SMTP) Name() string { return "smtp" }

func (p *SMTP) Send(ctx context.Context, env messaging.Envelope) (string, error) {
	to, err := mail.ParseAddress(env.Recipient)
	if err != nil {
		return "", NonRetryable("INVALID_RECIPIENT", err.Error())
	}

	timeout := p.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	if dl, ok := ctx.Deadline(); ok && time.Until(dl) < timeout {
		timeout = time.Until(dl)
	}
	// net/smtp has no timeouts of its own, so bound the whole conversation via the conn deadline.
	conn, err := (&net.Dialer{Timeout: timeout}).DialContext(ctx, "tcp", p.Addr)
	if err != nil {
		return "", err
	}
	_ = conn.SetDeadline(time.Now().Add(timeout))
	host, _, _ := net.SplitHostPort(p.Addr)
	c, err := smtp.NewClient(conn, host)
	if err != nil {
		conn.Close()
		return "", err
	}
	defer c.Close()

	ref := fmt.Sprintf("<%s@mcns>", env.NotificationID)
	if err := c.Mail(p.From); err != nil {
		return "", err
	}
	if err := c.Rcpt(to.Address); err != nil {
		return "", err
	}
	w, err := c.Data()
	if err != nil {
		return "", err
	}
	if _, err := w.Write(buildMessage(p.From, to.Address, env, ref)); err != nil {
		return "", err
	}
	if err := w.Close(); err != nil {
		return "", err
	}
	_ = c.Quit()
	return ref, nil
}

func buildMessage(from, to string, env messaging.Envelope, messageID string) []byte {
	var b strings.Builder
	h := func(k, v string) { b.WriteString(k + ": " + v + "\r\n") }
	h("From", from)
	h("To", to)
	h("Subject", mime.QEncoding.Encode("utf-8", env.Subject))
	h("Message-ID", messageID)
	h("Date", time.Now().UTC().Format(time.RFC1123Z))
	h("X-Notification-Id", env.NotificationID)
	h("X-Idempotency-Key", env.MessageID)
	h("MIME-Version", "1.0")
	h("Content-Type", `text/html; charset="utf-8"`)
	h("Content-Transfer-Encoding", "8bit")
	b.WriteString("\r\n")
	b.WriteString(strings.ReplaceAll(env.Body, "\n", "\r\n"))
	return []byte(b.String())
}
