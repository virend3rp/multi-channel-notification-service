// Package provider contains the outbound channel adapters (SMTP, SMS, push) and the
// error classification that decides whether a failed send is worth retrying.
package provider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/textproto"

	"mcns/internal/messaging"
)

// Provider sends one rendered notification and returns the provider's own message reference.
// Implementations must pass env.MessageID to the provider as an idempotency key when the
// provider supports one, so a re-sent message after a worker crash is deduplicated upstream.
type Provider interface {
	Name() string
	Send(ctx context.Context, env messaging.Envelope) (providerRef string, err error)
}

// Error is a classified provider failure.
type Error struct {
	Code      string // e.g. HTTP_503, SMTP_550, TIMEOUT, INVALID_RECIPIENT
	Retryable bool
	Msg       string
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Msg) }

func Retryable(code, msg string) *Error    { return &Error{Code: code, Retryable: true, Msg: msg} }
func NonRetryable(code, msg string) *Error { return &Error{Code: code, Retryable: false, Msg: msg} }

// Classify maps any send error onto *Error. Timeouts and connection failures are
// transient; anything already classified keeps its classification; unknown errors are
// treated as retryable because dropping a message is worse than retrying it a few times.
func Classify(err error) *Error {
	if err == nil {
		return nil
	}
	var pe *Error
	if errors.As(err, &pe) {
		return pe
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Retryable("TIMEOUT", err.Error())
	}
	var ne net.Error
	if errors.As(err, &ne) {
		if ne.Timeout() {
			return Retryable("TIMEOUT", err.Error())
		}
		return Retryable("NETWORK", err.Error())
	}
	var oe *net.OpError
	if errors.As(err, &oe) {
		return Retryable("NETWORK", err.Error())
	}
	var tpe *textproto.Error
	if errors.As(err, &tpe) {
		return ClassifySMTP(tpe.Code, tpe.Msg)
	}
	return Retryable("UNKNOWN", err.Error())
}

// ClassifyHTTP maps a provider HTTP status. 408, 425, 429 and 5xx are transient;
// other 4xx mean the request itself is wrong and retrying cannot help.
func ClassifyHTTP(status int, body string) *Error {
	code := fmt.Sprintf("HTTP_%d", status)
	switch {
	case status == 408 || status == 425 || status == 429 || status >= 500:
		return Retryable(code, body)
	case status >= 400:
		return NonRetryable(code, body)
	}
	return nil
}

// ClassifySMTP maps an SMTP reply code: 4xx are transient (greylisting, mailbox busy),
// 5xx are permanent (no such user, rejected).
func ClassifySMTP(code int, msg string) *Error {
	c := fmt.Sprintf("SMTP_%d", code)
	if code >= 400 && code < 500 {
		return Retryable(c, msg)
	}
	return NonRetryable(c, msg)
}
