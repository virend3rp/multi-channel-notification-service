// Package render turns a stored Mustache template plus a request payload into the
// final subject and body that get queued for delivery.
package render

import (
	"fmt"

	"github.com/cbroglie/mustache"

	"github.com/virend3rp/multi-channel-notification-service/internal/domain"
)

func init() {
	// A missing variable is a client error (400), not an empty string in a customer's inbox.
	mustache.AllowMissingVariables = false
}

type Rendered struct {
	Subject string `json:"subject,omitempty"`
	Body    string `json:"body"`
}

// Render applies payload to the template. Email bodies are HTML-escaped because they are
// sent as HTML; SMS and push are plain text, so escaping would show "&amp;" to users.
func Render(t domain.Template, payload map[string]any) (Rendered, error) {
	raw := t.Channel != domain.ChannelEmail

	body, err := renderOne(t.Body, payload, raw)
	if err != nil {
		return Rendered{}, fmt.Errorf("body: %w", err)
	}
	var subject string
	if t.Subject != "" {
		// Subjects are header text, never HTML.
		if subject, err = renderOne(t.Subject, payload, true); err != nil {
			return Rendered{}, fmt.Errorf("subject: %w", err)
		}
	}
	return Rendered{Subject: subject, Body: body}, nil
}

// Validate parses a template without rendering it, for the admin create/edit endpoints.
func Validate(subject, body string) error {
	if _, err := mustache.ParseString(body); err != nil {
		return fmt.Errorf("body: %w", err)
	}
	if subject != "" {
		if _, err := mustache.ParseString(subject); err != nil {
			return fmt.Errorf("subject: %w", err)
		}
	}
	return nil
}

func renderOne(src string, payload map[string]any, raw bool) (string, error) {
	tmpl, err := mustache.ParseStringRaw(src, raw)
	if err != nil {
		return "", err
	}
	if payload == nil {
		payload = map[string]any{}
	}
	return tmpl.Render(payload)
}
