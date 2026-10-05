package render

import (
	"strings"
	"testing"

	"mcns/internal/domain"
)

func TestRenderEmailEscapesHTML(t *testing.T) {
	tmpl := domain.Template{Channel: domain.ChannelEmail, Subject: "Hi {{name}}", Body: "<p>{{name}}</p>"}
	got, err := Render(tmpl, map[string]any{"name": "Tom & <Jerry>"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Body != "<p>Tom &amp; &lt;Jerry&gt;</p>" {
		t.Errorf("body = %q", got.Body)
	}
	if got.Subject != "Hi Tom & <Jerry>" {
		t.Errorf("subject should not be escaped, got %q", got.Subject)
	}
}

func TestRenderSMSIsRaw(t *testing.T) {
	tmpl := domain.Template{Channel: domain.ChannelSMS, Body: "Code {{code}} for {{who}}"}
	got, err := Render(tmpl, map[string]any{"code": 1234, "who": "A&B"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Body != "Code 1234 for A&B" {
		t.Errorf("body = %q", got.Body)
	}
	if got.Subject != "" {
		t.Errorf("subject = %q, want empty", got.Subject)
	}
}

func TestRenderMissingVariableFails(t *testing.T) {
	tmpl := domain.Template{Channel: domain.ChannelSMS, Body: "Code {{code}}"}
	_, err := Render(tmpl, map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "code") {
		t.Fatalf("want missing-variable error naming 'code', got %v", err)
	}
}

func TestRenderSections(t *testing.T) {
	tmpl := domain.Template{Channel: domain.ChannelPush, Body: "{{#items}}[{{.}}]{{/items}}"}
	got, err := Render(tmpl, map[string]any{"items": []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if got.Body != "[a][b]" {
		t.Errorf("body = %q", got.Body)
	}
}

func TestValidateRejectsUnclosedSection(t *testing.T) {
	if err := Validate("", "{{#open}} never closed"); err == nil {
		t.Fatal("want parse error")
	}
	if err := Validate("Hi {{name}}", "Body {{x}}"); err != nil {
		t.Fatalf("valid template rejected: %v", err)
	}
}
