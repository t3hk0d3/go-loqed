package portal_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/t3hk0d3/go-loqed/cloud/portal"
)

func TestPrintingATokenDoesNotLeakItsValue(t *testing.T) {
	const value = "FAKE-PERSONAL-ACCESS-TOKEN"
	tok := portal.Token{ID: "42", Name: "loqed-mqtt", Value: value}
	var outs []string
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		outs = append(outs, fmt.Sprintf(verb, tok), fmt.Sprintf(verb, []portal.Token{tok}))
	}
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("token", "v", tok, "all", []portal.Token{tok})
	slog.New(slog.NewTextHandler(&buf, nil)).Info("token", "v", tok, "all", []portal.Token{tok})
	outs = append(outs, buf.String())
	for _, out := range outs {
		if strings.Contains(out, value) {
			t.Fatalf("token leaked: %s", out)
		}
		if !strings.Contains(out, "loqed-mqtt") {
			t.Fatalf("token name missing: %s", out)
		}
	}
}
