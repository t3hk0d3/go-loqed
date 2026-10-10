package auth_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/t3hk0d3/go-loqed/cloud/portal"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/auth"
)

func TestPrintingAPortalMinterDoesNotLeakThePassword(t *testing.T) {
	const password = "FAKE-CLOUD-PASSWORD"
	m := auth.NewPortalMinter(portal.New(), "me@example.com", password, "loqed-mqtt", nil)
	m.Login = nil // func values print as addresses; keep the output stable
	var outs []string
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		outs = append(outs, fmt.Sprintf(verb, m), fmt.Sprintf(verb, *m))
	}
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("minter", "v", m)
	slog.New(slog.NewTextHandler(&buf, nil)).Info("minter", "v", m)
	outs = append(outs, buf.String())
	for _, out := range outs {
		if strings.Contains(out, password) {
			t.Fatalf("password leaked: %s", out)
		}
	}
}
