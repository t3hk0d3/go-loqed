package bridge_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/t3hk0d3/go-loqed/bridge"
)

func TestPrintingCredentialsDoesNotLeakKeys(t *testing.T) {
	const bridgeKey, keySecret = "RkFLRS1CUklER0UtS0VZ", "RkFLRS1LRVktU0VDUkVU"
	creds := bridge.Credentials{BridgeKey: bridgeKey, KeySecret: keySecret, LocalKeyID: 7}
	type holder struct {
		Creds bridge.Credentials
		bridge.Credentials
	}
	values := []any{creds, &creds, []bridge.Credentials{creds}, holder{creds, creds}}
	var outs []string
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		for _, v := range values {
			outs = append(outs, fmt.Sprintf(verb, v))
		}
	}
	for _, h := range []func(*bytes.Buffer) slog.Handler{
		func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
		func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
	} {
		for _, v := range values {
			var buf bytes.Buffer
			slog.New(h(&buf)).Info("creds", "v", v)
			outs = append(outs, buf.String())
		}
	}
	for _, out := range outs {
		if strings.Contains(out, bridgeKey) || strings.Contains(out, keySecret) {
			t.Fatalf("key leaked: %s", out)
		}
		if !strings.Contains(out, "7") {
			t.Fatalf("local key id missing: %s", out)
		}
	}
}
