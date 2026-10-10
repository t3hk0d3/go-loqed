package loqed_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
)

const fakeSecret = "FAKE-SECRET-c2VjcmV0"

type holder struct {
	Name   string
	Secret loqed.Secret
}

func assertRedacted(t *testing.T, out string) {
	t.Helper()
	if strings.Contains(out, fakeSecret) {
		t.Fatalf("secret leaked: %s", out)
	}
	if !strings.Contains(out, loqed.Redacted) {
		t.Fatalf("redaction marker missing: %s", out)
	}
}

func TestSecretIsRedactedByEveryFmtVerb(t *testing.T) {
	s := loqed.Secret(fakeSecret)
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%10s"} {
		t.Run(verb, func(t *testing.T) {
			assertRedacted(t, fmt.Sprintf(verb, s))
			assertRedacted(t, fmt.Sprintf(verb, &s))
		})
	}
	assertRedacted(t, fmt.Sprint(s))
	assertRedacted(t, fmt.Sprintln(s))
	assertRedacted(t, s.String())
	assertRedacted(t, s.GoString())
}

func TestSecretNestedInStructSliceOrMapIsRedactedByFmt(t *testing.T) {
	v := []holder{{Name: "front door", Secret: fakeSecret}}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		t.Run(verb, func(t *testing.T) {
			assertRedacted(t, fmt.Sprintf(verb, v))
			assertRedacted(t, fmt.Sprintf(verb, &v[0]))
			assertRedacted(t, fmt.Sprintf(verb, map[string]holder{"a": v[0]}))
		})
	}
}

func TestSecretIsRedactedBySlog(t *testing.T) {
	handlers := map[string]func(*bytes.Buffer) slog.Handler{
		"text": func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
		"json": func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
	}
	values := map[string]any{
		"direct":  loqed.Secret(fakeSecret),
		"struct":  holder{Name: "front door", Secret: fakeSecret},
		"pointer": &holder{Name: "front door", Secret: fakeSecret},
		"slice":   []holder{{Name: "front door", Secret: fakeSecret}},
	}
	for hn, h := range handlers {
		for vn, v := range values {
			t.Run(hn+"/"+vn, func(t *testing.T) {
				var buf bytes.Buffer
				slog.New(h(&buf)).Info("msg", "v", v)
				assertRedacted(t, buf.String())
			})
		}
	}
}

func TestSecretMarshalsToJSONAsRedactionMarker(t *testing.T) {
	b, err := json.Marshal(holder{Secret: fakeSecret})
	if err != nil {
		t.Fatal(err)
	}
	assertRedacted(t, string(b))
}

func TestSecretDecodesFromJSONString(t *testing.T) {
	var h holder
	if err := json.Unmarshal([]byte(`{"Secret":"`+fakeSecret+`"}`), &h); err != nil {
		t.Fatal(err)
	}
	if h.Secret.Reveal() != fakeSecret {
		t.Fatal("decoded secret differs from the input")
	}
}

func TestSecretRevealReturnsRawValue(t *testing.T) {
	if loqed.Secret(fakeSecret).Reveal() != fakeSecret {
		t.Fatal("Reveal differs from the raw value")
	}
}

func ExampleSecret() {
	key := loqed.Secret("SGFsbG8gd2VyZWxk")
	fmt.Printf("%v %+v\n", key, struct{ Key loqed.Secret }{key})
	b, _ := json.Marshal(key)
	fmt.Println(string(b), key.Reveal())
	// Output:
	// [redacted] {Key:[redacted]}
	// "[redacted]" SGFsbG8gd2VyZWxk
}

func TestEmptySecretPrintsAsEmpty(t *testing.T) {
	// An unset secret hides nothing, so "not configured" stays visible.
	if got := fmt.Sprintf("%v|%s|%#v", loqed.Secret(""), loqed.Secret(""), loqed.Secret("")); got != `||""` {
		t.Fatalf("got %q", got)
	}
	b, err := json.Marshal(loqed.Secret(""))
	if err != nil || string(b) != `""` {
		t.Fatalf("got %s, %v", b, err)
	}
}
