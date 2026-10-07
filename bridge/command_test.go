package bridge_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
)

func TestCommandMatchesGoldenVectors(t *testing.T) {
	golden := map[bridge.Action]string{
		bridge.ActionOpen:   "AAAAAAAAAAACBwAAAABlU/EARYhOAuwxCIZ/FjVsh9UOA640EQTc6vzh2ciIqxhAvYYBAQE%3D",
		bridge.ActionUnlock: "AAAAAAAAAAACBwAAAABlU/EAoZ0RimZk4XA5ythE2rUqS683q6es7JEPvFp/Y443zSEBAQI%3D",
		bridge.ActionLock:   "AAAAAAAAAAACBwAAAABlU/EAUTwY3s6dLYU3aJaH1s%2B%2BqSku8HERbL7hApizYmPXKfoBAQM%3D",
	}
	for action, want := range golden {
		var gotQuery string
		c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/to_lock" {
				t.Errorf("path %q", r.URL.Path)
			}
			gotQuery = r.URL.RawQuery
		}))
		if err := c.Command(context.Background(), action); err != nil {
			t.Fatal(err)
		}
		if gotQuery != "command_signed_base64="+want {
			t.Errorf("action %d:\n got  %s\n want command_signed_base64=%s", action, gotQuery, want)
		}
	}
}

func TestCommandRejectsUnknownAction(t *testing.T) {
	for _, a := range []bridge.Action{0, 9} {
		c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Errorf("request sent for invalid action")
		}))
		if err := c.Command(context.Background(), a); err == nil {
			t.Errorf("action %d: expected error", a)
		}
	}
}

func TestCommandErrorDoesNotLeakSignedCommand(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	err := c.Command(context.Background(), bridge.ActionLock)
	if !errors.Is(err, loqed.ErrUnauthorized) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), "AAAAAAAA") {
		t.Fatalf("error leaks command: %v", err)
	}
}
