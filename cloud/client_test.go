package cloud_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
)

// Based on HA core tests/components/loqed/fixtures/get_all_locks.json.
const locksJSON = `{"data":[{
  "id":"Foo","name":"MyLock","model_name":"LOQED Touch","battery_percentage":64,
  "battery_type":"nickel_metal_hydride","bolt_state":"day_lock","party_mode":false,
  "guest_access_mode":false,"twist_assist":false,"touch_to_connect":true,
  "lock_direction":"clockwise","mortise_lock_type":"cylinder_operated_no_handle_on_the_outside",
  "supported_lock_states":["open","day_lock","night_lock"],"online":true,
  "bridge_ip":"192.168.12.34","bridge_hostname":"LOQED-aabbccddeeff.local","bridge_mac_wifi":"aa:bb:cc:dd:ee:ff",
  "local_id":1,"key_secret":"SGFsbG8gd2VyZWxk","backend_key":"aGVsbG8gd29ybGQ=",
  "bridge_webhook_count":1,"bridge_key":"Ym9uam91ciBtb25kZQ=="
},{
  "id":"Bar","name":"Pure","battery_percentage":"80","bolt_state":"NIGHT_LOCK","online":0
}]}`

func newServer(t *testing.T, h http.HandlerFunc) *cloud.Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return cloud.New("tok", cloud.WithBaseURL(srv.URL))
}

func TestListLocks(t *testing.T) {
	c := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/locks/" || r.Header.Get("Authorization") != "Bearer tok" || r.Header.Get("Accept") != "application/json" {
			t.Errorf("unexpected request %s %v", r.URL.Path, r.Header)
		}
		_, _ = w.Write([]byte(locksJSON))
	})
	locks, err := c.ListLocks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(locks) != 2 {
		t.Fatalf("got %d locks", len(locks))
	}
	a, b := locks[0], locks[1]
	if a.ID != "Foo" || a.Name != "MyLock" || a.ModelName != "LOQED Touch" || a.BoltState != loqed.BoltDayLock ||
		a.BatteryPercentage != 64 || !a.TouchToConnect || a.Online == nil || !*a.Online ||
		a.BridgeIP != "192.168.12.34" || a.LocalID == nil || *a.LocalID != 1 || a.KeySecret != "SGFsbG8gd2VyZWxk" ||
		a.BridgeKey != "Ym9uam91ciBtb25kZQ==" || a.BridgeMacWifi != "aa:bb:cc:dd:ee:ff" {
		t.Fatalf("lock a: %+v", a)
	}
	if !a.HasLocalCredentials() {
		t.Error("lock a should have local credentials")
	}
	if b.HasLocalCredentials() || b.BoltState != loqed.BoltNightLock || b.BatteryPercentage != 80 || b.Online == nil || *b.Online {
		t.Fatalf("lock b: %+v", b)
	}
}

func TestListLocksRedirectToLoginIsUnauthorized(t *testing.T) {
	c := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/login", http.StatusFound)
	})
	if _, err := c.ListLocks(context.Background()); !errors.Is(err, loqed.ErrUnauthorized) {
		t.Fatalf("got %v", err)
	}
}

func TestListLocksRateLimited(t *testing.T) {
	c := newServer(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTooManyRequests) })
	if _, err := c.ListLocks(context.Background()); !errors.Is(err, loqed.ErrRateLimited) {
		t.Fatalf("got %v", err)
	}
}

func TestListLocksInvalidJSON(t *testing.T) {
	c := newServer(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data":"nope"}`)) })
	if _, err := c.ListLocks(context.Background()); !errors.Is(err, loqed.ErrInvalidPayload) {
		t.Fatalf("got %v", err)
	}
}

func TestCommand(t *testing.T) {
	var path string
	c := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer tok" {
			t.Errorf("unexpected %s %v", r.Method, r.Header)
		}
		path = r.URL.EscapedPath()
	})
	if err := c.Command(context.Background(), "Yq1gK4oeE9KWe0ByxjX2", loqed.BoltNightLock); err != nil {
		t.Fatal(err)
	}
	if path != "/api/locks/Yq1gK4oeE9KWe0ByxjX2/bolt_state/night_lock" {
		t.Fatalf("path %q", path)
	}
}

func TestCommandFollowedRedirectToLoginIsUnauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/login" {
			_, _ = w.Write([]byte("<!DOCTYPE html><html>login</html>"))
			return
		}
		http.Redirect(w, r, "/login", http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	// A caller-supplied client that follows redirects.
	c := cloud.New("tok", cloud.WithBaseURL(srv.URL), cloud.WithHTTPClient(http.DefaultClient))
	if err := c.Command(context.Background(), "x", loqed.BoltOpen); !errors.Is(err, loqed.ErrUnauthorized) {
		t.Fatalf("got %v", err)
	}
}

func TestHasLocalCredentialsRequiresValidLocalID(t *testing.T) {
	id := 256
	l := cloud.Lock{BridgeIP: "192.0.2.1", BridgeKey: "a", KeySecret: "b", LocalID: &id}
	if l.HasLocalCredentials() {
		t.Fatal("local_id 256 cannot be a key id")
	}
}

func TestCommandRejectsUnknownState(t *testing.T) {
	c := cloud.New("tok")
	if err := c.Command(context.Background(), "x", loqed.BoltUnknown); err == nil {
		t.Fatal("expected error")
	}
}
