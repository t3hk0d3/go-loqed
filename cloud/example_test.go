package cloud_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
)

// fakeLockAPI stands in for the LOQED cloud in the examples.
func fakeLockAPI() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer my-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/api/locks/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"data":[{"id":"QnZkWlR4","name":"Front door","bolt_state":"night_lock",
				"battery_percentage":"88","online":1,"bridge_ip":"192.168.1.20",
				"bridge_key":"Ym9uam91ciBtb25kZQ==","key_secret":"SGFsbG8gd2VyZWxk","local_id":"1"}]}`)
		case "/api/locks/QnZkWlR4/bolt_state/day_lock":
			_, _ = io.WriteString(w, `{}`)
		default:
			http.NotFound(w, r)
		}
	}))
}

func ExampleClient_ListLocks() {
	srv := fakeLockAPI()
	defer srv.Close()

	// A real client needs only the token: cloud.New("my-token").
	c := cloud.New("my-token", cloud.WithBaseURL(srv.URL))

	// Status reads are limited to 12 per 12 hours per account: call this
	// rarely and keep the result.
	locks, err := c.ListLocks(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	for _, l := range locks {
		fmt.Println(l.Name, l.BoltState, l.BatteryPercentage, *l.Online)
		if !l.HasLocalCredentials() {
			continue
		}
		// The lock's bridge can be driven locally.
		b, err := bridge.New(l.BridgeIP, bridge.Credentials{
			BridgeKey:  l.BridgeKey,
			KeySecret:  l.KeySecret,
			LocalKeyID: uint8(*l.LocalID),
		})
		if err != nil {
			log.Fatal(err)
		}
		_ = b
		fmt.Println("bridge at", l.BridgeIP, "key", *l.LocalID)
	}
	// Output:
	// Front door night_lock 88 true
	// bridge at 192.168.1.20 key 1
}

func ExampleClient_Command() {
	srv := fakeLockAPI()
	defer srv.Close()
	c := cloud.New("my-token", cloud.WithBaseURL(srv.URL))

	// Sent with the token's own lock key. nil means the cloud accepted the
	// request; the lock's webhooks confirm the move.
	if err := c.Command(context.Background(), "QnZkWlR4", loqed.BoltDayLock); err != nil {
		log.Fatal(err)
	}
	fmt.Println("accepted")
	// Output:
	// accepted
}

func ExampleParseWebhook() {
	// The body of a webhook configured in the LOQED portal. Cloud webhooks
	// are unsigned: authenticate the request (a secret URL path) before
	// parsing it.
	body := []byte(`{"lock_id":"8212","event_type":"STATE_CHANGED_NIGHT_LOCK","requested_state":"NIGHT_LOCK",
		"key_local_id":2,"key_name_user":"Alice"}`)
	ev, err := cloud.ParseWebhook(body)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(ev.Kind == cloud.KindStateReached, ev.LockID, ev.BoltState, *ev.KeyLocalID, ev.KeyNameUser)
	// Output:
	// true 8212 night_lock 2 Alice
}
