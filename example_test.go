package loqed_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
)

// sendOnce sends a lock command and decides whether it may be sent again.
func sendOnce(ctx context.Context, c *bridge.Client) string {
	err := c.Command(ctx, bridge.ActionOpen)
	var apiErr *loqed.APIError
	switch {
	case err == nil:
		return "received by the bridge; wait for its webhooks"
	case errors.Is(err, loqed.ErrUnreachable):
		// The request provably never left: sending it again is safe.
		return "unreachable: retry"
	case errors.Is(err, loqed.ErrNoResponse):
		// The bridge may have the command and may act on it. A second OPEN
		// could unlatch the door twice: never resend, wait for webhooks.
		return "no response: do not retry"
	case errors.As(err, &apiErr):
		// The bridge answered, so the request arrived.
		return fmt.Sprintf("HTTP %d: do not retry", apiErr.StatusCode)
	default:
		return "failed: " + err.Error()
	}
}

func newBridge(addr string) *bridge.Client {
	c, err := bridge.New(addr, bridge.Credentials{
		BridgeKey: "Ym9uam91ciBtb25kZQ==", KeySecret: "SGFsbG8gd2VyZWxk", LocalKeyID: 1,
	})
	if err != nil {
		log.Fatal(err)
	}
	return c
}

// Classify a failed command with errors.Is: only ErrUnreachable allows
// another attempt.
func Example_retry() {
	ctx := context.Background()

	// Nothing listens: the connection is refused before the request is sent.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	closedAddr := ln.Addr().String()
	_ = ln.Close()
	fmt.Println(sendOnce(ctx, newBridge(closedAddr)))

	// The bridge reads the command, then drops the connection unanswered.
	dropping := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	defer dropping.Close()
	fmt.Println(sendOnce(ctx, newBridge(dropping.Listener.Addr().String())))

	// The bridge answers with an error status.
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "busy", http.StatusServiceUnavailable)
	}))
	defer failing.Close()
	fmt.Println(sendOnce(ctx, newBridge(failing.Listener.Addr().String())))
	// Output:
	// unreachable: retry
	// no response: do not retry
	// HTTP 503: do not retry
}
