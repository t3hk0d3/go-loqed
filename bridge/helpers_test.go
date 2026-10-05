package bridge_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/bridge"
)

// Golden-vector inputs; see testdata/gen_vectors.py.
const (
	testBridgeKey = "Ym9uam91ciBtb25kZQ=="
	testKeySecret = "SGFsbG8gd2VyZWxk"
)

var fixedNow = time.Unix(1700000000, 0)

func newTestClient(t *testing.T, h http.Handler) *bridge.Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := bridge.New("192.0.2.1",
		bridge.Credentials{BridgeKey: testBridgeKey, KeySecret: testKeySecret, LocalKeyID: 1},
		bridge.WithBaseURL(srv.URL),
		bridge.WithClock(func() time.Time { return fixedNow }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
