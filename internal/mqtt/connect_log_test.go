package mqtt_test

import (
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"

	"github.com/t3hk0d3/go-loqed/internal/mqtt"
	"github.com/t3hk0d3/go-loqed/internal/testutil"
)

const (
	brokerUser = "broker-user"
	brokerPass = "broker-pass"
	badUser    = "wrong-user"
	badPass    = "wr0ng-s3cret"
)

// startFailingClient starts a client that cannot connect and returns its log.
func startFailingClient(t *testing.T, url string, user, pass string) *logBuffer {
	t.Helper()
	logs := &logBuffer{}
	c := mqtt.NewClient(mqtt.ClientConfig{URL: url, Username: user, Password: pass, ClientID: "gw-" + t.Name(), Topics: topics},
		slog.New(slog.NewTextHandler(logs, nil)))
	c.Start()
	t.Cleanup(c.Close)
	return logs
}

func assertNoSecrets(t *testing.T, logs string, secrets ...string) {
	t.Helper()
	for _, s := range secrets {
		if strings.Contains(logs, s) {
			t.Fatalf("log contains %q:\n%s", s, logs)
		}
	}
}

func TestConnectWithBadCredentialsLogsReasonWithoutSecrets(t *testing.T) {
	url := testutil.StartBrokerWithAuth(t, brokerUser, brokerPass)
	logs := startFailingClient(t, url, badUser, badPass)
	logs.waitLog(t, "cannot connect to MQTT broker")
	out := logs.String()
	for _, want := range []string{"level=WARN", "broker=" + url, `reason="not authorized (bad username or password)"`, "mqtt.username", "mqtt.password"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log lacks %q:\n%s", want, out)
		}
	}
	assertNoSecrets(t, out, badUser, badPass)
}

func TestConnectWithCredentialsInURLDoesNotLogThem(t *testing.T) {
	url := testutil.StartBrokerWithAuth(t, brokerUser, brokerPass)
	host := strings.TrimPrefix(url, "tcp://")
	logs := startFailingClient(t, "tcp://"+badUser+":"+badPass+"@"+host+"/?token=q-s3cret", "", "")
	logs.waitLog(t, "cannot connect to MQTT broker")
	out := logs.String()
	if !strings.Contains(out, "broker=tcp://"+host+" ") {
		t.Fatalf("broker not logged as scheme://host:port:\n%s", out)
	}
	assertNoSecrets(t, out, badUser, badPass, "q-s3cret", "token")
}

func TestConnectToClosedPortLogsConnectionRefused(t *testing.T) {
	url := testutil.ClosedPortURL(t)
	logs := startFailingClient(t, url, brokerUser, brokerPass)
	logs.waitLog(t, "cannot connect to MQTT broker")
	out := logs.String()
	for _, want := range []string{"broker=" + url, `reason="connection refused"`, "mqtt.url"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log lacks %q:\n%s", want, out)
		}
	}
	assertNoSecrets(t, out, brokerPass)
}

func TestConnectFailureReasons(t *testing.T) {
	netErr := func(err error) error { return fmt.Errorf("%w : %w", packets.ErrorNetworkError, err) }
	for _, tc := range []struct {
		name   string
		err    error
		reason string
		check  string
	}{
		{"refused", netErr(&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}),
			"connection refused", "mqtt.url"},
		{"bad credentials", packets.ErrorRefusedBadUsernameOrPassword, "not authorized (bad username or password)", "mqtt.password"},
		{"not authorised", packets.ErrorRefusedNotAuthorised, "not authorized (bad username or password)", "mqtt.username"},
		{"timeout", netErr(&net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded}), "timeout", "mqtt.url"},
		{"tls", netErr(x509.UnknownAuthorityError{}), "TLS", "mqtt.url"},
		{"host not found", netErr(&net.OpError{Op: "dial", Err: &net.DNSError{Err: "no such host", Name: "broker", IsNotFound: true}}),
			"host not found", "mqtt.url"},
		{"closed by broker", netErr(io.EOF), "connection closed by broker", "mqtt.url"},
		{"client id rejected", packets.ErrorRefusedIDRejected, "client id rejected", "mqtt.client_id"},
		{"other", errors.New("boom"), "other", "mqtt.url"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := &logBuffer{}
			c := mqtt.NewClient(mqtt.ClientConfig{URL: "tcp://broker:1883", ClientID: "x", Topics: topics},
				slog.New(slog.NewTextHandler(logs, nil)))
			c.ConnectFailed(tc.err)
			out := logs.String()
			if !strings.Contains(out, `reason="`+tc.reason+`"`) && !strings.Contains(out, "reason="+tc.reason+" ") {
				t.Fatalf("want reason %q:\n%s", tc.reason, out)
			}
			if !strings.Contains(out, tc.check) {
				t.Fatalf("want hint %q:\n%s", tc.check, out)
			}
			if strings.Contains(out, "boom") {
				t.Fatalf("raw error logged:\n%s", out)
			}
		})
	}
}

func TestRepeatedConnectFailuresAreRateLimited(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	logs := &logBuffer{}
	c := mqtt.NewClient(mqtt.ClientConfig{URL: "tcp://broker:1883", ClientID: "x", Topics: topics,
		Now: func() time.Time { return now }}, slog.New(slog.NewTextHandler(logs, nil)))
	refused := fmt.Errorf("%w : %w", packets.ErrorNetworkError, syscall.ECONNREFUSED)
	warnings := func() int { return strings.Count(logs.String(), "cannot connect to MQTT broker") }

	for range 3 {
		c.ConnectFailed(refused)
		now = now.Add(time.Minute)
	}
	if n := warnings(); n != 1 {
		t.Fatalf("repeats within the window logged: %d warnings\n%s", n, logs.String())
	}

	// A different reason is news and is logged at once.
	c.ConnectFailed(packets.ErrorRefusedBadUsernameOrPassword)
	if n := warnings(); n != 2 {
		t.Fatalf("new reason not logged: %d warnings\n%s", n, logs.String())
	}

	now = now.Add(10 * time.Minute)
	c.ConnectFailed(refused)
	out := logs.String()
	if n := warnings(); n != 3 || !strings.Contains(out, "repeated=2") {
		t.Fatalf("want a third warning counting 2 repeats:\n%s", out)
	}

	c.ConnectSucceeded()
	out = logs.String()
	if strings.Count(out, "level=INFO") != 1 || !strings.Contains(out, "connected to MQTT broker") ||
		!strings.Contains(out, "failed_attempts=5") {
		t.Fatalf("want one info line with the failed attempts:\n%s", out)
	}

	// After a successful connect the next failure is reported at once.
	c.ConnectFailed(refused)
	if n := warnings(); n != 4 {
		t.Fatalf("failure after reconnect not logged: %d warnings\n%s", n, logs.String())
	}
}

func TestConnectWithoutFailuresHasNoFailedAttempts(t *testing.T) {
	logs := &logBuffer{}
	c := mqtt.NewClient(mqtt.ClientConfig{URL: "tcp://u:pw@broker:1883", ClientID: "x", Topics: topics},
		slog.New(slog.NewTextHandler(logs, nil)))
	c.ConnectSucceeded()
	out := logs.String()
	if !strings.Contains(out, "connected to MQTT broker") || strings.Contains(out, "failed_attempts") {
		t.Fatalf("got:\n%s", out)
	}
	assertNoSecrets(t, out, ":pw@")
}
