// Package webhook serves bridge and cloud webhooks and the health check.
package webhook

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/gateway"
)

const maxBody = 64 << 10

// MQTTGrace is how long MQTT may be down before /healthz reports 503, so a
// broker restart does not make the add-on watchdog restart the gateway.
const MQTTGrace = 5 * time.Minute

type Sink interface {
	BridgeKey(lockID string) ([]byte, bool)
	DeliverBridgeEvent(lockID string, ev bridge.Event) error
	DeliverCloudEvent(lockID string, ev cloud.WebhookEvent) error
	Health() map[string]gateway.Health
}

type Options struct {
	Sink        Sink
	CloudSecret string               // empty disables POST /cloud/{secret}/{id}
	MQTTDownFor func() time.Duration // 0 while connected
	Now         func() time.Time
	Log         *slog.Logger
}

// HealthReport is the /healthz body.
type HealthReport struct {
	MQTTConnected bool                      `json:"mqtt_connected"`
	Locks         map[string]gateway.Health `json:"locks"`
}

func NewHandler(o Options) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook/{id}", o.bridgeWebhook)
	if o.CloudSecret != "" {
		mux.HandleFunc("POST /cloud/{secret}/{id}", o.cloudWebhook)
	}
	mux.HandleFunc("GET /healthz", o.healthz)
	return mux
}

func (o Options) bridgeWebhook(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	hash, ts := r.Header.Get("Hash"), r.Header.Get("Timestamp")
	if hash == "" || ts == "" {
		http.Error(w, "missing TIMESTAMP or HASH header", http.StatusBadRequest)
		return
	}
	key, ok := o.Sink.BridgeKey(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	ev, err := bridge.ParseEvent(key, body, hash, ts, o.Now())
	switch {
	case errors.Is(err, loqed.ErrStaleTimestamp):
		// Only reachable with a valid HASH, so this is a real clock problem.
		o.Log.Warn("rejected a bridge webhook with a stale timestamp", "lock_id", id, "err", err)
		http.Error(w, "stale timestamp", http.StatusUnauthorized)
		return
	case errors.Is(err, loqed.ErrBadSignature):
		o.Log.Warn("rejected a bridge webhook with a bad signature", "lock_id", id)
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	case err != nil:
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	o.deliverResult(w, r, o.Sink.DeliverBridgeEvent(id, ev))
}

func (o Options) cloudWebhook(w http.ResponseWriter, r *http.Request) {
	if subtle.ConstantTimeCompare([]byte(r.PathValue("secret")), []byte(o.CloudSecret)) != 1 {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	ev, err := cloud.ParseWebhook(body) // the body is never logged: it holds personal data
	if err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	id := r.PathValue("id")
	err = o.Sink.DeliverCloudEvent(id, ev)
	if errors.Is(err, gateway.ErrCloudIDMismatch) {
		o.Log.Warn("rejected a cloud webhook registered on another lock's URL; register each lock's own URL",
			"lock_id", id, "cloud_lock_id", ev.LockID)
		http.Error(w, "this URL belongs to another lock", http.StatusConflict)
		return
	}
	o.deliverResult(w, r, err)
}

func (o Options) deliverResult(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case err == nil:
		w.WriteHeader(http.StatusOK)
	case errors.Is(err, gateway.ErrUnknownLock):
		http.NotFound(w, r)
	default:
		http.Error(w, "busy, retry later", http.StatusServiceUnavailable)
	}
}

func (o Options) healthz(w http.ResponseWriter, _ *http.Request) {
	var down time.Duration
	if o.MQTTDownFor != nil {
		down = o.MQTTDownFor()
	}
	code := http.StatusOK
	if down > MQTTGrace {
		code = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(HealthReport{MQTTConnected: down == 0, Locks: o.Sink.Health()})
}
