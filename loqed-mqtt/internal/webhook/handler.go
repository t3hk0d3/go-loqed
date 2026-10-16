// Package webhook serves bridge and cloud webhooks and the health check.
package webhook

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/gateway"
)

const maxBody = 64 << 10

// MQTTGrace is how long MQTT may be down before /healthz reports 503, so a
// broker restart does not make the add-on watchdog restart the gateway.
const MQTTGrace = 5 * time.Minute

type Sink interface {
	BridgeKey(lockID string) ([]byte, bool)
	DeliverBridgeEvent(lockID string, ev bridge.Event) error
	DeliverCloudWebhook(lockID string, body []byte) (cloud.WebhookEvent, error)
	Health() map[string]gateway.Health
}

type Options struct {
	Sink        Sink
	CloudSecret string // empty disables POST /cloud/{secret}/{id}
	// BridgeTimestampTolerance is the accepted age of a bridge webhook's
	// TIMESTAMP; 0 accepts any age. It also sets how long applied deliveries
	// are remembered so that a repeat is applied only once.
	BridgeTimestampTolerance time.Duration
	MQTTDownFor              func() time.Duration // 0 while connected
	Now                      func() time.Time
	Log                      *slog.Logger

	seen *seenDeliveries
}

// HealthReport is the /healthz body.
type HealthReport struct {
	MQTTConnected bool                      `json:"mqtt_connected"`
	Locks         map[string]gateway.Health `json:"locks"`
}

type server struct {
	http.Handler
	seen *seenDeliveries
}

func NewHandler(o Options) http.Handler {
	if o.Now == nil {
		o.Now = time.Now
	}
	o.seen = newSeenDeliveries(o.BridgeTimestampTolerance)
	s := &server{seen: o.seen}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook/{id}", o.bridgeWebhook)
	if o.CloudSecret != "" {
		mux.HandleFunc("POST /cloud/{secret}/{id}", o.cloudWebhook)
	}
	mux.HandleFunc("GET /healthz", o.healthz)
	s.Handler = mux
	return s
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
	now := o.Now()
	ev, err := bridge.ParseEventWithin(key, body, hash, ts, now, o.BridgeTimestampTolerance)
	switch {
	case errors.Is(err, loqed.ErrStaleTimestamp):
		// Only reachable with a valid HASH: the bridge delivered it late
		// (it calls its webhooks one after another) or the clocks differ.
		o.Log.Warn("rejected a bridge webhook with a stale timestamp; if the bridge delivers late, raise "+
			"webhook.bridge_timestamp_tolerance (0 turns the check off), otherwise check NTP on this host and the bridge",
			"lock_id", id, "err", err)
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
	// The HASH is verified, so its normalized form (as ParseEventWithin
	// compares it) identifies this delivery.
	sig := strings.ToLower(strings.TrimSpace(hash))
	if !o.seen.claim(id, sig, now) {
		// Answer 200: the delivery was applied, and an error would only make
		// a retrying sender try again. Never log the HASH or the body.
		if ok, repeated := o.seen.shouldLog(id, now); ok {
			args := []any{"lock_id", id}
			if repeated > 0 {
				args = append(args, "repeated", repeated)
			}
			o.Log.Info("ignored a bridge webhook delivery that was already applied (delivered twice or resent)", args...)
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	err = o.Sink.DeliverBridgeEvent(id, ev)
	if err != nil {
		o.seen.release(id, sig) // not applied: a retry must not count as a repeat
	}
	o.deliverResult(w, r, err)
}

func (o Options) cloudWebhook(w http.ResponseWriter, r *http.Request) {
	if subtle.ConstantTimeCompare([]byte(r.PathValue("secret")), []byte(o.CloudSecret)) != 1 {
		// Never log the path: it may hold a (mistyped) secret.
		o.Log.Debug("rejected a cloud webhook with a wrong secret; check the URL registered at app.loqed.com")
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	id := r.PathValue("id")
	ev, err := o.Sink.DeliverCloudWebhook(id, body) // the body is never logged: it holds personal data
	if errors.Is(err, loqed.ErrInvalidPayload) {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	if errors.Is(err, gateway.ErrCloudIDMismatch) {
		o.Log.Warn("rejected a cloud webhook registered on another lock's URL; register each lock's own URL",
			"lock_id", id, "cloud_lock_id", ev.LockID, "bound_cloud_lock_id", gateway.BoundCloudID(err))
		http.Error(w, "this URL belongs to another lock", http.StatusConflict)
		return
	}
	if errors.Is(err, gateway.ErrUnknownLock) {
		o.Log.Debug("rejected a cloud webhook for an unknown lock; check the URL registered at app.loqed.com", "lock_id", id)
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
