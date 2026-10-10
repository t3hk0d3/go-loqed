package gateway

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/model"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/store"
)

var (
	ErrUnknownLock = errors.New("gateway: unknown lock")
	ErrBusy        = errors.New("gateway: lock supervisor is busy")
)

type running struct {
	s      *Supervisor
	cancel context.CancelFunc
	done   chan struct{}
}

// Manager routes webhooks and commands to lock supervisors and stops the
// supervisors of locks removed from the account at runtime.
type Manager struct {
	mu   sync.Mutex
	sups map[string]*running
	wg   sync.WaitGroup
}

func NewManager(sups []*Supervisor) *Manager {
	m := &Manager{sups: make(map[string]*running, len(sups))}
	for _, s := range sups {
		m.sups[s.ID()] = &running{s: s, done: make(chan struct{})}
	}
	return m
}

// Run runs every supervisor until ctx is cancelled. Run must be called once.
func (m *Manager) Run(ctx context.Context) {
	m.mu.Lock()
	for _, r := range m.sups {
		sctx, cancel := context.WithCancel(ctx)
		r.cancel = cancel
		m.wg.Go(func() {
			defer close(r.done)
			r.s.Run(sctx)
		})
	}
	m.mu.Unlock()
	m.wg.Wait()
}

// Remove stops the supervisors of ids and waits until they have exited, so
// they can no longer publish. It returns the ids that were running.
func (m *Manager) Remove(ids []string) []string {
	var stopped []*running
	var out []string
	m.mu.Lock()
	for _, id := range ids {
		if r, ok := m.sups[id]; ok {
			delete(m.sups, id)
			stopped = append(stopped, r)
			out = append(out, id)
		}
	}
	m.mu.Unlock()
	for _, r := range stopped {
		if r.cancel != nil {
			r.cancel()
			<-r.done
		}
	}
	return out
}

func (m *Manager) get(lockID string) (*Supervisor, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.sups[lockID]
	if !ok {
		return nil, false
	}
	return r.s, true
}

func (m *Manager) deliver(lockID string, msg any) error {
	s, ok := m.get(lockID)
	if !ok {
		return ErrUnknownLock
	}
	if !s.Deliver(msg) {
		return ErrBusy
	}
	return nil
}

func (m *Manager) DeliverBridgeEvent(lockID string, ev bridge.Event) error {
	return m.deliver(lockID, BridgeEventMsg{Event: ev})
}

// DeliverCloudEvent routes a cloud webhook by the lock id in its URL. The
// body's numeric lock id must match the one learned for that lock.
func (m *Manager) DeliverCloudEvent(lockID string, ev cloud.WebhookEvent) error {
	s, ok := m.get(lockID)
	if !ok {
		return ErrUnknownLock
	}
	if err := s.BindCloudID(ev.LockID); err != nil {
		return err
	}
	if !s.Deliver(CloudEventMsg{Event: ev}) {
		return ErrBusy
	}
	return nil
}

// DeliverCloudWebhook decodes a raw cloud webhook body and routes it like
// DeliverCloudEvent. It is the one path for every cloud webhook input (HTTP
// and MQTT), so both share the binding rules. The decoded event is returned
// so a caller can log its lock id on ErrCloudIDMismatch; it is the zero value
// when decoding failed (loqed.ErrInvalidPayload). The body is never logged.
func (m *Manager) DeliverCloudWebhook(lockID string, body []byte) (cloud.WebhookEvent, error) {
	ev, err := cloud.ParseWebhook(body)
	if err != nil {
		return cloud.WebhookEvent{}, err
	}
	return ev, m.DeliverCloudEvent(lockID, ev)
}

func (m *Manager) DeliverCommand(lockID string, c model.Command, id string, at time.Time) error {
	return m.deliver(lockID, CommandMsg{Command: c, ID: id, At: at})
}

// UpdateRecords hands refreshed credentials to the running supervisors.
// DeliverWebhooksRequest routes a raw SetWebhooks request to its lock.
func (m *Manager) DeliverWebhooksRequest(lockID string, body []byte) error {
	return m.deliver(lockID, WebhooksRequestMsg{Body: body})
}

func (m *Manager) UpdateRecords(recs []store.LockRecord) {
	for _, r := range recs {
		// Unknown locks are not managed; a full queue only delays the
		// update until the lock's own next refresh.
		_ = m.deliver(r.ID, RecordMsg{Record: r})
	}
}

func (m *Manager) BridgeKey(lockID string) ([]byte, bool) {
	s, ok := m.get(lockID)
	if !ok {
		return nil, false
	}
	return s.BridgeKey()
}

func (m *Manager) Health() map[string]Health {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]Health, len(m.sups))
	for id, r := range m.sups {
		out[id] = r.s.Health()
	}
	return out
}
