package gateway

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/model"
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

// Run runs every supervisor until ctx is cancelled.
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

func (m *Manager) DeliverCloudEvent(ev cloud.WebhookEvent) error {
	return m.deliver(ev.LockID, CloudEventMsg{Event: ev})
}

func (m *Manager) DeliverCommand(lockID string, c model.Command, at time.Time) error {
	return m.deliver(lockID, CommandMsg{Command: c, At: at})
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
