package gateway

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"slices"
	"strconv"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/model"
)

// sortedHooks returns a copy of hooks in id order.
func sortedHooks(hooks []bridge.Webhook) []bridge.Webhook {
	out := slices.Clone(hooks)
	slices.SortStableFunc(out, func(a, b bridge.Webhook) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// webhookRevision identifies a webhook list (spec 5.9): sha256 over the
// entries in id order, each field (decimal id, URL, decimal trigger bitmap)
// prefixed with its 4-byte big-endian length; the first 16 hex characters.
func webhookRevision(hooks []bridge.Webhook) string {
	h := sha256.New()
	field := func(s string) {
		_, _ = h.Write(binary.BigEndian.AppendUint32(nil, uint32(len(s)))) //nolint:gosec // URLs are far below 4 GiB
		_, _ = h.Write([]byte(s))
	}
	for _, w := range sortedHooks(hooks) {
		field(strconv.FormatInt(int64(w.ID), 10))
		field(w.URL)
		field(strconv.FormatUint(uint64(w.Triggers&bridge.AllTriggers), 10))
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// ownWebhookURL is the gateway's registration URL for this lock ("" if it
// cannot be built).
func (s *Supervisor) ownWebhookURL() string {
	u, err := s.d.WebhookURL(s.Record())
	if err != nil {
		return ""
	}
	return u
}

// publishWebhooks publishes hooks as the lock's webhook list and remembers it
// as the list SetWebhooks requests are checked against.
func (s *Supervisor) publishWebhooks(hooks []bridge.Webhook) {
	hooks = sortedHooks(hooks)
	own := s.ownWebhookURL()
	l := model.WebhookList{Revision: webhookRevision(hooks), FetchedAt: s.d.Now(), Count: len(hooks),
		Webhooks: make([]model.WebhookEntry, 0, len(hooks))}
	for _, w := range hooks {
		l.Webhooks = append(l.Webhooks, model.WebhookEntry{ID: int(w.ID), URL: w.URL, Triggers: w.Triggers.Names(),
			Gateway: own != "" && w.URL == own})
	}
	s.hooks, s.hookList = hooks, &l
	if err := s.d.Publisher.PublishWebhooks(s.id, l); err != nil {
		s.log.Warn("publishing the bridge webhook list failed", "err", err)
	}
}

// listWebhooks reads the bridge's webhooks with the request timeout.
func (s *Supervisor) listWebhooks(ctx context.Context) ([]bridge.Webhook, error) {
	if s.bridge == nil {
		return nil, errNoBridge
	}
	c, cancel := s.reqCtx(ctx)
	defer cancel()
	return s.bridge.ListWebhooks(c)
}

// readWebhooks reads and publishes the list (local mode only). Transport
// failures count like any bridge request; it reports whether it worked.
func (s *Supervisor) readWebhooks(ctx context.Context) bool {
	err := s.rereadWebhooks(ctx)
	if errors.Is(err, loqed.ErrUnreachable) || errors.Is(err, loqed.ErrNoResponse) {
		s.httpFailure(ctx, err)
	}
	return err == nil
}

// rereadWebhooks reads and publishes the list without counting a failure,
// for callers that are themselves in the middle of bridge requests.
func (s *Supervisor) rereadWebhooks(ctx context.Context) error {
	if s.mode != model.ModeLocal {
		return errNotLocal
	}
	hooks, err := s.listWebhooks(ctx)
	if err != nil {
		s.log.Debug("reading the bridge webhook list failed", "err", err)
		return err
	}
	s.publishWebhooks(hooks)
	return nil
}

var errNotLocal = errors.New("gateway: the lock is not in local mode")

// webhookCountChanged: /status reports another number of webhooks than the
// published list holds, so the list is out of date.
func (s *Supervisor) webhookCountChanged(st *bridge.Status) bool {
	return s.hookList != nil && int(st.WebhooksNumber) != s.hookList.Count
}
