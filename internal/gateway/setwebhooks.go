package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

// webhookOp is one bridge write of a SetWebhooks request.
type webhookOp struct {
	delete   bool
	id       int // delete
	url      string
	triggers bridge.Triggers // create
}

// webhookPlan is what a SetWebhooks request changes: deletions in id order,
// then creations in request order; kept lists the ids left unchanged.
type webhookPlan struct {
	ops  []webhookOp
	kept []int
}

// planWebhooks checks a request against the current list and works out the
// bridge writes (spec 5.9). own is the gateway's webhook URL for this lock
// ("" if unknown); the gateway's own webhook is never written. A rejected
// request returns *model.WebhooksInvalidError.
func planWebhooks(current []bridge.Webhook, req model.WebhooksRequest, own, lockID string) (webhookPlan, error) {
	invalid := func(i int, format string, args ...any) error {
		return &model.WebhooksInvalidError{Detail: fmt.Sprintf("webhooks[%d]: ", i) + fmt.Sprintf(format, args...), RequestID: req.RequestID}
	}
	current = sortedHooks(current)
	ownPath := "/webhook/" + lockID
	isOwn := func(u string) bool {
		if own != "" {
			return u == own
		}
		// Without a known own URL, every registration on this lock's
		// gateway path is left to the registration check.
		p, err := url.Parse(u)
		return err == nil && p.Path == ownPath
	}
	byID := map[int]bridge.Webhook{}
	for _, w := range current {
		byID[int(w.ID)] = w
	}

	keep := map[int]bool{}     // ids left as they are
	recreate := map[int]bool{} // ids deleted to be created again
	idURLs := map[string]int{} // URL of every webhook listed by id → its entry
	var creates []webhookOp    // in request order
	for i, spec := range req.Webhooks {
		if spec.ID == nil {
			continue
		}
		w, ok := byID[*spec.ID]
		switch {
		case !ok:
			return webhookPlan{}, invalid(i, "id %d is not on the bridge", *spec.ID)
		case keep[*spec.ID] || recreate[*spec.ID]:
			return webhookPlan{}, invalid(i, "id %d is listed twice", *spec.ID)
		}
		idURLs[w.URL] = i
		switch {
		case isOwn(w.URL):
			if spec.HasTriggers && spec.Triggers != bridge.AllTriggers {
				return webhookPlan{}, invalid(i, "the gateway's own webhook always has all triggers")
			}
			keep[*spec.ID] = true
		case spec.HasTriggers && spec.Triggers != w.Triggers&bridge.AllTriggers:
			recreate[*spec.ID] = true
		default:
			keep[*spec.ID] = true
		}
	}
	seenURL := map[string]bool{}
	for i, spec := range req.Webhooks {
		if spec.ID != nil {
			if recreate[*spec.ID] {
				creates = append(creates, webhookOp{url: byID[*spec.ID].URL, triggers: spec.Triggers})
			}
			continue
		}
		u := *spec.URL
		switch {
		case seenURL[u]:
			return webhookPlan{}, invalid(i, "url is listed twice")
		case hasURL(idURLs, u):
			return webhookPlan{}, invalid(i, "url is the url of webhooks[%d]", idURLs[u])
		}
		seenURL[u] = true
		if p, err := url.Parse(u); err == nil && p.Path == ownPath && (own == "" || u != own) {
			return webhookPlan{}, invalid(i, "url is on this lock's gateway webhook path but is not the gateway's own url")
		}
		if u == own {
			if spec.Triggers != bridge.AllTriggers {
				return webhookPlan{}, invalid(i, "the gateway's own webhook always has all triggers")
			}
			continue // registration stays with the registration check
		}
		var match *bridge.Webhook
		for _, w := range current {
			if w.URL != u || keep[int(w.ID)] || recreate[int(w.ID)] {
				continue
			}
			if w.Triggers&bridge.AllTriggers == spec.Triggers {
				match = &w
				break
			}
			if match == nil {
				match = &w
			}
		}
		switch {
		case match == nil:
			creates = append(creates, webhookOp{url: u, triggers: spec.Triggers})
		case match.Triggers&bridge.AllTriggers == spec.Triggers:
			keep[int(match.ID)] = true
		default:
			recreate[int(match.ID)] = true
			creates = append(creates, webhookOp{url: u, triggers: spec.Triggers})
		}
	}

	var p webhookPlan
	for _, w := range current {
		id := int(w.ID)
		switch {
		case keep[id] || isOwn(w.URL):
			p.kept = append(p.kept, id)
		default: // unlisted, or deleted to be re-created
			p.ops = append(p.ops, webhookOp{delete: true, id: id, url: w.URL})
		}
	}
	p.ops = append(p.ops, creates...)
	slices.Sort(p.kept)
	return p, nil
}

func hasURL(m map[string]int, u string) bool { _, ok := m[u]; return ok }

// WebhooksRequestMsg is a raw SetWebhooks request received over MQTT.
type WebhooksRequestMsg struct{ Body []byte }

// maxQueuedWebhookRequests: requests waiting behind the running one.
const maxQueuedWebhookRequests = 4

// webhookJob is a SetWebhooks request being applied, one bridge call per step.
type webhookJob struct {
	requestID *string
	plan      webhookPlan
	next      int          // index of the next op
	before    map[int]bool // ids on the bridge before the request
	removed   []int
	created   []string // URLs created, in order
}

// commandBusy: a lock command is being delivered, awaits its confirmation,
// or waits for one that does.
func (s *Supervisor) commandBusy() bool {
	p := &s.cmds
	return p.pending != nil || (p.active != nil && !p.active.terminal())
}

// webhooksReady: a SetWebhooks step can run now.
func (s *Supervisor) webhooksReady() bool {
	return (s.hookJob != nil || len(s.hookQueue) > 0) && !s.commandBusy()
}

func (s *Supervisor) onWebhooksRequest(ctx context.Context, body []byte) {
	if s.hookJob == nil && len(s.hookQueue) == 0 && !s.commandBusy() {
		s.startWebhooks(ctx, body)
		return
	}
	if len(s.hookQueue) >= maxQueuedWebhookRequests {
		var rid *string
		req, err := model.ParseWebhooksRequest(body)
		var inv *model.WebhooksInvalidError
		switch {
		case err == nil:
			rid = req.RequestID
		case errors.As(err, &inv):
			rid = inv.RequestID
		}
		s.webhooksResult(model.WebhooksResult{RequestID: rid, Status: model.WebhooksFailed,
			Error: model.Ptr(model.WebhooksErrConflict), Detail: model.Ptr("too many queued requests")})
		return
	}
	s.hookQueue = append(s.hookQueue, body)
}

// startWebhooks checks a request (spec 5.9 order) and starts its job.
func (s *Supervisor) startWebhooks(ctx context.Context, body []byte) {
	fail := func(rid *string, class, detail string) {
		r := model.WebhooksResult{RequestID: rid, Status: model.WebhooksFailed, Error: &class}
		if detail != "" {
			r.Detail = &detail
		}
		s.webhooksResult(r)
	}
	req, err := model.ParseWebhooksRequest(body)
	var inv *model.WebhooksInvalidError
	if errors.As(err, &inv) {
		fail(inv.RequestID, model.WebhooksErrInvalid, inv.Detail)
		return
	}
	if s.mode != model.ModeLocal || s.bridge == nil {
		fail(req.RequestID, model.WebhooksErrOffline, "")
		return
	}
	// The checks against the list only mean something for the list the
	// client saw, so the revision comes first.
	if s.hookList == nil || req.Revision != s.hookList.Revision {
		r := model.WebhooksResult{RequestID: req.RequestID, Status: model.WebhooksFailed, Error: model.Ptr(model.WebhooksErrConflict),
			Detail: model.Ptr("the webhook list changed; retry with the new revision")}
		if s.readWebhooks(ctx) {
			r.Revision = model.Ptr(s.hookList.Revision)
		}
		s.webhooksResult(r)
		return
	}
	plan, err := planWebhooks(s.hooks, req, s.ownWebhookURL(), s.id)
	if errors.As(err, &inv) {
		fail(inv.RequestID, model.WebhooksErrInvalid, inv.Detail)
		return
	}
	if len(plan.ops) == 0 {
		s.webhooksResult(model.WebhooksResult{RequestID: req.RequestID, Status: model.WebhooksOK, Kept: plan.kept,
			Revision: model.Ptr(s.hookList.Revision)})
		return
	}
	before := map[int]bool{}
	for _, w := range s.hooks {
		before[int(w.ID)] = true
	}
	s.hookJob = &webhookJob{requestID: req.RequestID, plan: plan, before: before}
	s.stepWebhooks(ctx)
}

// stepWebhooks makes the next bridge call of the running request, or starts
// a queued one. It does nothing while a command is in progress.
func (s *Supervisor) stepWebhooks(ctx context.Context) {
	if !s.webhooksReady() || ctx.Err() != nil {
		return
	}
	j := s.hookJob
	if j == nil {
		body := s.hookQueue[0]
		s.hookQueue = s.hookQueue[1:]
		s.startWebhooks(ctx, body)
		return
	}
	if s.mode != model.ModeLocal || s.bridge == nil {
		s.finishWebhooks(ctx, model.WebhooksErrOffline, nil)
		return
	}
	op := j.plan.ops[j.next]
	c, cancel := s.reqCtx(ctx)
	var err error
	if op.delete {
		err = s.bridge.DeleteWebhook(c, op.id)
	} else {
		err = s.bridge.CreateWebhook(c, op.url, op.triggers)
	}
	cancel()
	if err != nil {
		s.log.Warn("changing the bridge webhooks failed", "webhook_id", op.id, "target", schemeHost(op.url), "err", err)
		s.finishWebhooks(ctx, webhookErrClass(err), err)
		return
	}
	if op.delete {
		j.removed = append(j.removed, op.id)
		s.log.Info("deleted a bridge webhook", "webhook_id", op.id, "target", schemeHost(op.url))
	} else {
		j.created = append(j.created, op.url)
		s.log.Info("created a bridge webhook", "target", schemeHost(op.url), "triggers", op.triggers.Names())
	}
	j.next++
	if j.next == len(j.plan.ops) {
		s.finishWebhooks(ctx, "", nil)
	}
}

// finishWebhooks ends the running request: the list is read again, the
// result published, and only then a failed call counts as a bridge failure.
func (s *Supervisor) finishWebhooks(ctx context.Context, class string, err error) {
	j := s.hookJob
	s.hookJob = nil
	r := model.WebhooksResult{RequestID: j.requestID, Status: model.WebhooksOK, Removed: j.removed, Kept: j.plan.kept}
	if class != "" {
		r.Status, r.Error = model.WebhooksFailed, &class
		if j.next > 0 {
			r.Status = model.WebhooksPartial
		}
	}
	if s.rereadWebhooks(ctx) == nil {
		r.Revision = model.Ptr(s.hookList.Revision)
		r.Added = addedIDs(s.hooks, j.before, j.created)
	}
	s.webhooksResult(r)
	if errors.Is(err, loqed.ErrUnreachable) || errors.Is(err, loqed.ErrNoResponse) || errors.Is(err, loqed.ErrUnauthorized) {
		s.httpFailure(ctx, err)
	}
}

// addedIDs matches created URLs to new ids in the list read afterwards; a URL
// that appears more than once there is reported once (its newest id).
func addedIDs(after []bridge.Webhook, before map[int]bool, created []string) []int {
	var ids []int
	for _, u := range slices.Compact(slices.Sorted(slices.Values(created))) {
		newest := -1
		for _, w := range after {
			if w.URL == u && !before[int(w.ID)] {
				newest = max(newest, int(w.ID))
			}
		}
		if newest >= 0 {
			ids = append(ids, newest)
		}
	}
	slices.Sort(ids)
	return ids
}

func (s *Supervisor) webhooksResult(r model.WebhooksResult) {
	if err := s.d.Publisher.PublishWebhooksResult(s.id, r); err != nil {
		s.log.Warn("publishing the SetWebhooks result failed", "err", err)
	}
}

// webhookErrClass maps a failed bridge write onto a result error class.
func webhookErrClass(err error) string {
	switch {
	case errors.Is(err, loqed.ErrUnreachable), errors.Is(err, errNoBridge):
		return model.FailUnreachable
	case errors.Is(err, loqed.ErrNoResponse), loqed.IsServerError(err),
		errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return model.FailNoResponse
	case errors.Is(err, loqed.ErrUnauthorized):
		return model.FailUnauthorized
	default:
		return model.FailRejected
	}
}

// schemeHost is what logs may show of a webhook URL.
func schemeHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Scheme + "://" + u.Host
}
