package gateway

import (
	"fmt"
	"net/url"
	"slices"

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
