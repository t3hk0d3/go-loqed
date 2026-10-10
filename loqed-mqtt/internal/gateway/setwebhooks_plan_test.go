package gateway

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/model"
)

// opsString renders planned ops as "-3", "+http://x (all)".
func opsString(p webhookPlan) []string {
	var out []string
	for _, op := range p.ops {
		if op.delete {
			out = append(out, fmt.Sprintf("-%d", op.id))
		} else {
			out = append(out, fmt.Sprintf("+%s %v", op.url, op.triggers.Names()))
		}
	}
	return out
}

func idSpec(id int, triggers ...bridge.Triggers) model.WebhookSpec {
	s := model.WebhookSpec{ID: &id}
	if len(triggers) > 0 {
		s.Triggers, s.HasTriggers = triggers[0], true
	}
	return s
}

func urlSpec(url string, t bridge.Triggers) model.WebhookSpec {
	return model.WebhookSpec{URL: &url, Triggers: t, HasTriggers: true}
}

var planCurrent = []bridge.Webhook{
	hook(3, "http://ha/api/webhook/abc", bridge.AllTriggers),
	hook(5, "https://hooks.nabu.casa/xyz", bridge.TriggerBattery|bridge.TriggerOnlineStatus),
	hook(7, ownURL, bridge.AllTriggers),
}

func TestPlanWebhooks(t *testing.T) {
	cases := []struct {
		name  string
		specs []model.WebhookSpec
		ops   []string
		kept  []int
	}{
		{"keep by id, remove the unlisted",
			[]model.WebhookSpec{idSpec(3)}, []string{"-5"}, []int{3, 7}},
		{"keep by id with the same triggers",
			[]model.WebhookSpec{idSpec(3, bridge.AllTriggers), idSpec(5, bridge.TriggerOnlineStatus|bridge.TriggerBattery)}, nil, []int{3, 5, 7}},
		{"re-create by id with other triggers",
			[]model.WebhookSpec{idSpec(5, bridge.AllTriggers), idSpec(3)}, []string{"-5", "+https://hooks.nabu.casa/xyz [all]"}, []int{3, 7}},
		{"keep by url with the same triggers",
			[]model.WebhookSpec{urlSpec("https://hooks.nabu.casa/xyz", bridge.TriggerBattery|bridge.TriggerOnlineStatus), idSpec(3)}, nil, []int{3, 5, 7}},
		{"re-create by url with other triggers",
			[]model.WebhookSpec{urlSpec("https://hooks.nabu.casa/xyz", bridge.TriggerBattery), idSpec(3)},
			[]string{"-5", "+https://hooks.nabu.casa/xyz [battery]"}, []int{3, 7}},
		{"add a new url, deletions first then creations in request order",
			[]model.WebhookSpec{urlSpec("http://new/b", bridge.AllTriggers), urlSpec("http://new/a", bridge.TriggerBattery)},
			[]string{"-3", "-5", "+http://new/b [all]", "+http://new/a [battery]"}, []int{7}},
		{"own webhook listed by id",
			[]model.WebhookSpec{idSpec(7), idSpec(3), idSpec(5)}, nil, []int{3, 5, 7}},
		{"own webhook listed by url with all triggers",
			[]model.WebhookSpec{urlSpec(ownURL, bridge.AllTriggers), idSpec(3), idSpec(5)}, nil, []int{3, 5, 7}},
		{"empty list keeps only the own webhook",
			nil, []string{"-3", "-5"}, []int{7}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := planWebhooks(planCurrent, model.WebhooksRequest{Revision: "r", Webhooks: tc.specs}, ownURL, "lock1")
			if err != nil {
				t.Fatal(err)
			}
			if got := opsString(p); !slices.Equal(got, tc.ops) {
				t.Errorf("ops %v, want %v", got, tc.ops)
			}
			if !slices.Equal(p.kept, tc.kept) {
				t.Errorf("kept %v, want %v", p.kept, tc.kept)
			}
		})
	}
}

func TestPlanWebhooksOwnURLWhileNotRegisteredChangesNothingForIt(t *testing.T) {
	current := []bridge.Webhook{hook(3, "http://ha/x", bridge.AllTriggers)}
	p, err := planWebhooks(current, model.WebhooksRequest{Webhooks: []model.WebhookSpec{urlSpec(ownURL, bridge.AllTriggers), idSpec(3)}}, ownURL, "lock1")
	if err != nil || len(p.ops) != 0 || !slices.Equal(p.kept, []int{3}) {
		t.Fatalf("ops %v kept %v err %v", opsString(p), p.kept, err)
	}
}

func TestPlanWebhooksRejects(t *testing.T) {
	cases := []struct {
		name  string
		specs []model.WebhookSpec
		entry string
	}{
		{"unknown id", []model.WebhookSpec{idSpec(3), idSpec(4)}, "webhooks[1]"},
		{"duplicate id", []model.WebhookSpec{idSpec(3), idSpec(3)}, "webhooks[1]"},
		{"duplicate url", []model.WebhookSpec{urlSpec("http://n/", bridge.AllTriggers), urlSpec("http://n/", bridge.TriggerBattery)}, "webhooks[1]"},
		{"url of a webhook kept by id", []model.WebhookSpec{urlSpec("http://ha/api/webhook/abc", bridge.AllTriggers), idSpec(3)}, "webhooks[0]"},
		{"foreign url on this lock's gateway path", []model.WebhookSpec{urlSpec("http://10.9.9.9:8099/webhook/lock1", bridge.AllTriggers)}, "webhooks[0]"},
		{"own webhook by id with fewer triggers", []model.WebhookSpec{idSpec(7, bridge.TriggerBattery)}, "webhooks[0]"},
		{"own webhook by url with fewer triggers", []model.WebhookSpec{idSpec(3), urlSpec(ownURL, bridge.TriggerBattery)}, "webhooks[1]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rid := "r1"
			_, err := planWebhooks(planCurrent, model.WebhooksRequest{RequestID: &rid, Webhooks: tc.specs}, ownURL, "lock1")
			var inv *model.WebhooksInvalidError
			if !errors.As(err, &inv) {
				t.Fatalf("err = %v, want invalid", err)
			}
			if !strings.HasPrefix(inv.Detail, tc.entry) || strings.Contains(inv.Detail, "://") {
				t.Errorf("detail %q: want it to start with %s and hold no URL", inv.Detail, tc.entry)
			}
			if inv.RequestID == nil || *inv.RequestID != "r1" {
				t.Errorf("request id %v not kept", inv.RequestID)
			}
		})
	}
}

func TestPlanWebhooksWithoutOwnURLRejectsEveryGatewayPathForThisLock(t *testing.T) {
	_, err := planWebhooks(planCurrent, model.WebhooksRequest{Webhooks: []model.WebhookSpec{urlSpec(ownURL, bridge.AllTriggers)}}, "", "lock1")
	var inv *model.WebhooksInvalidError
	if !errors.As(err, &inv) {
		t.Fatalf("err = %v, want invalid", err)
	}
}
