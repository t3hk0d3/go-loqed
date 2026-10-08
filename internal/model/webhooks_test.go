package model_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func TestParseWebhooksRequestAcceptsEveryEntryForm(t *testing.T) {
	req, err := model.ParseWebhooksRequest([]byte(`{"revision":"abc","request_id":"r1","webhooks":[
		{"id":3},
		{"id":5,"triggers":["battery","online_status"]},
		{"url":"https://hooks.example/x"},
		{"url":"http://192.168.2.11:8123/api/webhook/def","triggers":["all"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.Revision != "abc" || req.RequestID == nil || *req.RequestID != "r1" || len(req.Webhooks) != 4 {
		t.Fatalf("got %+v", req)
	}
	w := req.Webhooks
	if w[0].ID == nil || *w[0].ID != 3 || w[0].URL != nil || w[0].HasTriggers {
		t.Errorf("entry 0 = %+v", w[0])
	}
	if w[1].ID == nil || *w[1].ID != 5 || !w[1].HasTriggers || w[1].Triggers != bridge.TriggerBattery|bridge.TriggerOnlineStatus {
		t.Errorf("entry 1 = %+v", w[1])
	}
	if w[2].URL == nil || *w[2].URL != "https://hooks.example/x" || w[2].Triggers != bridge.AllTriggers {
		t.Errorf("entry 2 = %+v (url entries default to all triggers)", w[2])
	}
	if w[3].Triggers != bridge.AllTriggers {
		t.Errorf("entry 3 = %+v", w[3])
	}
}

func TestParseWebhooksRequestWithoutRequestID(t *testing.T) {
	req, err := model.ParseWebhooksRequest([]byte(`{"revision":"abc","webhooks":[]}`))
	if err != nil || req.RequestID != nil || len(req.Webhooks) != 0 {
		t.Fatalf("got %+v, %v", req, err)
	}
}

func TestParseWebhooksRequestRejects(t *testing.T) {
	long := `"http://h/` + strings.Repeat("a", model.MaxWebhookURLLen) + `"`
	many := strings.TrimSuffix(strings.Repeat(`{"id":1},`, model.MaxWebhooksEntries+1), ",")
	cases := []struct {
		name, payload, detail string
		keepsID               bool
	}{
		{"oversized", `{"revision":"a","request_id":"r","webhooks":[],"pad":"` + strings.Repeat("x", model.MaxWebhooksPayload) + `"}`, "too large", false},
		{"garbage", `LOCK`, "JSON object", false},
		{"array", `[1,2]`, "JSON object", false},
		{"missing revision", `{"request_id":"r","webhooks":[]}`, "revision", true},
		{"missing webhooks", `{"request_id":"r","revision":"a"}`, "webhooks", true},
		{"too many", `{"request_id":"r","revision":"a","webhooks":[` + many + `]}`, "at most 20", true},
		{"both id and url", `{"request_id":"r","revision":"a","webhooks":[{"id":1,"url":"http://h/"}]}`, "webhooks[0]", true},
		{"neither id nor url", `{"request_id":"r","revision":"a","webhooks":[{"id":1},{"triggers":["all"]}]}`, "webhooks[1]", true},
		{"unknown key", `{"request_id":"r","revision":"a","webhooks":[{"id":1,"name":"x"}]}`, "webhooks[0]", true},
		{"id not integer", `{"request_id":"r","revision":"a","webhooks":[{"id":"1"}]}`, "webhooks[0]", true},
		{"empty triggers", `{"request_id":"r","revision":"a","webhooks":[{"id":1,"triggers":[]}]}`, "webhooks[0]", true},
		{"unknown trigger", `{"request_id":"r","revision":"a","webhooks":[{"id":1,"triggers":["doorbell"]}]}`, "webhooks[0]", true},
		{"relative url", `{"request_id":"r","revision":"a","webhooks":[{"url":"/api/webhook/x"}]}`, "webhooks[0]", true},
		{"ftp url", `{"request_id":"r","revision":"a","webhooks":[{"url":"ftp://h/x"}]}`, "webhooks[0]", true},
		{"no host", `{"request_id":"r","revision":"a","webhooks":[{"url":"http:///x"}]}`, "webhooks[0]", true},
		{"url too long", `{"request_id":"r","revision":"a","webhooks":[{"url":` + long + `}]}`, "webhooks[0]", true},
		{"request_id too long", `{"request_id":"` + strings.Repeat("r", model.MaxCommandIDLen+1) + `","revision":"a","webhooks":[]}`, "request_id", false},
		{"request_id not printable", `{"request_id":"a\nb","revision":"a","webhooks":[]}`, "request_id", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := model.ParseWebhooksRequest([]byte(tc.payload))
			var inv *model.WebhooksInvalidError
			if !errors.As(err, &inv) {
				t.Fatalf("err = %v, want *WebhooksInvalidError", err)
			}
			if !strings.Contains(inv.Detail, tc.detail) {
				t.Errorf("detail %q does not mention %q", inv.Detail, tc.detail)
			}
			if strings.Contains(inv.Detail, "://") || strings.Contains(inv.Detail, "/x") {
				t.Errorf("detail %q contains a URL", inv.Detail)
			}
			if tc.keepsID != (inv.RequestID != nil && *inv.RequestID == "r") {
				t.Errorf("request id = %v, want kept=%v", inv.RequestID, tc.keepsID)
			}
		})
	}
}

func TestWebhooksResultEncodesEmptyListsAndNulls(t *testing.T) {
	b, err := json.Marshal(model.WebhooksResult{Status: model.WebhooksFailed, Error: model.Ptr(model.WebhooksErrOffline)})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"request_id":null,"status":"failed","error":"offline","detail":null,"removed":[],"added":[],"kept":[],"revision":null}`
	if string(b) != want {
		t.Fatalf("got  %s\nwant %s", b, want)
	}
}

func TestWebhookListEncoding(t *testing.T) {
	l := model.WebhookList{Revision: "9f2c41d0a1b2c3d4", FetchedAt: time.Date(2026, 10, 8, 8, 0, 0, 123e6, time.FixedZone("x", 3600)),
		Count: 1, Webhooks: []model.WebhookEntry{{ID: 7, URL: "http://g:8099/webhook/L", Triggers: []string{"all"}, Gateway: true}}}
	b, err := json.Marshal(l)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"revision":"9f2c41d0a1b2c3d4","fetched_at":"2026-10-08T07:00:00Z","count":1,` +
		`"webhooks":[{"id":7,"url":"http://g:8099/webhook/L","triggers":["all"],"gateway":true}]}`
	if string(b) != want {
		t.Fatalf("got  %s\nwant %s", b, want)
	}
}
