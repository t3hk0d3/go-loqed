package gateway

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/config"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/model"
)

const (
	haURL   = "http://192.168.2.10:8123/api/webhook/abc"
	nabuURL = "https://hooks.nabu.casa/xyz"
)

// webhooksHarness is a local-mode lock whose bridge has two foreign
// webhooks (3, 5) and the gateway's own (7).
func webhooksHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.hooks = []bridge.Webhook{hook(3, haURL, bridge.AllTriggers),
		hook(5, nabuURL, bridge.TriggerBattery|bridge.TriggerOnlineStatus), hook(7, ownURL, bridge.AllTriggers)}
	h.start()
	h.bridge.calls = nil
	return h
}

// setWebhooks sends a request whose "revision" is filled in with the
// published one unless the body sets it.
func (h *harness) setWebhooks(body string) {
	if !strings.Contains(body, `"revision"`) {
		body = strings.Replace(body, "{", `{"revision":"`+h.lastList().Revision+`",`, 1)
	}
	h.send(WebhooksRequestMsg{Body: []byte(body)})
}

// finishWebhooks ticks until no SetWebhooks request is running or waiting.
func (h *harness) finishWebhooks() {
	h.t.Helper()
	for range 60 {
		if h.s.hookJob == nil && len(h.s.hookQueue) == 0 {
			return
		}
		h.advance(time.Second)
	}
	h.t.Fatal("SetWebhooks request did not finish")
}

func (h *harness) lastResult() model.WebhooksResult {
	h.t.Helper()
	if len(h.pub.results) == 0 {
		h.t.Fatal("no SetWebhooks result published")
	}
	return h.pub.results[len(h.pub.results)-1]
}

func writes(calls []string) []string {
	return slices.DeleteFunc(slices.Clone(calls), func(c string) bool { return c == "list" })
}

func errStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func TestSetWebhooksAppliesTheRequest(t *testing.T) {
	h := webhooksHarness(t)
	h.setWebhooks(`{"request_id":"cleanup-1","webhooks":[{"id":3},{"url":"http://192.168.2.11:8123/api/webhook/def"}]}`)
	h.finishWebhooks()
	if got, want := writes(h.bridge.calls), []string{"delete 5", "create http://192.168.2.11:8123/api/webhook/def"}; !slices.Equal(got, want) {
		t.Fatalf("bridge writes %v, want %v", got, want)
	}
	r := h.lastResult()
	if errStr(r.RequestID) != "cleanup-1" || r.Status != model.WebhooksOK || r.Error != nil ||
		!slices.Equal(r.Removed, []int{5}) || !slices.Equal(r.Added, []int{100}) || !slices.Equal(r.Kept, []int{3, 7}) {
		t.Fatalf("result %+v", r)
	}
	l := h.lastList()
	if !slices.Equal(listIDs(l), []int{3, 7, 100}) || errStr(r.Revision) != l.Revision || l.Revision != webhookRevision(h.bridge.hooks) {
		t.Fatalf("list %v revision %q, result revision %q", listIDs(l), l.Revision, errStr(r.Revision))
	}
}

func TestSetWebhooksMakesOneBridgeCallPerStep(t *testing.T) {
	h := webhooksHarness(t)
	h.setWebhooks(`{"webhooks":[{"url":"http://a/1"},{"url":"http://a/2"}]}`)
	if got := len(writes(h.bridge.calls)); got != 1 {
		t.Fatalf("%d bridge writes before the first step ended", got)
	}
	h.advance(time.Second)
	if got := len(writes(h.bridge.calls)); got != 2 {
		t.Fatalf("%d bridge writes after one more step", got)
	}
	h.finishWebhooks()
	if got := writes(h.bridge.calls); !slices.Equal(got, []string{"delete 3", "delete 5", "create http://a/1", "create http://a/2"}) {
		t.Fatalf("writes %v", got)
	}
}

func TestSetWebhooksRecreatesOnChangedTriggers(t *testing.T) {
	h := webhooksHarness(t)
	h.setWebhooks(`{"webhooks":[{"id":3},{"id":5,"triggers":["all"]}]}`)
	h.finishWebhooks()
	if got := writes(h.bridge.calls); !slices.Equal(got, []string{"delete 5", "create " + nabuURL}) {
		t.Fatalf("writes %v", got)
	}
	r := h.lastResult()
	if r.Status != model.WebhooksOK || !slices.Equal(r.Removed, []int{5}) || !slices.Equal(r.Added, []int{100}) {
		t.Fatalf("result %+v", r)
	}
	if got := h.bridge.hooks[len(h.bridge.hooks)-1].Triggers; got != bridge.AllTriggers {
		t.Fatalf("re-created with triggers %v", got)
	}
}

func TestSetWebhooksWithNothingToChangeWritesNothing(t *testing.T) {
	h := webhooksHarness(t)
	h.setWebhooks(`{"webhooks":[{"id":3},{"id":5},{"id":7}]}`)
	h.finishWebhooks()
	r := h.lastResult()
	if len(writes(h.bridge.calls)) != 0 || r.Status != model.WebhooksOK || !slices.Equal(r.Kept, []int{3, 5, 7}) || errStr(r.Revision) != h.lastList().Revision {
		t.Fatalf("writes %v result %+v", h.bridge.calls, r)
	}
}

func TestInvalidSetWebhooksTouchesNoBridge(t *testing.T) {
	for name, body := range map[string]string{
		"not json":   `LOCK`,
		"unknown id": `{"webhooks":[{"id":4}]}`,
		"structure":  `{"webhooks":[{"id":3,"url":"http://a/"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			h := webhooksHarness(t)
			lists := len(h.pub.lists)
			h.setWebhooks(body)
			h.finishWebhooks()
			r := h.lastResult()
			if len(h.bridge.calls) != 0 || len(h.pub.lists) != lists {
				t.Fatalf("bridge calls %v, lists %d→%d", h.bridge.calls, lists, len(h.pub.lists))
			}
			if r.Status != model.WebhooksFailed || errStr(r.Error) != model.WebhooksErrInvalid || r.Detail == nil {
				t.Fatalf("result %+v", r)
			}
		})
	}
}

func TestSetWebhooksWithAnOldRevisionConflicts(t *testing.T) {
	h := webhooksHarness(t)
	h.bridge.hooks = append(h.bridge.hooks, hook(9, "http://other/", bridge.AllTriggers)) // changed meanwhile
	h.setWebhooks(`{"revision":"0000000000000000","webhooks":[{"id":3}]}`)
	h.finishWebhooks()
	r := h.lastResult()
	if len(writes(h.bridge.calls)) != 0 || r.Status != model.WebhooksFailed || errStr(r.Error) != model.WebhooksErrConflict {
		t.Fatalf("writes %v result %+v", h.bridge.calls, r)
	}
	if !slices.Equal(listIDs(h.lastList()), []int{3, 5, 7, 9}) || errStr(r.Revision) != h.lastList().Revision {
		t.Fatalf("list not re-read: %v", listIDs(h.lastList()))
	}
}

func TestSetWebhooksOutsideLocalIsOffline(t *testing.T) {
	h := webhooksHarness(t)
	rev := h.lastList().Revision
	h.toCloud()
	h.bridge.calls = nil
	h.setWebhooks(`{"revision":"` + rev + `","webhooks":[{"id":3}]}`)
	h.finishWebhooks()
	r := h.lastResult()
	if len(h.bridge.calls) != 0 || r.Status != model.WebhooksFailed || errStr(r.Error) != model.WebhooksErrOffline {
		t.Fatalf("calls %v result %+v", h.bridge.calls, r)
	}
}

func TestSetWebhooksStopsAtTheFirstFailingCall(t *testing.T) {
	h := webhooksHarness(t)
	h.bridge.createErrs = []error{&loqed.APIError{StatusCode: 400}}
	h.setWebhooks(`{"webhooks":[{"id":3},{"id":5,"triggers":["all"]},{"url":"http://a/1"}]}`)
	h.finishWebhooks()
	if got := writes(h.bridge.calls); !slices.Equal(got, []string{"delete 5", "create " + nabuURL}) {
		t.Fatalf("writes %v; nothing after the failing call", got)
	}
	r := h.lastResult()
	if r.Status != model.WebhooksPartial || errStr(r.Error) != model.FailRejected || !slices.Equal(r.Removed, []int{5}) || len(r.Added) != 0 {
		t.Fatalf("result %+v", r)
	}
	if !slices.Equal(listIDs(h.lastList()), []int{3, 7}) || errStr(r.Revision) != h.lastList().Revision {
		t.Fatalf("list %v not republished", listIDs(h.lastList()))
	}
}

func TestSetWebhooksFailingFirstCallIsFailed(t *testing.T) {
	h := webhooksHarness(t)
	h.bridge.deleteErrs = []error{loqed.ErrUnreachable}
	h.setWebhooks(`{"webhooks":[{"id":3}]}`)
	h.finishWebhooks()
	r := h.lastResult()
	if r.Status != model.WebhooksFailed || errStr(r.Error) != model.FailUnreachable || len(r.Removed) != 0 {
		t.Fatalf("result %+v", r)
	}
}

func TestSetWebhooksWithAFailedReReadHasNoRevision(t *testing.T) {
	h := webhooksHarness(t)
	h.bridge.deleteErrs = []error{loqed.ErrNoResponse}
	h.bridge.listErr = loqed.ErrNoResponse
	h.setWebhooks(`{"webhooks":[{"id":3}]}`)
	h.finishWebhooks()
	r := h.lastResult()
	if r.Revision != nil || errStr(r.Error) != model.FailNoResponse {
		t.Fatalf("result %+v", r)
	}
}

func TestSetWebhooksUnauthorizedRefreshesAfterTheResult(t *testing.T) {
	h := webhooksHarness(t)
	h.bridge.deleteErrs = []error{loqed.ErrUnauthorized}
	h.setWebhooks(`{"webhooks":[{"id":3}]}`)
	h.finishWebhooks()
	if errStr(h.lastResult().Error) != model.FailUnauthorized || !slices.Equal(h.refreshes, []Reason{ReasonUnauthorized}) {
		t.Fatalf("result %+v refreshes %v", h.lastResult(), h.refreshes)
	}
}

func TestCommandGoesBeforeTheNextWebhookCall(t *testing.T) {
	h := webhooksHarness(t)
	h.setWebhooks(`{"webhooks":[{"url":"http://a/1"},{"url":"http://a/2"}]}`) // delete 3 runs now
	h.command(model.CommandLock)
	h.advance(time.Second)
	h.advance(time.Second)
	if got := writes(h.bridge.calls); !slices.Equal(got, []string{"delete 3", "command"}) {
		t.Fatalf("calls %v; the job must wait for the command", got)
	}
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(1)))
	h.finishWebhooks()
	if got := writes(h.bridge.calls); !slices.Equal(got, []string{"delete 3", "command", "delete 5", "create http://a/1", "create http://a/2"}) {
		t.Fatalf("calls %v", got)
	}
}

func TestSetWebhooksWaitsForACommandInProgress(t *testing.T) {
	h := webhooksHarness(t)
	h.command(model.CommandLock)
	h.setWebhooks(`{"webhooks":[{"id":3},{"id":7}]}`)
	h.advance(time.Second)
	if got := writes(h.bridge.calls); !slices.Equal(got, []string{"command"}) {
		t.Fatalf("calls %v", got)
	}
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(1)))
	h.finishWebhooks()
	if got := writes(h.bridge.calls); !slices.Equal(got, []string{"command", "delete 5"}) {
		t.Fatalf("calls %v", got)
	}
}

func TestSetWebhooksEndsOfflineWhenTheLockLeftLocal(t *testing.T) {
	h := webhooksHarness(t)
	h.setWebhooks(`{"webhooks":[{"url":"http://a/1"}]}`) // delete 3 runs now
	h.s.setMode(model.ModeCloud)
	h.finishWebhooks()
	r := h.lastResult()
	if r.Status != model.WebhooksPartial || errStr(r.Error) != model.WebhooksErrOffline || !slices.Equal(r.Removed, []int{3}) {
		t.Fatalf("result %+v", r)
	}
}

func TestQueuedRequestBehindAChangeConflicts(t *testing.T) {
	h := webhooksHarness(t)
	rev := h.lastList().Revision
	h.setWebhooks(`{"request_id":"a","webhooks":[{"id":3},{"url":"http://a/1"}]}`)
	if h.s.hookJob == nil {
		t.Fatal("first request already finished; the second would not be queued")
	}
	h.setWebhooks(`{"revision":"` + rev + `","request_id":"b","webhooks":[{"id":3},{"id":5}]}`)
	h.finishWebhooks()
	if len(h.pub.results) != 2 {
		t.Fatalf("%d results", len(h.pub.results))
	}
	a, b := h.pub.results[0], h.pub.results[1]
	if errStr(a.RequestID) != "a" || a.Status != model.WebhooksOK || errStr(b.RequestID) != "b" || errStr(b.Error) != model.WebhooksErrConflict {
		t.Fatalf("results %+v / %+v", a, b)
	}
}

func TestTooManyQueuedRequestsConflictAtOnce(t *testing.T) {
	h := webhooksHarness(t)
	h.setWebhooks(`{"webhooks":[{"url":"http://a/1"}]}`)
	for i := range maxQueuedWebhookRequests + 1 {
		h.setWebhooks(fmt.Sprintf(`{"request_id":"q%d","webhooks":[{"id":3}]}`, i))
	}
	if len(h.pub.results) != 1 {
		t.Fatalf("%d immediate results, want 1", len(h.pub.results))
	}
	r := h.lastResult()
	if errStr(r.RequestID) != fmt.Sprintf("q%d", maxQueuedWebhookRequests) || errStr(r.Error) != model.WebhooksErrConflict ||
		!strings.Contains(errStr(r.Detail), "queued") {
		t.Fatalf("result %+v", r)
	}
}

func TestRegistrationCheckWaitsForARunningRequest(t *testing.T) {
	h := webhooksHarness(t)
	h.setWebhooks(`{"webhooks":[{"url":"http://a/1"},{"url":"http://a/2"}]}`) // 4 calls, 1 made
	h.s.nextReconcile = h.now                                                 // registration check due
	h.advance(time.Second)
	if h.s.hookJob == nil || slices.Contains(h.bridge.calls, "list") {
		t.Fatalf("calls %v: the registration check read the list while the request ran", h.bridge.calls)
	}
	h.finishWebhooks()
	h.s.nextReconcile = h.now
	h.advance(time.Second)
	if n := len(slices.DeleteFunc(slices.Clone(h.bridge.calls), func(c string) bool { return c != "list" })); n != 2 {
		t.Fatalf("calls %v: want the re-read after the request and the registration check after it", h.bridge.calls)
	}
}

func TestAddedURLAppearingTwiceIsReportedOnce(t *testing.T) {
	h := webhooksHarness(t)
	h.setWebhooks(`{"webhooks":[{"id":3},{"id":5},{"url":"http://a/1"}]}`)
	h.bridge.hooks = append(h.bridge.hooks, hook(50, "http://a/1", bridge.AllTriggers)) // a concurrent duplicate
	h.finishWebhooks()
	if r := h.lastResult(); len(r.Added) != 1 {
		t.Fatalf("added %v", r.Added)
	}
}

func TestSetWebhooksLogsOnlySchemeAndHost(t *testing.T) {
	var buf bytes.Buffer
	h := newHarness(t, testRecord(), config.LockSetting{}, func(d *Deps) { d.Log = slog.New(slog.NewTextHandler(&buf, nil)) })
	h.bridge.hooks = []bridge.Webhook{hook(3, haURL, bridge.AllTriggers), hook(7, ownURL, bridge.AllTriggers)}
	h.start()
	h.setWebhooks(`{"webhooks":[{"url":"https://new.example/secret/path?token=x"}]}`)
	h.finishWebhooks()
	out := buf.String()
	if strings.Contains(out, "/secret") || strings.Contains(out, "token=x") || strings.Contains(out, "/api/webhook/abc") {
		t.Fatalf("log leaks a webhook path:\n%s", out)
	}
	if !strings.Contains(out, "https://new.example") || !strings.Contains(out, "http://192.168.2.10:8123") {
		t.Fatalf("log lacks the scheme and host:\n%s", out)
	}
}

func TestDeliverWebhooksRequestRoutesByLock(t *testing.T) {
	h := webhooksHarness(t)
	m := NewManager([]*Supervisor{h.s})
	if err := m.DeliverWebhooksRequest("lock1", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if msg := <-h.s.in; !bytes.Equal(msg.(WebhooksRequestMsg).Body, []byte(`{}`)) {
		t.Fatalf("got %+v", msg)
	}
	if err := m.DeliverWebhooksRequest("nope", nil); !errors.Is(err, ErrUnknownLock) {
		t.Fatalf("err %v", err)
	}
}

func TestConflictWithAFailedReReadGivesNoRevision(t *testing.T) {
	h := webhooksHarness(t)
	h.bridge.listErr = loqed.ErrNoResponse
	h.setWebhooks(`{"revision":"0000000000000000","webhooks":[{"id":3}]}`)
	if r := h.lastResult(); errStr(r.Error) != model.WebhooksErrConflict || r.Revision != nil {
		t.Fatalf("result %+v: a stale revision would only conflict again", r)
	}
}
