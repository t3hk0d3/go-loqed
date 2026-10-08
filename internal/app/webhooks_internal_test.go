package app

import (
	"errors"
	"testing"

	"github.com/t3hk0d3/go-loqed/internal/gateway"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func TestUndeliveredWebhooksRequestGetsAFailedResult(t *testing.T) {
	cases := []struct {
		err   error
		class string
	}{
		{gateway.ErrBusy, model.WebhooksErrConflict},
		{gateway.ErrUnknownLock, model.WebhooksErrOffline},
		{errors.New("other"), model.WebhooksErrOffline},
	}
	for _, tc := range cases {
		r := undeliveredWebhooksResult([]byte(`{"revision":"a","request_id":"r1","webhooks":[]}`), tc.err)
		if r.Status != model.WebhooksFailed || r.Error == nil || *r.Error != tc.class || r.Detail == nil ||
			r.RequestID == nil || *r.RequestID != "r1" {
			t.Errorf("%v: result %+v", tc.err, r)
		}
	}
}
