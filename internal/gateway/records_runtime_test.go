package gateway

import (
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

func TestStateCarriesTokenExpiry(t *testing.T) {
	exp := time.Date(2027, 4, 5, 19, 15, 26, 0, time.UTC)
	known := true
	h := newHarness(t, testRecord(), config.LockSetting{}, func(d *Deps) {
		d.TokenExpiry = func() (time.Time, bool) { return exp, known }
	})
	h.start()
	if got := h.state().TokenExpiresAt; got == nil || !got.Equal(exp) {
		t.Fatalf("token_expires_at %v", got)
	}
	known = false
	h.send(reached("STATE_CHANGED_LATCH", nil))
	if got := h.state().TokenExpiresAt; got != nil {
		t.Fatalf("unknown expiry must be omitted: %v", got)
	}
}

func TestRecordUpdateReplacesCredentials(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	built := h.bridgesBuilt
	fresh := testRecord()
	fresh.LocalID = ourKeyPtr(9) // a re-minted token comes with a new key slot
	h.send(RecordMsg{Record: fresh})
	if id := h.s.Record().LocalID; id == nil || *id != 9 {
		t.Fatalf("record %+v", h.s.Record())
	}
	if h.bridgesBuilt != built+1 || h.s.bridge == nil {
		t.Fatalf("the bridge client must be rebuilt with the new key: built %d", h.bridgesBuilt-built)
	}
}

func TestManagerUpdateRecordsSkipsUnknownLocks(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	m := NewManager([]*Supervisor{h.s})
	other := store.LockRecord{ID: "other"}
	m.UpdateRecords([]store.LockRecord{testRecord(), other})
	if len(h.s.in) != 1 {
		t.Fatalf("queued %d", len(h.s.in))
	}
}

func ourKeyPtr(v int) *int { return &v }
