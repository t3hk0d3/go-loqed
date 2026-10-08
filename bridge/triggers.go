package bridge

import (
	"encoding/json"
	"errors"
	"fmt"

	loqed "github.com/t3hk0d3/go-loqed"
)

// triggerNames are the bridge's trigger_* field names without the prefix,
// in bit order.
var triggerNames = [...]string{
	"state_changed_open",
	"state_changed_latch",
	"state_changed_night_lock",
	"state_changed_unknown",
	"state_goto_open",
	"state_goto_latch",
	"state_goto_night_lock",
	"battery",
	"online_status",
}

// TriggersAll is the name that stands for AllTriggers.
const TriggersAll = "all"

// ParseTriggers turns trigger names (or "all") into a bitmap. Duplicates are
// allowed; an unknown name or an empty list is an error.
func ParseTriggers(names []string) (Triggers, error) {
	if len(names) == 0 {
		return 0, errors.New("bridge: no triggers given")
	}
	var t Triggers
names:
	for _, n := range names {
		if n == TriggersAll {
			t |= AllTriggers
			continue
		}
		for i, tn := range triggerNames {
			if n == tn {
				t |= 1 << i
				continue names
			}
		}
		return 0, fmt.Errorf("bridge: unknown trigger %q", n)
	}
	return t, nil
}

// Names returns ["all"] for AllTriggers, else the set trigger names in bit
// order. Bits above the known triggers are ignored.
func (t Triggers) Names() []string {
	t &= AllTriggers
	if t == AllTriggers {
		return []string{TriggersAll}
	}
	names := []string{}
	for i, n := range triggerNames {
		if t&(1<<i) != 0 {
			names = append(names, n)
		}
	}
	return names
}

// UnmarshalJSON decodes a GET /webhooks entry; trigger_* flags may be numbers
// or numeric strings, and absent flags are 0.
func (w *Webhook) UnmarshalJSON(b []byte) error {
	var raw struct {
		ID  loqed.Int `json:"id"`
		URL string    `json:"url"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	var flags map[string]json.RawMessage
	if err := json.Unmarshal(b, &flags); err != nil {
		return err
	}
	var t Triggers
	for i, n := range triggerNames {
		v, ok := flags["trigger_"+n]
		if !ok {
			continue
		}
		var f loqed.Int
		if err := json.Unmarshal(v, &f); err != nil {
			return fmt.Errorf("trigger_%s: %w", n, err)
		}
		if f != 0 {
			t |= 1 << i
		}
	}
	*w = Webhook{ID: raw.ID, URL: raw.URL, Triggers: t}
	return nil
}
