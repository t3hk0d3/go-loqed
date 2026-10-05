package gateway

import (
	"slices"

	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

// SettingFor finds lock_settings for a lock by id, then by name.
func SettingFor(settings config.LockSettingsMap, rec store.LockRecord) config.LockSetting {
	if s, ok := settings[rec.ID]; ok {
		return s
	}
	if s, ok := settings[rec.Name]; ok {
		return s
	}
	return config.LockSetting{}
}

// ApplySetting overlays manual values onto cloud data.
func ApplySetting(rec store.LockRecord, s config.LockSetting) store.LockRecord {
	if s.BridgeIP != "" {
		rec.BridgeIP = s.BridgeIP
	}
	if s.BridgeKey != "" {
		rec.BridgeKey = s.BridgeKey
	}
	if s.KeySecret != "" {
		rec.KeySecret = s.KeySecret
	}
	if s.LocalID != nil {
		v := *s.LocalID
		rec.LocalID = &v
	}
	return rec
}

// KeysPinned: lock_settings overrides the bridge keys, so a cloud refresh
// cannot fix an auth failure.
func KeysPinned(s config.LockSetting) bool {
	return s.BridgeKey != "" || s.KeySecret != "" || s.LocalID != nil
}

// IPPinned: lock_settings overrides the bridge IP, so a cloud refresh
// cannot fix an unreachable bridge.
func IPPinned(s config.LockSetting) bool { return s.BridgeIP != "" }

// Select applies the allow-list (ids or names, in allow-list order).
// An empty allow-list selects everything.
func Select(records []store.LockRecord, allow []string) (selected []store.LockRecord, missing []string) {
	if len(allow) == 0 {
		return append([]store.LockRecord(nil), records...), nil
	}
	seen := map[string]bool{}
	for _, a := range allow {
		found := false
		for _, r := range records {
			if r.ID == a || r.Name == a {
				found = true
				if !seen[r.ID] {
					seen[r.ID] = true
					selected = append(selected, r)
				}
			}
		}
		if !found {
			missing = append(missing, a)
		}
	}
	return selected, missing
}

// UnmatchedSettings lists lock_settings keys that match no lock id or name
// (logged as warnings).
func UnmatchedSettings(settings config.LockSettingsMap, records []store.LockRecord) []string {
	var out []string
	for key := range settings {
		found := false
		for _, r := range records {
			if r.ID == key || r.Name == key {
				found = true
				break
			}
		}
		if !found {
			out = append(out, key)
		}
	}
	slices.Sort(out)
	return out
}
