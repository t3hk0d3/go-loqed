package store_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/store"
)

func TestOpenMissingThenRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "locks.json")
	st, status, err := store.Open(path)
	if err != nil || status != store.StatusMissing {
		t.Fatalf("status %v err %v", status, err)
	}
	id := 1
	err = st.Update(func(c *store.Cache) {
		c.TokenSHA256 = store.TokenHash("tok")
		c.Minted = &store.MintedToken{ID: "t1", Value: "secret"}
		c.FetchedAt = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
		c.Locks = []store.LockRecord{{ID: "lock1", Name: "Front door", BridgeIP: "192.168.1.50", LocalID: &id, KeySecret: "a", BridgeKey: "b"}}
	})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", info.Mode().Perm())
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Fatalf("temp files left behind: %v", entries)
	}

	again, status, err := store.Open(path)
	if err != nil || status != store.StatusLoaded {
		t.Fatalf("status %v err %v", status, err)
	}
	c := again.Snapshot()
	if c.Version != store.Version || c.Minted.Value != "secret" || c.TokenSHA256 != store.TokenHash("tok") || len(c.Locks) != 1 {
		t.Fatalf("got %+v", c)
	}
	if r, ok := c.Find("Front door"); !ok || r.ID != "lock1" || !r.HasLocalCredentials() {
		t.Fatalf("find by name: %+v %v", r, ok)
	}
	if _, ok := c.Find("lock1"); !ok {
		t.Fatal("find by id")
	}
}

func TestOpenCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locks.json")
	_ = os.WriteFile(path, []byte("{not json"), 0o600)
	st, status, err := store.Open(path)
	if err != nil || status != store.StatusCorrupt || len(st.Snapshot().Locks) != 0 {
		t.Fatalf("status %v err %v", status, err)
	}
	_ = os.WriteFile(path, []byte(`{"version":99}`), 0o600)
	if _, status, _ := store.Open(path); status != store.StatusUnknownVersion {
		t.Fatalf("unknown version: got %v", status)
	}
}

func TestBudgetPublishedIDsAndMintPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locks.json")
	st, _, _ := store.Open(path)
	t0 := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	err := st.Update(func(c *store.Cache) {
		c.Budget = store.BudgetState{Calls: []time.Time{t0, t0.Add(time.Minute)}, BlockedUntil: t0.Add(12 * time.Hour)}
		c.PublishedIDs = []string{"lock1", "lock2"}
		c.Minted = &store.MintedToken{ID: "t1", Value: "v", EmailSHA256: store.EmailHash(" Me@Example.com "), MintedAt: t0}
		c.LastMintAt = t0
	})
	if err != nil {
		t.Fatal(err)
	}
	again, _, _ := store.Open(path)
	c := again.Snapshot()
	if len(c.Budget.Calls) != 2 || !c.Budget.BlockedUntil.Equal(t0.Add(12*time.Hour)) || len(c.PublishedIDs) != 2 ||
		c.Minted.EmailSHA256 != store.EmailHash("me@example.com") || !c.LastMintAt.Equal(t0) {
		t.Fatalf("got %+v", c)
	}
	snap := again.Snapshot()
	snap.Budget.Calls[0] = time.Time{}
	snap.PublishedIDs[0] = "x"
	if c2 := again.Snapshot(); c2.Budget.Calls[0].IsZero() || c2.PublishedIDs[0] != "lock1" {
		t.Fatal("snapshot shares slices with the store")
	}
}

func TestInstallIDIsStable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locks.json")
	st, _, _ := store.Open(path)
	id, err := st.InstallID()
	if err != nil || len(id) != 8 {
		t.Fatalf("id %q err %v", id, err)
	}
	again, _, _ := store.Open(path)
	if id2, _ := again.InstallID(); id2 != id {
		t.Fatalf("install id changed: %q → %q", id, id2)
	}
}

func TestUpdateWriteFailureKeepsMemoryAndWrapsErrWrite(t *testing.T) {
	dir := t.TempDir()
	st, _, _ := store.Open(filepath.Join(dir, "locks.json"))
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	err := st.Update(func(c *store.Cache) { c.CloudSecret = "s" })
	if !errors.Is(err, store.ErrWrite) {
		t.Fatalf("got %v", err)
	}
	if st.Snapshot().CloudSecret != "s" {
		t.Fatal("in-memory update lost")
	}
}

func TestMergeKeepsLocalCredentials(t *testing.T) {
	id := 3
	old := store.LockRecord{ID: "a", Name: "Old", BridgeIP: "192.0.2.1", KeySecret: "k", BridgeKey: "b", LocalID: &id}
	old.CloudWebhookID = "6148"
	merged := store.Merge(old, store.LockRecord{ID: "a", Name: "New"})
	if merged.Name != "New" || !store.SameLocal(merged, old) || merged.CloudWebhookID != "6148" {
		t.Fatalf("got %+v", merged)
	}
	changed := store.Merge(old, store.LockRecord{ID: "a", BridgeIP: "192.0.2.9"})
	if changed.BridgeIP != "192.0.2.9" || store.SameLocal(changed, old) {
		t.Fatalf("got %+v", changed)
	}
	bad := 300
	if (store.LockRecord{BridgeIP: "x", KeySecret: "k", BridgeKey: "b", LocalID: &bad}).HasLocalCredentials() {
		t.Fatal("local_id 300 is not usable")
	}
}

func TestSnapshotIsACopy(t *testing.T) {
	st, _, _ := store.Open(filepath.Join(t.TempDir(), "locks.json"))
	_ = st.Update(func(c *store.Cache) { c.Locks = []store.LockRecord{{ID: "a"}} })
	snap := st.Snapshot()
	snap.Locks[0].ID = "mutated"
	if st.Snapshot().Locks[0].ID != "a" {
		t.Fatal("snapshot shares memory with the store")
	}
}

func TestFromCloud(t *testing.T) {
	id := 2
	r := store.FromCloud(cloud.Lock{ID: "x", Name: "n", ModelName: "m", BridgeIP: "1.2.3.4", BridgeHostname: "h",
		BridgeMacWifi: "mac", LocalID: &id, KeySecret: "k", BridgeKey: "b", BackendKey: "bk"})
	if r.ID != "x" || r.ModelName != "m" || *r.LocalID != 2 || r.BackendKey != "bk" || r.BridgeMacWifi != "mac" {
		t.Fatalf("%+v", r)
	}
}

func TestCheckWritableKeepsLoadedCache(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locks.json")
	st, _, _ := store.Open(path)
	if err := st.Update(func(c *store.Cache) { c.CloudSecret = "s"; c.InstallID = "abcd1234" }); err != nil {
		t.Fatal(err)
	}
	loaded, status, _ := store.Open(path)
	if status != store.StatusLoaded {
		t.Fatalf("status %v", status)
	}
	if err := loaded.CheckWritable(); err != nil {
		t.Fatalf("CheckWritable: %v", err)
	}
	again, _, _ := store.Open(path)
	if c := again.Snapshot(); c.CloudSecret != "s" || c.InstallID != "abcd1234" {
		t.Fatalf("cache changed on disk: %+v", c)
	}
}

func TestCheckWritableCreatesMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locks.json")
	st, _, _ := store.Open(path)
	if err := st.CheckWritable(); err != nil {
		t.Fatalf("CheckWritable: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", fi.Mode().Perm())
	}
}

func TestCheckWritableReadOnlyDirectory(t *testing.T) {
	for _, existing := range []bool{false, true} {
		dir := t.TempDir()
		path := filepath.Join(dir, "locks.json")
		st, _, _ := store.Open(path)
		if existing {
			if err := st.Update(func(c *store.Cache) { c.CloudSecret = "s" }); err != nil {
				t.Fatal(err)
			}
			st, _, _ = store.Open(path)
		}
		before := st.Snapshot()
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
		if os.Geteuid() == 0 {
			t.Skip("root ignores directory permissions")
		}
		err := st.CheckWritable()
		if !errors.Is(err, store.ErrWrite) || !strings.Contains(err.Error(), path) {
			t.Fatalf("existing=%v: got %v", existing, err)
		}
		if after := st.Snapshot(); after.CloudSecret != before.CloudSecret || after.InstallID != before.InstallID {
			t.Fatalf("existing=%v: snapshot changed", existing)
		}
	}
}
