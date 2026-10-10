// Package store persists cloud lock credentials so restarts do not need
// the (rate-limited) cloud.
package store

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
)

const Version = 1

type LockRecord struct {
	ID             string       `json:"id"`
	Name           string       `json:"name"`
	ModelName      string       `json:"model_name"`
	BridgeIP       string       `json:"bridge_ip"`
	BridgeHostname string       `json:"bridge_hostname"` // informational only, never resolved
	BridgeMacWifi  string       `json:"bridge_mac_wifi"`
	LocalID        *int         `json:"local_id"`
	KeySecret      loqed.Secret `json:"key_secret"`
	BridgeKey      loqed.Secret `json:"bridge_key"`
	BackendKey     loqed.Secret `json:"backend_key"`
	// CloudWebhookID is the cloud's numeric lock id, learned from the first
	// cloud webhook on this lock's URL (the Lock API does not expose it).
	CloudWebhookID string `json:"cloud_webhook_id,omitempty"`
}

func (r LockRecord) HasLocalCredentials() bool {
	return r.BridgeIP != "" && r.BridgeKey != "" && r.KeySecret != "" &&
		r.LocalID != nil && *r.LocalID >= 0 && *r.LocalID <= 255
}

// Merge returns fresh, keeping old's local-credential fields where fresh
// lacks them (the cloud's local fields are undocumented and may vanish) and
// the learned cloud webhook id.
func Merge(old, fresh LockRecord) LockRecord {
	if fresh.BridgeIP == "" {
		fresh.BridgeIP = old.BridgeIP
	}
	if fresh.KeySecret == "" {
		fresh.KeySecret = old.KeySecret
	}
	if fresh.BridgeKey == "" {
		fresh.BridgeKey = old.BridgeKey
	}
	if fresh.LocalID == nil && old.LocalID != nil {
		v := *old.LocalID
		fresh.LocalID = &v
	}
	if fresh.CloudWebhookID == "" {
		fresh.CloudWebhookID = old.CloudWebhookID
	}
	return fresh
}

// SameLocal reports whether two records address the bridge identically.
func SameLocal(a, b LockRecord) bool {
	return a.BridgeIP == b.BridgeIP && a.KeySecret == b.KeySecret && a.BridgeKey == b.BridgeKey &&
		(a.LocalID == nil) == (b.LocalID == nil) && (a.LocalID == nil || *a.LocalID == *b.LocalID)
}

func FromCloud(l cloud.Lock) LockRecord {
	r := LockRecord{ID: l.ID, Name: l.Name, ModelName: l.ModelName, BridgeIP: l.BridgeIP, BridgeHostname: l.BridgeHostname,
		BridgeMacWifi: l.BridgeMacWifi, KeySecret: l.KeySecret, BridgeKey: l.BridgeKey, BackendKey: l.BackendKey}
	if l.LocalID != nil {
		v := *l.LocalID
		r.LocalID = &v
	}
	return r
}

// MintedToken is a token this gateway created via the portal.
type MintedToken struct {
	ID          string       `json:"id"`
	Value       loqed.Secret `json:"value"`
	EmailSHA256 string       `json:"email_sha256,omitempty"` // account it belongs to (EmailHash)
	MintedAt    time.Time    `json:"minted_at"`
}

// String keeps Value redacted when a Cache is printed with %s: fmt prints
// a nested pointer it cannot format with that verb without calling the
// pointee's field methods.
func (m MintedToken) String() string {
	type plain MintedToken
	return fmt.Sprintf("%+v", plain(m))
}

// BudgetState persists the cloud request window across restarts.
type BudgetState struct {
	Calls        []time.Time `json:"calls,omitempty"`
	BlockedUntil time.Time   `json:"blocked_until,omitzero"`
}

type Cache struct {
	Version      int          `json:"version"`
	InstallID    string       `json:"install_id,omitempty"`
	TokenSHA256  string       `json:"token_sha256,omitempty"`
	Minted       *MintedToken `json:"minted_token,omitempty"`
	LastMintAt   time.Time    `json:"last_mint_at,omitzero"` // includes failed attempts
	CloudSecret  loqed.Secret `json:"cloud_secret,omitempty"`
	FetchedAt    time.Time    `json:"fetched_at"`
	PublishedIDs []string     `json:"published_ids,omitempty"`
	Budget       BudgetState  `json:"budget"`
	Locks        []LockRecord `json:"locks"`
}

// Find looks a lock up by id, then by name.
func (c Cache) Find(key string) (LockRecord, bool) {
	for _, r := range c.Locks {
		if r.ID == key {
			return r, true
		}
	}
	for _, r := range c.Locks {
		if r.Name == key {
			return r, true
		}
	}
	return LockRecord{}, false
}

func (c Cache) clone() Cache {
	out := c
	out.Locks = slices.Clone(c.Locks)
	for i := range out.Locks {
		if id := out.Locks[i].LocalID; id != nil {
			v := *id
			out.Locks[i].LocalID = &v
		}
	}
	if c.Minted != nil {
		m := *c.Minted
		out.Minted = &m
	}
	out.PublishedIDs = slices.Clone(c.PublishedIDs)
	out.Budget.Calls = slices.Clone(c.Budget.Calls)
	return out
}

type Status int

const (
	StatusLoaded Status = iota
	StatusMissing
	StatusCorrupt
	StatusUnknownVersion // written by a newer or older release; treated as missing
)

// ErrWrite reports that the cache could not be persisted. In-memory state
// is still updated, so the gateway keeps running.
var ErrWrite = errors.New("store: cannot write the credential cache")

type Store struct {
	path  string
	mu    sync.Mutex
	cache Cache
}

// Open loads the cache. Missing or corrupt files yield an empty cache and
// the matching Status; only unexpected I/O errors are returned.
func Open(path string) (*Store, Status, error) {
	s := &Store{path: path, cache: Cache{Version: Version}}
	b, err := os.ReadFile(path) //nolint:gosec // G304: path comes from configuration
	if errors.Is(err, fs.ErrNotExist) {
		return s, StatusMissing, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("store: %w", err)
	}
	var f cacheFile
	if json.Unmarshal(b, &f) != nil {
		return s, StatusCorrupt, nil
	}
	c := f.cache()
	if c.Version != Version {
		return s, StatusUnknownVersion, nil
	}
	s.cache = c
	return s, StatusLoaded, nil
}

// Path is the cache file location (for messages).
func (s *Store) Path() string { return s.path }

func (s *Store) Snapshot() Cache {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cache.clone()
}

// Update applies fn and persists the result. The in-memory cache is
// updated even when writing fails (the error wraps ErrWrite).
func (s *Store) Update(fn func(*Cache)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.cache)
	s.cache.Version = Version
	if err := write(s.path, s.cache); err != nil {
		return fmt.Errorf("%w %s: %w", ErrWrite, s.path, err)
	}
	return nil
}

// CheckWritable writes the current cache back to disk. At startup it proves
// that the budget, a minted token and learned data will survive a restart.
func (s *Store) CheckWritable() error {
	return s.Update(func(*Cache) {})
}

// InstallID returns this installation's stable random id, creating and
// persisting it on first use. It names minted tokens.
func (s *Store) InstallID() (string, error) {
	s.mu.Lock()
	id := s.cache.InstallID
	s.mu.Unlock()
	if id != "" {
		return id, nil
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	id = hex.EncodeToString(b)
	err := s.Update(func(c *Cache) {
		if c.InstallID == "" {
			c.InstallID = id
		}
		id = c.InstallID
	})
	return id, err
}

// cacheFile is the on-disk form of Cache. A loqed.Secret encodes to JSON as
// loqed.Redacted, so the file spells the secrets out as plain strings; each
// outer field hides the embedded field with the same JSON name.
type cacheFile struct {
	Cache
	Minted      *mintedFile `json:"minted_token,omitempty"`
	CloudSecret string      `json:"cloud_secret,omitempty"`
	Locks       []lockFile  `json:"locks"`
}

type mintedFile struct {
	MintedToken
	Value string `json:"value"`
}

type lockFile struct {
	LockRecord
	KeySecret  string `json:"key_secret"`
	BridgeKey  string `json:"bridge_key"`
	BackendKey string `json:"backend_key"`
}

func toFile(c Cache) cacheFile {
	f := cacheFile{Cache: c, CloudSecret: c.CloudSecret.Reveal()}
	if c.Minted != nil {
		f.Minted = &mintedFile{MintedToken: *c.Minted, Value: c.Minted.Value.Reveal()}
	}
	if c.Locks != nil {
		f.Locks = make([]lockFile, len(c.Locks))
	}
	for i, r := range c.Locks {
		f.Locks[i] = lockFile{LockRecord: r, KeySecret: r.KeySecret.Reveal(), BridgeKey: r.BridgeKey.Reveal(), BackendKey: r.BackendKey.Reveal()}
	}
	return f
}

func (f cacheFile) cache() Cache {
	c := f.Cache
	c.CloudSecret = loqed.Secret(f.CloudSecret)
	c.Minted = nil
	if f.Minted != nil {
		m := f.Minted.MintedToken
		m.Value = loqed.Secret(f.Minted.Value)
		c.Minted = &m
	}
	c.Locks = nil
	if f.Locks != nil {
		c.Locks = make([]LockRecord, len(f.Locks))
	}
	for i, l := range f.Locks {
		r := l.LockRecord
		r.KeySecret, r.BridgeKey, r.BackendKey = loqed.Secret(l.KeySecret), loqed.Secret(l.BridgeKey), loqed.Secret(l.BackendKey)
		c.Locks[i] = r
	}
	return c
}

func write(path string, c Cache) error {
	b, err := json.MarshalIndent(toFile(c), "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("store: %w", err)
	}
	f, err := os.CreateTemp(dir, ".locks-*.json")
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }() // no-op after a successful rename
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return fmt.Errorf("store: %w", err)
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return fmt.Errorf("store: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("store: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("store: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("store: %w", err)
	}
	return nil
}

func TokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// EmailHash identifies the portal account a minted token belongs to.
func EmailHash(email string) string {
	return TokenHash(strings.ToLower(strings.TrimSpace(email)))
}
