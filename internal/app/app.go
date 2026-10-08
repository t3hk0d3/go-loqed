// Package app wires loqed-mqtt together.
package app

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/cloud/portal"
	"github.com/t3hk0d3/go-loqed/internal/auth"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/gateway"
	"github.com/t3hk0d3/go-loqed/internal/mqtt"
	"github.com/t3hk0d3/go-loqed/internal/mqtt/hass"
	"github.com/t3hk0d3/go-loqed/internal/store"
	"github.com/t3hk0d3/go-loqed/internal/webhook"
)

type Options struct {
	Config        config.Config
	Log           *slog.Logger
	Version       string
	CloudBaseURL  string // empty = production
	PortalBaseURL string // empty = production
	Now           func() time.Time
	Ready         func(webhookAddr string) // test hook
	Timing        func(*gateway.Timing)    // test hook: shorten supervisor timings
}

// budgetWindow is LOQED's documented rate-limit window.
const budgetWindow = 12 * time.Hour

// missingLockRefreshAge: an allow-listed lock missing from a cache younger
// than this does not trigger a refresh (a typo must not spend a call on
// every restart).
const missingLockRefreshAge = 12 * time.Hour

// Run starts the gateway and blocks until ctx is cancelled (returns nil)
// or a fatal error occurs.
func Run(ctx context.Context, o Options) error {
	cfg, log := o.Config, o.Log
	now := o.Now
	if now == nil {
		now = time.Now
	}
	cloudBase, portalBase := o.CloudBaseURL, o.PortalBaseURL
	if cloudBase == "" {
		cloudBase = cloud.DefaultBaseURL
	}
	if portalBase == "" {
		portalBase = portal.DefaultBaseURL
	}

	st, status, err := store.Open(cfg.CachePath)
	if err != nil {
		return err
	}
	switch status {
	case store.StatusCorrupt:
		log.Warn("the credential cache is unreadable; rebuilding it from the cloud", "path", cfg.CachePath)
	case store.StatusUnknownVersion:
		log.Warn("the credential cache was written by another version; rebuilding it from the cloud", "path", cfg.CachePath)
	}
	// Without a writable cache the budget, a minted token and learned data
	// would not survive a restart, so a restart loop could exceed LOQED's
	// limit and add a lock key per mint: stop before any cloud call.
	if err := st.CheckWritable(); err != nil {
		return fmt.Errorf("the credential cache cannot be written; fix the permissions of its directory: %w", err)
	}
	before := st.Snapshot()
	installID, err := st.InstallID()
	if err != nil {
		return fmt.Errorf("the credential cache cannot be written; fix the permissions of its directory: %w", err)
	}
	cloudSecret := ""
	if cfg.Webhook.PublicURL != "" {
		if cloudSecret, err = ensureCloudSecret(cfg, st); err != nil {
			return err
		}
	}

	var minter auth.Minter
	if cfg.CanMint() {
		minter = auth.NewPortalMinter(portal.New(portal.WithBaseURL(portalBase)), cfg.CloudEmail, cfg.CloudPassword,
			auth.TokenName(installID), log)
	}
	resolver := auth.NewResolver(cfg.CloudToken, cfg.CloudEmail, minter, st, now, log)
	var saveWarn sync.Once
	budget := gateway.NewBudget(cfg.CloudBudget, budgetWindow, now, before.Budget, func(b store.BudgetState) {
		if err := st.Update(func(c *store.Cache) { c.Budget = b }); err != nil {
			saveWarn.Do(func() { log.Error("cannot persist the cloud request budget", "err", err) })
		}
	})
	hub := gateway.NewCloudHub(budget, resolver,
		func(tok string) gateway.CloudAPI { return cloud.New(tok, cloud.WithBaseURL(cloudBase)) }, now, log)
	refresher := gateway.NewRefresher(hub, st, now)

	if err := initialRefresh(ctx, cfg, st, status, resolver, refresher, now, log); err != nil {
		if ctx.Err() != nil {
			return nil // stopped during startup
		}
		return err
	}
	all := st.Snapshot().Locks
	selected, missing := gateway.Select(all, cfg.Locks)
	for _, m := range missing {
		log.Warn("a lock from the locks allow-list is not on the account", "lock", m)
	}
	for _, k := range gateway.UnmatchedSettings(cfg.LockSettings, all) {
		log.Warn("lock_settings entry matches no lock id or name", "entry", k)
	}
	if len(selected) == 0 {
		return errors.New("no locks to manage: the account has no locks, or the locks allow-list matches none")
	}

	ln, err := net.Listen("tcp", cfg.Webhook.Listen)
	if err != nil {
		return fmt.Errorf("listening for webhooks on %s: %w", cfg.Webhook.Listen, err)
	}
	port := ln.Addr().(*net.TCPAddr).Port

	published := newPublished(selected)
	removed := removedIDs(before, selected)
	published.addPending(removed)
	topics := mqtt.Topics{Base: cfg.MQTT.BaseTopic}
	var discovery mqtt.Discovery
	if cfg.HomeAssistant.Enabled {
		discovery = hass.New(cfg.HomeAssistant.DiscoveryPrefix, topics, o.Version)
	}
	mq := mqtt.NewClient(mqtt.ClientConfig{
		URL: cfg.MQTT.URL, Username: cfg.MQTT.Username, Password: cfg.MQTT.Password, ClientID: cfg.MQTT.ClientID,
		Topics: topics, Discovery: discovery, CloudWebhooks: cfg.MQTT.CloudWebhooks, Now: now,
		OnRemovedCleared: func(ids []string) {
			published.clearPending(ids)
			savePublished(st, published.persistIDs(), log)
		},
	}, log)
	mq.SetLocks(published.infos(), removed)
	savePublished(st, published.persistIDs(), log)
	mq.Start()
	defer mq.Close()

	cloudHost := hostPort(cloudBase)
	deps := gateway.Deps{
		Publisher: mq, Cloud: hub, Refresh: refresher.Refresh,
		NewBridge: func(rec store.LockRecord) (gateway.BridgeAPI, error) {
			if !rec.HasLocalCredentials() {
				return nil, errors.New("lock has no usable local credentials")
			}
			c, err := bridge.New(rec.BridgeIP, bridge.Credentials{BridgeKey: rec.BridgeKey, KeySecret: rec.KeySecret,
				LocalKeyID: uint8(*rec.LocalID)}, bridge.WithClock(now)) //nolint:gosec // G115: HasLocalCredentials checks 0..255
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		Probe:      gateway.TCPProbe,
		ProbeCloud: func(ctx context.Context) error { return gateway.TCPProbe(ctx, cloudHost) },
		WebhookURL: func(rec store.LockRecord) (string, error) {
			return webhook.PrivateURL(cfg.Webhook.PrivateURL, port, rec.ID, rec.BridgeIP)
		},
		CloudWebhooks: cloudWebhooksConfigured(cfg),
		SaveCloudWebhookID: func(lockID, id string) error {
			return st.Update(func(c *store.Cache) {
				for i := range c.Locks {
					if c.Locks[i].ID == lockID {
						c.Locks[i].CloudWebhookID = id
					}
				}
			})
		},
		TokenExpiry: resolver.Expiry,
		Now:         now,
		Log:         log,
	}
	timing := gateway.DefaultTiming(cfg.LivenessInterval.D(), cfg.ReconcileInterval.D(), budget.Spacing())
	timing.DuplicateWindow = cfg.EventDedupWindow.D()
	if !cfg.EventDedupEnabled {
		timing.DuplicateWindow = 0 // every delivery is published
	}
	if o.Timing != nil {
		o.Timing(&timing)
	}
	sups := make([]*gateway.Supervisor, 0, len(selected))
	for _, r := range selected {
		sups = append(sups, gateway.NewSupervisor(r, gateway.SettingFor(cfg.LockSettings, r), deps, timing))
		warnContainerNetwork(cfg, r, log)
	}
	manager := gateway.NewManager(sups)
	refresher.OnRemoved = func(ids []string) {
		// Runs on a supervisor goroutine; stopping others must not wait on it.
		go func() {
			gone := manager.Remove(ids)
			if len(gone) == 0 {
				return
			}
			log.Info("locks were removed from the account", "lock_ids", gone)
			mq.SetLocks(published.remove(gone), gone)
			savePublished(st, published.persistIDs(), log)
		}()
	}

	srv := &http.Server{
		Handler: webhook.NewHandler(webhook.Options{Sink: manager, CloudSecret: cloudSecret,
			BridgeTimestampTolerance: cfg.Webhook.BridgeTimestampTolerance.D(),
			MQTTDownFor:              mq.DisconnectedFor, Now: now, Log: log}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	runCtx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			stop(fmt.Errorf("webhook server: %w", err))
		}
	}()
	if cloudSecret != "" {
		for _, r := range selected {
			log.Info("register this URL as the webhook of this lock in the API section of app.loqed.com",
				"lock", r.Name, "url", webhook.CloudURL(cfg.Webhook.PublicURL, cloudSecret, r.ID))
		}
	}
	if cfg.MQTT.CloudWebhooks {
		for _, r := range selected {
			log.Info("relay this lock's cloud webhooks to its MQTT topic (see the add-on documentation)",
				"lock", r.Name, "topic", topics.CloudWebhook(mqtt.TopicID(r.ID)))
		}
	}
	go forwardCommands(runCtx, mq, manager, log)
	go forwardCloudWebhooks(runCtx, mq, manager, log)
	go refreshByAge(runCtx, refresher, cfg.CacheMaxAge.D(), log)
	go watchTokenExpiry(runCtx, resolver.CheckExpiry, func(ctx context.Context) {
		// A new token comes with a new lock key: use both from now on.
		hub.ResetToken()
		recs, err := refresher.RefreshAll(ctx)
		if recs == nil {
			log.Warn("refreshing the locks after replacing the token failed", "err", err)
			return
		}
		manager.UpdateRecords(recs)
	}, expiryCheckInterval, log)
	if o.Ready != nil {
		o.Ready(ln.Addr().String())
	}

	manager.Run(runCtx)
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	if cause := context.Cause(runCtx); ctx.Err() == nil && cause != nil {
		return cause
	}
	return nil
}

func forwardCommands(ctx context.Context, mq *mqtt.Client, m *gateway.Manager, log *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case c := <-mq.Commands():
			if err := m.DeliverCommand(c.LockID, c.Command, c.ID, c.At); err != nil {
				log.Warn("command not delivered", "lock_id", c.LockID, "err", err)
			}
		}
	}
}

// cloudWebhooksConfigured: cloud webhooks can arrive over HTTP (public_url)
// or over MQTT (mqtt.cloud_webhooks); either lets cloud mode rely on push.
func cloudWebhooksConfigured(cfg config.Config) bool {
	return cfg.Webhook.PublicURL != "" || cfg.MQTT.CloudWebhooks
}

// forwardCloudWebhooks hands relayed cloud webhook bodies to the gateway.
// There is no one to answer, so every rejection is a warning; the body is
// never logged (it carries personal data).
func forwardCloudWebhooks(ctx context.Context, mq *mqtt.Client, m *gateway.Manager, log *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case cw := <-mq.CloudWebhooks():
			ev, err := m.DeliverCloudWebhook(cw.LockID, cw.Body)
			switch {
			case err == nil:
			case errors.Is(err, gateway.ErrCloudIDMismatch):
				log.Warn("dropped a cloud webhook relayed for another lock; relay each lock's webhook to its own topic",
					"lock_id", cw.LockID, "cloud_lock_id", ev.LockID, "bound_cloud_lock_id", gateway.BoundCloudID(err))
			case errors.Is(err, loqed.ErrInvalidPayload):
				log.Warn("dropped an invalid cloud webhook relayed over MQTT", "lock_id", cw.LockID)
			case errors.Is(err, gateway.ErrUnknownLock):
				log.Warn("dropped a cloud webhook for an unknown lock", "lock_id", cw.LockID)
			case errors.Is(err, gateway.ErrBusy):
				log.Warn("lock is busy; dropped a cloud webhook", "lock_id", cw.LockID)
			default:
				log.Warn("cloud webhook not delivered", "lock_id", cw.LockID)
			}
		}
	}
}

// expiryCheckInterval: how often the token's expiry is checked.
const expiryCheckInterval = time.Hour

// watchTokenExpiry checks the token's expiry now and every interval, and
// runs onRemint after the token was replaced.
func watchTokenExpiry(ctx context.Context, check func(context.Context) (bool, error), onRemint func(context.Context),
	interval time.Duration, log *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		reminted, err := check(ctx)
		if err != nil && ctx.Err() == nil {
			log.Debug("token expiry check failed", "err", err)
		}
		if reminted {
			onRemint(ctx)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// refreshByAge applies cache_max_age while running (checked hourly).
func refreshByAge(ctx context.Context, r *gateway.Refresher, maxAge time.Duration, log *slog.Logger) {
	if maxAge <= 0 {
		return
	}
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if ran, err := r.RefreshIfOlder(ctx, maxAge); ran && err != nil {
				log.Warn("scheduled credential refresh failed", "err", err)
			}
		}
	}
}

func initialRefresh(ctx context.Context, cfg config.Config, st *store.Store, status store.Status,
	resolver *auth.Resolver, refresher *gateway.Refresher, now func() time.Time, log *slog.Logger) error {
	snap := st.Snapshot()
	tok, err := resolver.Token(ctx)
	if errors.Is(err, auth.ErrNoToken) && len(snap.Locks) == 0 {
		return err
	}
	if err != nil {
		if len(snap.Locks) > 0 {
			log.Warn("no usable LOQED token; starting from the credential cache", "err", err)
			return nil
		}
		return fmt.Errorf("cannot start: no credential cache and no usable LOQED token: %w", err)
	}
	age := now().Sub(snap.FetchedAt)
	need := status != store.StatusLoaded ||
		snap.TokenSHA256 != store.TokenHash(tok) ||
		len(snap.Locks) == 0 ||
		(missingFromCache(snap, cfg.Locks) && age > missingLockRefreshAge) ||
		(cfg.CacheMaxAge > 0 && age > cfg.CacheMaxAge.D())
	if !need {
		return nil
	}
	_, err = refresher.RefreshAll(ctx)
	switch {
	case errors.Is(err, store.ErrWrite):
		log.Error("lock data refreshed but the credential cache cannot be written; the next start needs the cloud again", "err", err)
		return nil
	case err != nil && len(snap.Locks) > 0:
		log.Warn("cloud refresh failed; starting from the credential cache", "err", err)
		return nil
	case err != nil:
		return fmt.Errorf("cannot start: no credential cache and the LOQED cloud is unavailable: %w", err)
	}
	return nil
}

func missingFromCache(c store.Cache, allow []string) bool {
	for _, a := range allow {
		if _, ok := c.Find(a); !ok {
			return true
		}
	}
	return false
}

// removedIDs lists ids that had retained topics (published_ids, or cached
// locks from before published_ids existed) and are no longer selected.
func removedIDs(before store.Cache, selected []store.LockRecord) []string {
	keep := make(map[string]bool, len(selected))
	for _, r := range selected {
		keep[r.ID] = true
	}
	var out []string
	seen := map[string]bool{}
	add := func(id string) {
		if !keep[id] && !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	for _, id := range before.PublishedIDs {
		add(id)
	}
	for _, r := range before.Locks {
		add(r.ID)
	}
	return out
}

// published tracks the locks currently exposed over MQTT.
// pending holds removed ids whose retained topics are not yet known to be
// cleared; they stay in published_ids so a restart still clears them.
type published struct {
	mu      sync.Mutex
	locks   []mqtt.LockInfo
	pending []string
}

func newPublished(recs []store.LockRecord) *published {
	p := &published{}
	for _, r := range recs {
		p.locks = append(p.locks, mqtt.LockInfo{ID: r.ID, Name: r.Name, Model: r.ModelName, MacWifi: r.BridgeMacWifi})
	}
	return p
}

func (p *published) infos() []mqtt.LockInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.locks)
}

// persistIDs lists the published locks plus removed ones not yet cleared.
func (p *published) persistIDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.locks)+len(p.pending))
	for _, l := range p.locks {
		out = append(out, l.ID)
	}
	return append(out, p.pending...)
}

func (p *published) addPending(ids []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, id := range ids {
		if !slices.Contains(p.pending, id) {
			p.pending = append(p.pending, id)
		}
	}
}

func (p *published) clearPending(ids []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pending = slices.DeleteFunc(p.pending, func(id string) bool { return slices.Contains(ids, id) })
}

func (p *published) remove(ids []string) []mqtt.LockInfo {
	p.addPending(ids)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.locks = slices.DeleteFunc(p.locks, func(l mqtt.LockInfo) bool { return slices.Contains(ids, l.ID) })
	return slices.Clone(p.locks)
}

func savePublished(st *store.Store, ids []string, log *slog.Logger) {
	if err := st.Update(func(c *store.Cache) { c.PublishedIDs = ids }); err != nil {
		log.Warn("cannot save the published lock list", "err", err)
	}
}

func warnContainerNetwork(cfg config.Config, r store.LockRecord, log *slog.Logger) {
	if cfg.Webhook.PrivateURL != "" || r.BridgeIP == "" {
		return
	}
	if ip, err := webhook.SourceIP(r.BridgeIP); err == nil && webhook.LikelyContainerAddress(ip, r.BridgeIP) {
		log.Warn("the bridge would call a container-internal address it cannot reach; use host networking or set webhook.private_url",
			"lock", r.Name, "address", ip.String())
	}
}

// hostPort returns host:port of a base URL (port 443 for https).
func hostPort(base string) string {
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	if u.Port() != "" {
		return u.Host
	}
	if u.Scheme == "http" {
		return net.JoinHostPort(u.Hostname(), "80")
	}
	return net.JoinHostPort(u.Hostname(), "443")
}

func ensureCloudSecret(cfg config.Config, st *store.Store) (string, error) {
	if cfg.Webhook.CloudSecret != "" {
		return cfg.Webhook.CloudSecret, nil
	}
	if s := st.Snapshot().CloudSecret; s != "" {
		return s, nil
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	secret := base64.RawURLEncoding.EncodeToString(b)
	if err := st.Update(func(c *store.Cache) { c.CloudSecret = secret }); err != nil {
		return "", fmt.Errorf("saving the cloud webhook secret: %w", err)
	}
	return secret, nil
}
