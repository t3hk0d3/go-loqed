package portal_test

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

type fakeToken struct{ ID, Name, Value string }

// fakePortal mimics the Laravel/Inertia integrations portal closely enough
// to exercise cookies, CSRF, redirects, version mismatches and flashes.
type fakePortal struct {
	mu               sync.Mutex
	version          string
	email            string
	password         string
	sessions         map[string]*fakeSession
	tokens           []fakeToken
	nextID           int
	createCalls      int
	brokenTokensPage bool
	noXSRFCookie     bool // the real portal was observed without an XSRF-TOKEN cookie
	rejectCSRF       bool // answer every mutation with 419
	bumpOnCreate     bool // change the asset version while handling a create
}

type fakeSession struct {
	authed      bool
	loginErrors map[string]string
	flashToken  string
}

func newFakePortal(t *testing.T) (*fakePortal, *httptest.Server) {
	t.Helper()
	f := &fakePortal{version: "v1", email: "me@example.com", password: "s3cret", sessions: map[string]*fakeSession{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv
}

// csrfFor contains characters that need URL-encoding in a cookie.
func (f *fakePortal) csrfFor(sid string) string { return "csrf+/=" + sid }

func (f *fakePortal) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	sid := ""
	if c, err := r.Cookie("laravel_session"); err == nil {
		sid = c.Value
	}
	sess, ok := f.sessions[sid]
	if !ok {
		sid = fmt.Sprintf("s%d", len(f.sessions)+1)
		sess = &fakeSession{}
		f.sessions[sid] = sess
	}
	http.SetCookie(w, &http.Cookie{Name: "laravel_session", Value: sid, Path: "/"})
	if !f.noXSRFCookie {
		http.SetCookie(w, &http.Cookie{Name: "XSRF-TOKEN", Value: url.QueryEscape(f.csrfFor(sid)), Path: "/"})
	}

	// Laravel's VerifyCsrfToken accepts X-CSRF-TOKEN (meta) or X-XSRF-TOKEN (cookie).
	if r.Method != http.MethodGet {
		valid := r.Header.Get("X-CSRF-TOKEN") == f.csrfFor(sid) || r.Header.Get("X-XSRF-TOKEN") == f.csrfFor(sid)
		if f.rejectCSRF || !valid {
			w.WriteHeader(419)
			return
		}
	}
	inertia := r.Header.Get("X-Inertia") == "true"
	if inertia && r.Method == http.MethodGet && r.Header.Get("X-Inertia-Version") != f.version {
		// Real Inertia reflashes session data, so the flash survives the 409.
		w.Header().Set("X-Inertia-Location", r.URL.Path)
		w.WriteHeader(http.StatusConflict)
		return
	}

	switch {
	case r.URL.Path == "/login" && r.Method == http.MethodGet:
		errs := sess.loginErrors
		sess.loginErrors = nil
		if errs == nil {
			errs = map[string]string{}
		}
		f.render(w, sid, inertia, "Auth/Login", map[string]any{"errors": errs})
	case r.URL.Path == "/login" && r.Method == http.MethodPost:
		var body struct {
			Email, Password string
			Remember        any
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Remember == true {
			// The real portal answers 500 to "remember me" (observed 2026-10-05).
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if body.Email == f.email && body.Password == f.password {
			sess.authed = true
			http.Redirect(w, r, "/dashboard", http.StatusFound)
			return
		}
		sess.loginErrors = map[string]string{"email": "These credentials do not match our records."}
		http.Redirect(w, r, "/login", http.StatusFound)
	case !sess.authed:
		http.Redirect(w, r, "/login", http.StatusFound)
	case r.URL.Path == "/dashboard":
		f.render(w, sid, inertia, "Dashboard", map[string]any{"errors": []any{}})
	case r.URL.Path == "/personal-access-tokens" && r.Method == http.MethodGet:
		list := []map[string]any{}
		for _, t := range f.tokens {
			list = append(list, map[string]any{"id": t.ID, "name": t.Name})
		}
		props := map[string]any{"errors": []any{}, "tokens": list}
		if sess.flashToken != "" {
			props["accessToken"] = sess.flashToken
			sess.flashToken = ""
		}
		component := "PersonalAccessTokens"
		if f.brokenTokensPage {
			component = "SomethingElse"
		}
		f.render(w, sid, inertia, component, props)
	case r.URL.Path == "/create-personal-access-tokens" && r.Method == http.MethodPost:
		var body struct{ Name string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.createCalls++
		f.nextID++
		tok := fakeToken{ID: fmt.Sprintf("%040d", f.nextID), Name: body.Name, Value: fmt.Sprintf("pat-%d", f.nextID)}
		f.tokens = append(f.tokens, tok)
		sess.flashToken = tok.Value
		if f.bumpOnCreate {
			f.version += "-new"
		}
		http.Redirect(w, r, "/personal-access-tokens", http.StatusFound)
	case strings.HasPrefix(r.URL.Path, "/personal-access-tokens/") && r.Method == http.MethodDelete:
		id := strings.TrimPrefix(r.URL.Path, "/personal-access-tokens/")
		kept := f.tokens[:0]
		for _, t := range f.tokens {
			if t.ID != id {
				kept = append(kept, t)
			}
		}
		f.tokens = kept
		http.Redirect(w, r, "/personal-access-tokens", http.StatusSeeOther)
	case r.URL.Path == "/logout" && r.Method == http.MethodPost:
		sess.authed = false
		http.Redirect(w, r, "/login", http.StatusFound)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakePortal) render(w http.ResponseWriter, sid string, inertia bool, component string, props map[string]any) {
	page, _ := json.Marshal(map[string]any{"component": component, "props": props, "url": "/", "version": f.version})
	if inertia {
		w.Header().Set("X-Inertia", "true")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(page)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=UTF-8")
	_, _ = fmt.Fprintf(w, `<!DOCTYPE html><html><head><meta name="csrf-token" content="%s"></head>`+
		`<body><div id="app" data-page="%s"></div></body></html>`,
		html.EscapeString(f.csrfFor(sid)), html.EscapeString(string(page)))
}

func (f *fakePortal) set(fn func(*fakePortal)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakePortal) tokenNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, t := range f.tokens {
		out = append(out, t.Name)
	}
	return out
}
