package relay

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rwrz/ai-control-plane/internal/pricing"
	"github.com/rwrz/ai-control-plane/internal/relay/extract"
)

func TestParseKeyring(t *testing.T) {
	t.Run("empty spec disables auth", func(t *testing.T) {
		kr, err := ParseKeyring("  ")
		if err != nil || kr.Enabled() {
			t.Fatalf("kr.Enabled()=%v err=%v, want disabled and no error", kr.Enabled(), err)
		}
	})

	t.Run("maps keys to tenants", func(t *testing.T) {
		kr, err := ParseKeyring(" k1:backend , k2:voice_agent ")
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := kr.Lookup("k1"); !ok || got != "backend" {
			t.Errorf("k1 -> %q,%v want backend", got, ok)
		}
		if got, ok := kr.Lookup("k2"); !ok || got != "voice_agent" {
			t.Errorf("k2 -> %q,%v want voice_agent", got, ok)
		}
		if _, ok := kr.Lookup("nope"); ok {
			t.Error("unknown key authenticated")
		}
		if _, ok := kr.Lookup(""); ok {
			t.Error("empty credential authenticated")
		}
	})

	// A malformed entry must fail loudly at boot. Skipping it would silently
	// leave a consumer unable to authenticate, or worse, leave auth "on" with
	// fewer keys than intended.
	t.Run("malformed entries are rejected", func(t *testing.T) {
		for _, spec := range []string{"noseparator", "k1:", ":tenant", "k1:t,bad"} {
			if _, err := ParseKeyring(spec); err == nil {
				t.Errorf("ParseKeyring(%q) accepted a malformed spec", spec)
			}
		}
	})

	// The error must not echo the credential — boot errors get logged.
	t.Run("errors never contain the key", func(t *testing.T) {
		_, err := ParseKeyring("supersecret")
		if err == nil {
			t.Fatal("expected error")
		}
		if strings.Contains(err.Error(), "supersecret") {
			t.Errorf("error leaks the credential: %v", err)
		}
	})
}

// authProvider builds a Provider in front of upstream with the given keyring.
func authProvider(t *testing.T, upstreamURL, spec string) (*Provider, *recordingSink, *bool) {
	t.Helper()
	kr, err := ParseKeyring(spec)
	if err != nil {
		t.Fatal(err)
	}
	reached := false
	sink := &recordingSink{}
	p, err := newProvider(ProviderConfig{
		Name:    "openai",
		Target:  upstreamURL,
		Extract: staticExtract(extract.OpenAIExtract),
		Keyring: kr,
		InjectAuth: func(r *http.Request) {
			reached = true
			r.Header.Set("Authorization", "Bearer real-provider-key")
		},
	}, &pricing.Config{}, sink, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return p, sink, &reached
}

func TestAuthRejectsAndAccepts(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The relay credential must never reach the provider, in either channel.
		if r.Header.Get(authHeader) != "" {
			t.Errorf("relay credential leaked upstream in %s", authHeader)
		}
		if r.URL.Query().Has(authQueryKey) {
			t.Errorf("relay credential leaked upstream in query: %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(upstreamBody))
	}))
	defer upstream.Close()

	t.Run("no credential is refused", func(t *testing.T) {
		p, _, reached := authProvider(t, upstream.URL, "k1:backend")
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/relay/openai/v1/chat/completions", nil))

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", rec.Code)
		}
		if *reached {
			t.Error("an unauthenticated request reached the provider credential")
		}
	})

	t.Run("wrong credential is refused", func(t *testing.T) {
		p, _, reached := authProvider(t, upstream.URL, "k1:backend")
		req := httptest.NewRequest(http.MethodPost, "/v1/relay/openai/v1/chat/completions", nil)
		req.Header.Set(authHeader, "wrong")
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized || *reached {
			t.Errorf("status=%d reached=%v, want 401 and no provider access", rec.Code, *reached)
		}
	})

	t.Run("header credential is accepted and stripped", func(t *testing.T) {
		p, _, reached := authProvider(t, upstream.URL, "k1:backend")
		req := httptest.NewRequest(http.MethodPost, "/v1/relay/openai/v1/chat/completions", nil)
		req.Header.Set(authHeader, "k1")
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK || !*reached {
			t.Errorf("status=%d reached=%v, want 200 and provider reached", rec.Code, *reached)
		}
	})

	// WebSocket clients often cannot set handshake headers but can always add a
	// query parameter — Phase 2 audio depends on this path working.
	t.Run("query credential is accepted and stripped", func(t *testing.T) {
		p, _, reached := authProvider(t, upstream.URL, "k1:backend")
		req := httptest.NewRequest(http.MethodPost,
			"/v1/relay/openai/v1/chat/completions?relay_key=k1&model=x", nil)
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK || !*reached {
			t.Errorf("status=%d reached=%v, want 200", rec.Code, *reached)
		}
	})

	t.Run("disabled keyring lets everything through", func(t *testing.T) {
		p, _, reached := authProvider(t, upstream.URL, "")
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/relay/openai/v1/chat/completions", nil))

		if rec.Code != http.StatusOK || !*reached {
			t.Errorf("status=%d reached=%v, want 200 when auth is off", rec.Code, *reached)
		}
	})
}

// A key is bound to a tenant so a leaked credential cannot book spend against
// somebody else, whatever it claims in X-Usage-Tenant.
func TestAuthenticatedKeyOverridesClaimedTenant(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(upstreamBody))
	}))
	defer upstream.Close()

	p, sink, _ := authProvider(t, upstream.URL, "coach-key:voice_agent")
	req := httptest.NewRequest(http.MethodPost, "/v1/relay/openai/v1/chat/completions", nil)
	req.Header.Set(authHeader, "coach-key")
	req.Header.Set("X-Usage-Tenant", "the Go backend") // a lie
	p.ServeHTTP(httptest.NewRecorder(), req)

	if len(sink.events) != 1 {
		t.Fatalf("recorded %d events, want 1", len(sink.events))
	}
	if got := sink.events[0].Tenant; got != "voice_agent" {
		t.Errorf("tenant = %q, want voice_agent — a caller billed another tenant", got)
	}
}

func TestParseAuthMode(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "", want: AuthEnforce}, // saying nothing gets the safe mode
		{in: "enforce", want: AuthEnforce},
		{in: "shadow", want: AuthShadow},
		{in: "  SHADOW  ", want: AuthShadow}, // an env var pasted with whitespace
		{in: "off", wantErr: true},           // plausible, and NOT a silent pass-through
		{in: "true", wantErr: true},
	} {
		got, err := ParseAuthMode(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseAuthMode(%q) = %q, want an error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseAuthMode(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseAuthMode(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// shadowProvider is authProvider with the keyring in shadow mode.
func shadowProvider(t *testing.T, upstreamURL, spec string) (*Provider, *recordingSink, *bool) {
	t.Helper()
	p, sink, reached := authProvider(t, upstreamURL, spec)
	p.authMode = AuthShadow
	return p, sink, reached
}

// Shadow mode exists for the window between enabling keys on the relay and
// every consumer having been redeployed with one. In that window an enforcing
// relay does not merely stop metering the stragglers — it FAILS their AI calls,
// because it sits in the request path. This is the test that the fallback is a
// real one: the call must reach the provider and still be attributed.
func TestShadowModeServesUnauthenticatedCallers(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer real-provider-key" {
			t.Errorf("upstream credential not injected for a shadow-mode call")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"gpt-4o","usage":{"prompt_tokens":10,"completion_tokens":5}}`))
	}))
	defer upstream.Close()

	p, sink, reached := shadowProvider(t, upstream.URL, "good-key:backend")

	req := httptest.NewRequest(http.MethodPost, "/v1/relay/openai/v1/chat/completions", nil)
	req.Header.Set("X-Usage-Tenant", "backend")
	req.Header.Set("X-Usage-Module", "dictionary")
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: shadow mode must not reject", rec.Code)
	}
	if !*reached {
		t.Error("request never reached the provider")
	}
	// Attribution falls back to what the caller declared — the behaviour of an
	// unauthenticated relay, which is exactly what shadow mode emulates.
	if got := sink.events; len(got) != 1 || got[0].Tenant != "backend" {
		t.Errorf("events = %+v, want one attributed to the declared tenant", got)
	}
}

// The same request, enforcing. Together these pin the ONLY difference between
// the modes: whether an unauthenticated caller is served.
func TestEnforceModeRejectsWhatShadowServes(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p, _, reached := authProvider(t, upstream.URL, "good-key:backend")

	req := httptest.NewRequest(http.MethodPost, "/v1/relay/openai/v1/chat/completions", nil)
	req.Header.Set("X-Usage-Tenant", "backend")
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if *reached {
		t.Error("an unauthenticated request reached the provider under enforce")
	}
}

// A valid key must still win in shadow mode: the keyring is checked, not
// bypassed, so an authenticated caller is still bound to its own tenant and
// cannot bill another consumer by claiming one in a header.
func TestShadowModeStillBindsAValidKeyToItsTenant(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"gpt-4o","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer upstream.Close()

	p, sink, _ := shadowProvider(t, upstream.URL, "coach-key:voice_agent")

	req := httptest.NewRequest(http.MethodPost, "/v1/relay/openai/v1/chat/completions", nil)
	req.Header.Set(authHeader, "coach-key")
	req.Header.Set("X-Usage-Tenant", "the Go backend") // a lie
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if got := sink.events; len(got) != 1 || got[0].Tenant != "voice_agent" {
		t.Errorf("events = %+v, want the tenant bound to the key (voice_agent)", got)
	}
}
