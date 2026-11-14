package relay

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/rwrz/ai-control-plane/internal/pricing"
	"github.com/rwrz/ai-control-plane/internal/relay/extract"
)

type capturingSink struct {
	mu     sync.Mutex
	events []Event
}

func (s *capturingSink) Record(_ context.Context, e Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func (s *capturingSink) all() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.events...)
}

// testPrices builds the table in-process rather than loading pricing.toml, so
// these tests assert relay behaviour and do not fail when a rate is edited.
func testPrices(t *testing.T) *pricing.Config {
	t.Helper()
	return &pricing.Config{Models: map[string]pricing.ModelPrice{
		"deepseek/deepseek-v4-flash": {
			InputPerMtok:       0.0882,
			CachedInputPerMtok: 0.0882,
			OutputPerMtok:      0.1764,
		},
	}}
}

// OpenRouter serves under /api/v1/..., OpenAI under /v1/... . The Director
// strips only the route prefix and forwards the rest verbatim, so this pins the
// exact upstream path — get it wrong and every call 404s at the vendor, which
// looks like a model problem rather than a routing one.
func TestOpenRouterForwardsPathAndSwapsCredential(t *testing.T) {
	var gotPath, gotAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotAuth = r.URL.Path, r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"deepseek/deepseek-v4-flash",
			"usage":{"prompt_tokens":1000,"completion_tokens":500,"total_tokens":1500}}`))
	}))
	defer upstream.Close()

	sink := &capturingSink{}
	p, err := newProvider(ProviderConfig{
		Name:    RouteOpenRouter,
		Target:  upstream.URL,
		Extract: staticExtract(extract.OpenAIExtract),
		InjectAuth: func(req *http.Request) {
			req.Header.Set("Authorization", "Bearer real-upstream-key")
		},
	}, testPrices(t), sink, slog.Default())
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost,
		"/v1/relay/openrouter/api/v1/chat/completions", strings.NewReader(`{"model":"x"}`))
	req.Header.Set("Authorization", "Bearer caller-virtual-credential")
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if gotPath != "/api/v1/chat/completions" {
		t.Errorf("upstream path = %q, want /api/v1/chat/completions", gotPath)
	}
	// The caller's credential must never reach the vendor: callers hold a relay
	// token, not a provider key, and forwarding it would leak one tenant's
	// header into another's upstream account.
	if gotAuth != "Bearer real-upstream-key" {
		t.Errorf("upstream Authorization = %q, want the relay-injected key", gotAuth)
	}
}

// OpenRouter returns the OpenAI CHAT usage shape (prompt_tokens /
// completion_tokens). The Responses API names the same numbers input_tokens /
// output_tokens, and the wrong reader yields a silent zero rather than an
// error — which would bill every OpenRouter call as free.
func TestOpenRouterMetersAndPricesTheCall(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"deepseek/deepseek-v4-flash",
			"usage":{"prompt_tokens":1000,"completion_tokens":500,"total_tokens":1500}}`))
	}))
	defer upstream.Close()

	sink := &capturingSink{}
	p, _ := newProvider(ProviderConfig{
		Name: RouteOpenRouter, Target: upstream.URL,
		Extract:    staticExtract(extract.OpenAIExtract),
		InjectAuth: func(req *http.Request) {},
	}, testPrices(t), sink, slog.Default())

	req := httptest.NewRequest(http.MethodPost, "/v1/relay/openrouter/api/v1/chat/completions", strings.NewReader("{}"))
	p.ServeHTTP(httptest.NewRecorder(), req)

	events := sink.all()
	if len(events) != 1 {
		t.Fatalf("recorded %d events, want 1 — an unmetered call is invisible spend", len(events))
	}
	e := events[0]
	if e.InputTokens != 1000 || e.OutputTokens != 500 {
		t.Errorf("tokens = in %d / out %d, want 1000/500 (chat dialect must not read as zero)", e.InputTokens, e.OutputTokens)
	}
	// A vendor-prefixed slug must survive lookup intact.
	if e.Model != "deepseek/deepseek-v4-flash" {
		t.Errorf("model = %q, want the slug verbatim", e.Model)
	}
	if !e.CostKnown {
		t.Error("cost unknown — the slug did not match the price table")
	}
	// 1000*0.0882/1e6 + 500*0.1764/1e6 = 0.0000882 + 0.0000882 = 0.0001764
	if want := 0.0001764; e.CostUSD < want*0.999 || e.CostUSD > want*1.001 {
		t.Errorf("cost = %v, want ~%v", e.CostUSD, want)
	}
}

// An unlisted slug must still be recorded. Dropping it would make the event
// disappear entirely, which reads as "no spend" rather than "unpriced spend".
func TestOpenRouterRecordsUnpricedModels(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"someone/brand-new-model",
			"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}`))
	}))
	defer upstream.Close()

	sink := &capturingSink{}
	p, _ := newProvider(ProviderConfig{
		Name: RouteOpenRouter, Target: upstream.URL,
		Extract:    staticExtract(extract.OpenAIExtract),
		InjectAuth: func(req *http.Request) {},
	}, testPrices(t), sink, slog.Default())
	p.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/relay/openrouter/api/v1/chat/completions", strings.NewReader("{}")))

	events := sink.all()
	if len(events) != 1 {
		t.Fatalf("recorded %d events, want 1 even when unpriced", len(events))
	}
	if events[0].CostKnown {
		t.Error("an unlisted model should report cost_known=false, not a fabricated price")
	}
	if events[0].InputTokens != 10 {
		t.Errorf("tokens lost for an unpriced model: %+v", events[0])
	}
}

// The response body must reach the caller byte-for-byte: metering reads it, so
// a bug that consumed it without restoring would return empty bodies to every
// client while looking healthy.
func TestOpenRouterPassesTheBodyThrough(t *testing.T) {
	const body = `{"model":"deepseek/deepseek-v4-flash","choices":[{"message":{"content":"hello"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer upstream.Close()

	p, _ := newProvider(ProviderConfig{
		Name: RouteOpenRouter, Target: upstream.URL,
		Extract:    staticExtract(extract.OpenAIExtract),
		InjectAuth: func(req *http.Request) {},
	}, testPrices(t), &capturingSink{}, slog.Default())

	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/relay/openrouter/api/v1/chat/completions", strings.NewReader("{}")))

	got, _ := io.ReadAll(rec.Body)
	if string(got) != body {
		t.Errorf("body was altered in transit:\n got %s\nwant %s", got, body)
	}
}

// On this branch an unconfigured provider is still mounted but refuses at
// request time (nil InjectAuth), which is how every other provider behaves —
// the point is that a keyless relay never forwards an unauthenticated request
// to the vendor.
func TestOpenRouterRefusesWithoutAKey(t *testing.T) {
	r, err := New(testPrices(t), &capturingSink{}, slog.Default(), Keys{
		OpenAI: "sk-openai", Google: "goog", OpenRouter: "",
	})
	if err != nil {
		t.Fatal(err)
	}
	p, ok := r.providers[RouteOpenRouter]
	if !ok {
		t.Fatal("route should exist so the refusal is explicit rather than a bare 404")
	}
	if p.injectAuth != nil {
		t.Error("provider has an injector despite no key being configured")
	}
}

// The route name is half of a contract with the Go backend, which builds
// ".../v1/relay/openrouter/api/v1/". Renaming either side silently stops
// metering rather than failing a build.
func TestOpenRouterRouteNameIsStable(t *testing.T) {
	if RouteOpenRouter != "openrouter" {
		t.Errorf("RouteOpenRouter = %q; the backend builds its base URL from this literal", RouteOpenRouter)
	}
	if openRouterTarget != "https://openrouter.ai" {
		t.Errorf("target = %q, want the bare host (OpenRouter serves under /api/v1)", openRouterTarget)
	}
}
