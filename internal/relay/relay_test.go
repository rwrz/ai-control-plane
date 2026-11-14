package relay

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rwrz/ai-control-plane/internal/pricing"
	"github.com/rwrz/ai-control-plane/internal/relay/extract"
)

const upstreamBody = `{"model":"gpt-4o-mini","usage":{"prompt_tokens":20,"completion_tokens":1,"total_tokens":21}}`

// recordingSink captures events for assertions. Record is called synchronously
// from modifyResponse for non-streaming responses, so no locking is needed.
type recordingSink struct{ events []Event }

func (s *recordingSink) Record(_ context.Context, ev Event) { s.events = append(s.events, ev) }

// newTestProvider wires a Provider in front of upstream with a known price table.
func newTestProvider(t *testing.T, upstreamURL string) (*Provider, *recordingSink) {
	t.Helper()

	prices := &pricing.Config{Models: map[string]pricing.ModelPrice{
		"gpt-4o-mini": {InputPerMtok: 0.15, OutputPerMtok: 0.60},
	}}
	sink := &recordingSink{}

	p, err := newProvider(ProviderConfig{
		Name:       "openai",
		Target:     upstreamURL,
		Extract:    staticExtract(extract.OpenAIExtract),
		InjectAuth: func(*http.Request) {},
	}, prices, sink, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("newProvider: %v", err)
	}
	return p, sink
}

// TestMetersGzippedResponse is a regression test for a silent metering failure.
//
// Every Go/Python SDK sends "Accept-Encoding: gzip". If the relay forwards that
// header upstream, the provider gzips its response and Go does NOT transparently
// decompress it (it only does so for the header it added itself) — so usage
// extraction parses gzip bytes as JSON, fails, and records nothing. The caller
// still decompresses on its own end and sees a perfectly valid response, so the
// metering gap is invisible. curl hid the bug by not requesting gzip at all.
func TestMetersGzippedResponse(t *testing.T) {
	var upstreamSawAcceptEncoding string

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamSawAcceptEncoding = r.Header.Get("Accept-Encoding")
		w.Header().Set("Content-Type", "application/json")

		if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			w.Header().Set("Content-Encoding", "gzip")
			gz := gzip.NewWriter(w)
			defer gz.Close()
			_, _ = io.WriteString(gz, upstreamBody)
			return
		}
		_, _ = io.WriteString(w, upstreamBody)
	}))
	defer upstream.Close()

	p, sink := newTestProvider(t, upstream.URL)
	relaySrv := httptest.NewServer(p)
	defer relaySrv.Close()

	req, err := http.NewRequest(http.MethodPost, relaySrv.URL+"/v1/relay/openai/v1/chat/completions", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Accept-Encoding", "gzip") // what every SDK sends
	req.Header.Set("X-Usage-Tenant", "backend")
	req.Header.Set("X-Usage-User", "user-1")
	req.Header.Set("X-Usage-Module", "rating")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("relay request: %v", err)
	}
	// Drain so modifyResponse completes before we assert.
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	if len(sink.events) != 1 {
		t.Fatalf("expected exactly 1 metered event, got %d (upstream saw Accept-Encoding=%q)",
			len(sink.events), upstreamSawAcceptEncoding)
	}

	ev := sink.events[0]
	if ev.InputTokens != 20 || ev.OutputTokens != 1 {
		t.Errorf("tokens = in:%d out:%d, want in:20 out:1", ev.InputTokens, ev.OutputTokens)
	}
	if ev.Model != "gpt-4o-mini" {
		t.Errorf("model = %q, want %q", ev.Model, "gpt-4o-mini")
	}
	if !ev.CostKnown {
		t.Error("CostKnown = false, want true for a priced model")
	}
	if ev.Tenant != "backend" || ev.User != "user-1" || ev.Module != "rating" {
		t.Errorf("attribution = tenant:%q user:%q module:%q, want backend/user-1/rating",
			ev.Tenant, ev.User, ev.Module)
	}
}

// Attribution headers are internal to the control plane and must never reach the
// provider.
func TestStripsUsageHeadersBeforeUpstream(t *testing.T) {
	var upstreamHeaders http.Header

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHeaders = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, upstreamBody)
	}))
	defer upstream.Close()

	p, _ := newTestProvider(t, upstream.URL)
	relaySrv := httptest.NewServer(p)
	defer relaySrv.Close()

	req, err := http.NewRequest(http.MethodPost, relaySrv.URL+"/v1/relay/openai/v1/chat/completions", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-Usage-Tenant", "backend")
	req.Header.Set("X-Usage-User", "user-1")
	req.Header.Set("Authorization", "Bearer caller-credential")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("relay request: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	for header := range upstreamHeaders {
		if strings.HasPrefix(http.CanonicalHeaderKey(header), "X-Usage-") {
			t.Errorf("attribution header %q leaked upstream", header)
		}
	}
	if got := upstreamHeaders.Get("Authorization"); got == "Bearer caller-credential" {
		t.Error("caller credential leaked upstream; relay must inject its own")
	}
}

// TestRefusesProviderWithoutCredential is a regression test for a failure that
// took an evening to diagnose in staging.
//
// The relay owns the provider credentials — the Director drops the caller's
// Authorization header and injects ours — so an unset key cannot be compensated
// for by the caller having one. Previously a missing key still built a working
// proxy that forwarded an EMPTY credential, and the provider answered with its
// own auth error (Gemini: "Method doesn't allow unregistered callers"). That
// surfaces at the consumer as an opaque 403 from a service that looks healthy,
// pointing whoever is debugging at the wrong end of the connection.
//
// A nil InjectAuth is the single source of truth for "no key configured";
// deriving it from the injector keeps it from drifting out of sync with the
// credential the Director would actually apply.
func TestRefusesProviderWithoutCredential(t *testing.T) {
	var upstreamCalled bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	sink := &recordingSink{}
	p, err := newProvider(ProviderConfig{
		Name:       "gemini",
		Target:     upstream.URL,
		Extract:    staticExtract(extract.GeminiExtract),
		InjectAuth: nil, // no key configured
	}, &pricing.Config{}, sink, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("newProvider: %v", err)
	}

	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/relay/gemini/v1beta/models/x:generateContent", strings.NewReader("{}")))

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 — an unconfigured provider must fail here, not at the upstream", rec.Code)
	}
	if upstreamCalled {
		t.Error("request reached the upstream without a credential; it must be refused before leaving the relay")
	}
	if !strings.Contains(rec.Body.String(), "gemini") {
		t.Errorf("body = %q, want it to name the unconfigured provider", rec.Body.String())
	}
	if len(sink.events) != 0 {
		t.Errorf("recorded %d events for a refused request, want 0", len(sink.events))
	}
}

// TestLogsUpstreamErrorBody covers the diagnostic gap that made a production
// incident take hours instead of one log line: on 2026-08-04 the relay repeated
// `upstream rejected request status=400` for two and a half hours without ever
// saying why, while OpenAI's body named the offending parameter in plain text.
//
// Two properties, and the second is the one that could break production: the
// caller must still receive the provider's error payload intact, because every
// SDK parses it to build the error it raises.
func TestLogsUpstreamErrorBody(t *testing.T) {
	const errBody = `{"error":{"message":"Unrecognized request argument supplied: reasoning_effort","type":"invalid_request_error"}}`

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, errBody)
	}))
	defer upstream.Close()

	var logs bytes.Buffer
	p, sink := newTestProvider(t, upstream.URL)
	p.log = slog.New(slog.NewTextHandler(&logs, nil))

	relaySrv := httptest.NewServer(p)
	defer relaySrv.Close()

	resp, err := http.Post(relaySrv.URL+"/v1/relay/openai/v1/chat/completions",
		"application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("caller saw status %d, want 400", resp.StatusCode)
	}
	// The caller's copy must be byte-identical: the relay read it to log it.
	if string(got) != errBody {
		t.Errorf("caller received a mangled body:\n got: %q\nwant: %q", got, errBody)
	}
	if !strings.Contains(logs.String(), "reasoning_effort") {
		t.Errorf("the reason was not logged; log was:\n%s", logs.String())
	}
	// A rejected call is not spend.
	if len(sink.events) != 0 {
		t.Errorf("recorded %d events for a failed call, want 0", len(sink.events))
	}
}

// TestOversizedErrorBodyIsTruncatedButStillDelivered pins the bound. A provider
// that echoes a large request back must not put all of it in our logs, and must
// still reach the caller whole.
func TestOversizedErrorBodyIsTruncatedButStillDelivered(t *testing.T) {
	huge := `{"error":"` + strings.Repeat("x", 8<<10) + `"}`

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_, _ = io.WriteString(w, huge)
	}))
	defer upstream.Close()

	var logs bytes.Buffer
	p, _ := newTestProvider(t, upstream.URL)
	p.log = slog.New(slog.NewTextHandler(&logs, nil))

	relaySrv := httptest.NewServer(p)
	defer relaySrv.Close()

	resp, err := http.Post(relaySrv.URL+"/v1/relay/openai/v1/chat/completions",
		"application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	got, _ := io.ReadAll(resp.Body)

	if len(got) != len(huge) {
		t.Errorf("caller received %d bytes, want the whole %d-byte body", len(got), len(huge))
	}
	if logs.Len() > 2*maxErrorBodyBytes {
		t.Errorf("log line is %d bytes; the body cap of %d is not being applied",
			logs.Len(), maxErrorBodyBytes)
	}
}

// scribeBody is a real Scribe (ElevenLabs STT) response shape. Note what it does
// NOT contain: the model. That absence is the whole point of the test below.
const scribeBody = `{"language_code":"eng","text":"hello there",` +
	`"words":[{"end":2.5,"speaker_id":"speaker_0"}]}`

// TestScribeIsPricedFromTheCallerDeclaredModel is a regression test for a
// metering gap that cost the entire ElevenLabs STT bill.
//
// Scribe names its model nowhere the relay can reach: the response omits it, and
// the request is multipart carrying audio up to 5GB that this relay refuses to
// buffer to read one form field. So ElevenLabsSTTExtract returns usage with an
// empty Model, and the caller's X-Usage-Model hint is the ONLY source there is.
//
// Undeclared, the row is not merely unpriced — the central control plane rejects
// it outright ("model is required", see usage.ReportedEvent.validate), so a
// sidecar records it, forwards it, and the centre throws it away. Every
// transcription in a coaching session vanished that way while LLM and TTS rows
// landed normally, which is exactly how it stayed hidden.
func TestScribeIsPricedFromTheCallerDeclaredModel(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, scribeBody)
	}))
	defer upstream.Close()

	newScribeProvider := func(t *testing.T) (*Provider, *recordingSink) {
		t.Helper()
		prices := &pricing.Config{Models: map[string]pricing.ModelPrice{
			"scribe_v2": {PerAudioHour: 0.22},
		}}
		sink := &recordingSink{}
		p, err := newProvider(ProviderConfig{
			Name:       "elevenlabs",
			Target:     upstream.URL,
			Extract:    elevenLabsExtractorFor,
			InjectAuth: func(*http.Request) {},
		}, prices, sink, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatalf("newProvider: %v", err)
		}
		return p, sink
	}

	call := func(t *testing.T, declaredModel string) Event {
		t.Helper()
		p, sink := newScribeProvider(t)
		srv := httptest.NewServer(p)
		defer srv.Close()

		req, err := http.NewRequest(http.MethodPost,
			srv.URL+"/v1/relay/elevenlabs/v1/speech-to-text", strings.NewReader("{}"))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("X-Usage-Tenant", "voice_agent")
		if declaredModel != "" {
			req.Header.Set("X-Usage-Model", declaredModel)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("relay request: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		if len(sink.events) != 1 {
			t.Fatalf("expected exactly 1 metered event, got %d", len(sink.events))
		}
		return sink.events[0]
	}

	t.Run("declared model prices the row", func(t *testing.T) {
		ev := call(t, "scribe_v2")

		if ev.Model != "scribe_v2" {
			t.Errorf("model = %q, want %q — the hint did not fill the gap", ev.Model, "scribe_v2")
		}
		if ev.Modality != extract.ModalitySTT {
			t.Errorf("modality = %q, want %q", ev.Modality, extract.ModalitySTT)
		}
		if ev.AudioSeconds != 2.5 {
			t.Errorf("audio seconds = %v, want 2.5", ev.AudioSeconds)
		}
		if !ev.CostKnown || ev.CostUSD <= 0 {
			t.Errorf("cost = %v (known=%v), want a priced row", ev.CostUSD, ev.CostKnown)
		}
	})

	// The failing shape, pinned so it cannot come back unnoticed: the call is
	// still relayed and the usage still measured, but with no model the row is
	// unpriced here and refused by the centre.
	t.Run("undeclared model leaves the row unnamed", func(t *testing.T) {
		ev := call(t, "")

		if ev.Model != "" {
			t.Errorf("model = %q, want empty — Scribe reports none and none was declared", ev.Model)
		}
		if ev.AudioSeconds != 2.5 {
			t.Errorf("audio seconds = %v, want 2.5 — usage is measured either way", ev.AudioSeconds)
		}
		if ev.CostKnown {
			t.Error("CostKnown = true, want false: an unnamed model must never price as free")
		}
	})
}

// TestStatedCostOutranksTheRateTable pins the ElevenLabs Scribe pricing path.
//
// Scribe is the one relayed call where the provider quotes its own price, and
// where our reconstruction is measurably wrong: ElevenLabs bills whole ~2.9752s
// credits and adds an invisible 20% for keyterms, neither of which a duration
// times a rate can express. Measured on a live session the reconstruction ran
// about 27% under.
func TestStatedCostOutranksTheRateTable(t *testing.T) {
	prices := &pricing.Config{Models: map[string]pricing.ModelPrice{
		"scribe_v2": {PerAudioHour: 0.22},
	}}
	stated := func(v float64) *float64 { return &v }

	// 10s of audio at $0.22/hour is $0.000611 by the rate table. The provider
	// says $0.001 — quantised into credits, and 4 credits not 3.
	t.Run("the quoted price wins", func(t *testing.T) {
		cost, known := PriceUsage(prices, "scribe_v2", extract.ModalitySTT, &extract.Usage{
			Modality: extract.ModalitySTT, AudioSeconds: 10, ProviderCostUSD: stated(0.001),
		})
		if !known || cost != 0.001 {
			t.Errorf("cost = %v (known=%v), want 0.001, true", cost, known)
		}
	})

	// The regression this guards: the stated figure ALREADY includes keyterms
	// and entity/speaker surcharges. Scaling it again by our inferred multiplier
	// would bill those premiums twice.
	t.Run("the surcharge is not applied twice", func(t *testing.T) {
		cost, _ := PriceUsage(prices, "scribe_v2", extract.ModalitySTT, &extract.Usage{
			Modality: extract.ModalitySTT, AudioSeconds: 10,
			SurchargeMultiplier: 1.3, ProviderCostUSD: stated(0.001),
		})
		if cost != 0.001 {
			t.Errorf("cost = %v, want 0.001 — the surcharge was applied on top", cost)
		}
	})

	// A quoted price makes a model priceable that the rate table cannot cover.
	// scribe_v2_realtime has no pricing.toml entry and bills at a higher tier;
	// without this it lands unpriced while the provider is stating the number.
	t.Run("an unlisted model is priced, not dropped", func(t *testing.T) {
		cost, known := PriceUsage(prices, "scribe_v2_realtime", extract.ModalitySTT, &extract.Usage{
			Modality: extract.ModalitySTT, AudioSeconds: 10, ProviderCostUSD: stated(0.002),
		})
		if !known || cost != 0.002 {
			t.Errorf("cost = %v (known=%v), want 0.002, true", cost, known)
		}
	})

	// Zero is a quoted price. Sub-credit audio genuinely costs nothing, and
	// treating that as "no quote" would silently fall back to the rate table
	// and bill for it.
	t.Run("a quoted zero is honoured, not treated as absent", func(t *testing.T) {
		cost, known := PriceUsage(prices, "scribe_v2", extract.ModalitySTT, &extract.Usage{
			Modality: extract.ModalitySTT, AudioSeconds: 0.5, ProviderCostUSD: stated(0),
		})
		if !known || cost != 0 {
			t.Errorf("cost = %v (known=%v), want 0, true", cost, known)
		}
	})

	// Providers that quote nothing must be untouched by any of this.
	t.Run("no quote falls back to the rate table", func(t *testing.T) {
		cost, known := PriceUsage(prices, "scribe_v2", extract.ModalitySTT, &extract.Usage{
			Modality: extract.ModalitySTT, AudioSeconds: 3600,
		})
		// rate/3600*3600 does not land exactly on the rate in binary floating point.
		if !known || math.Abs(cost-0.22) > 1e-9 {
			t.Errorf("cost = %v (known=%v), want ~0.22, true", cost, known)
		}
	})
}

// TestScribeIsBilledAtTheQuotedPrice is the end-to-end half: headers on a real
// relayed response reaching Event.CostUSD.
//
// The unit tests above prove PriceUsage prefers a quoted price; this proves the
// quote is actually picked up off the wire. Between them sits stampStatedCost,
// and a provider whose StatedCost hook was never wired would fail here alone —
// silently, by falling back to the rate table and looking merely inaccurate.
func TestScribeIsBilledAtTheQuotedPrice(t *testing.T) {
	// Verbatim from a live Scribe call: 1.85s of audio, which ElevenLabs
	// quantised to one ~2.9752s credit at 1/5500 USD.
	const (
		statedCost = 0.00018182
		body       = `{"language_code":"eng","text":"hello there",` +
			`"words":[{"end":1.799,"speaker_id":"speaker_0"}]}`
	)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("character-cost", "1")
		w.Header().Set("fiat-cost-before-overages", "0.00018182")
		w.Header().Set("fiat-currency", "usd")
		_, _ = io.WriteString(w, body)
	}))
	defer upstream.Close()

	prices := &pricing.Config{Models: map[string]pricing.ModelPrice{
		"scribe_v2": {PerAudioHour: 0.22},
	}}
	sink := &recordingSink{}
	p, err := newProvider(ProviderConfig{
		Name:       "elevenlabs",
		Target:     upstream.URL,
		Extract:    elevenLabsExtractorFor,
		StatedCost: elevenLabsStatedCost,
		InjectAuth: func(*http.Request) {},
	}, prices, sink, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("newProvider: %v", err)
	}

	srv := httptest.NewServer(p)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost,
		srv.URL+"/v1/relay/elevenlabs/v1/speech-to-text", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-Usage-Tenant", "voice_agent")
	req.Header.Set("X-Usage-Model", "scribe_v2")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("relay request: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	if len(sink.events) != 1 {
		t.Fatalf("expected exactly 1 metered event, got %d", len(sink.events))
	}
	ev := sink.events[0]

	if ev.ProviderCostUSD == nil {
		t.Fatal("ProviderCostUSD is nil: the quoted price never left the response headers")
	}
	if *ev.ProviderCostUSD != statedCost || ev.CostUSD != statedCost {
		t.Errorf("stated=%v cost=%v, want both %v", *ev.ProviderCostUSD, ev.CostUSD, statedCost)
	}
	if !ev.CostKnown {
		t.Error("CostKnown = false on a call the provider itself priced")
	}
	// The quantity is still recorded: it is what reconciles against an invoice
	// line, and the quoted price does not replace it.
	if ev.AudioSeconds != 1.799 {
		t.Errorf("audio seconds = %v, want 1.799 — the measurement was lost", ev.AudioSeconds)
	}
	// And the rate table would have said something materially lower — which is
	// the whole reason to prefer the quote.
	if reconstructed := 0.22 * 1.799 / 3600; ev.CostUSD <= reconstructed {
		t.Errorf("cost %v did not exceed the reconstruction %v; the quote was ignored",
			ev.CostUSD, reconstructed)
	}
}

// The production spec must actually carry the hook. Every other test in this
// file builds its own ProviderConfig, so removing StatedCost from the real
// elevenlabs entry in New() would break nothing and quietly return Scribe to
// being priced from the rate table.
func TestElevenLabsSpecWiresTheStatedCostHook(t *testing.T) {
	r, err := New(&pricing.Config{}, &recordingSink{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		Keys{ElevenLabs: "k"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p, ok := r.providers["elevenlabs"]
	if !ok {
		t.Fatal("no elevenlabs provider")
	}
	if p.statedCost == nil {
		t.Fatal("elevenlabs has no StatedCost hook; Scribe is back on the rate table")
	}
	if p.statedCost("/v1/speech-to-text") == nil {
		t.Error("no stated-cost reader for the Scribe path")
	}
	// TTS quotes credits, not money, and must keep using pricing.toml.
	if p.statedCost("/v1/text-to-speech/voice") != nil {
		t.Error("text-to-speech must not claim a stated cost: it quotes no fiat figure")
	}
}

// newScribeProvider wires an ElevenLabs provider — extractor and stated-cost
// hook both — in front of a fake Scribe.
func newScribeProvider(t *testing.T, upstreamURL string) (*Provider, *recordingSink) {
	t.Helper()

	sink := &recordingSink{}
	p, err := newProvider(ProviderConfig{
		Name:       "elevenlabs",
		Target:     upstreamURL,
		Extract:    elevenLabsExtractorFor,
		StatedCost: elevenLabsStatedCost,
		InjectAuth: func(*http.Request) {},
	}, &pricing.Config{Models: map[string]pricing.ModelPrice{
		"scribe_v2": {PerAudioHour: 0.22},
	}}, sink, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("newProvider: %v", err)
	}
	return p, sink
}

// callScribe relays one transcription request, optionally declaring hints.
func callScribe(t *testing.T, p *Provider, hints string) {
	t.Helper()

	srv := httptest.NewServer(p)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost,
		srv.URL+"/v1/relay/elevenlabs/v1/speech-to-text", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-Usage-Tenant", "voice_agent")
	req.Header.Set("X-Usage-Model", "scribe_v2")
	if hints != "" {
		req.Header.Set("X-Usage-Request", hints)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("relay request: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

// An empty Scribe transcript: what a VAD segment that caught noise rather than
// speech comes back as. Scribe reports no duration of its own and the relay
// reconstructs one from word timings, so no words means no quantity at all.
const wordlessScribeBody = `{"language_code":"eng","language_probability":0.31,"text":"","words":[]}`

// A call with nothing to bill and nobody quoting a price is not a free call —
// it is nothing at all, and it must not become a row.
//
// Recorded, it travels: a sidecar ships it to a centre whose ingest rejects it
// for carrying no billable quantity, which is exactly how this was found. In
// voice-agent production it was running at roughly one rejected row per six
// accepted, every fifteen-second flush.
//
// This is NOT the unpriced case below it in record(). Unpriced is a real
// quantity missing a rate and must be kept; this is a call the ledger can
// neither price nor reconcile nor show anything about.
func TestCallWithNothingBillableIsNotRecorded(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, wordlessScribeBody)
	}))
	defer upstream.Close()

	p, sink := newScribeProvider(t, upstream.URL)
	callScribe(t, p, "")

	if len(sink.events) != 0 {
		t.Errorf("recorded %d events, want none: %+v", len(sink.events), sink.events)
	}
}

// The same wordless response with ElevenLabs' own price on it is real spend.
//
// This is the shape production actually returns: Scribe quantises to whole
// ~2.9752s credits, so a noise-only segment still costs a credit and still comes
// back with fiat-cost-before-overages set. Dropping it because the reconstructed
// duration is zero would discard money the provider will invoice.
func TestCallWithNoQuantityIsRecordedWhenTheProviderQuotesAPrice(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("fiat-cost-before-overages", "0.00018182")
		w.Header().Set("fiat-currency", "usd")
		_, _ = io.WriteString(w, wordlessScribeBody)
	}))
	defer upstream.Close()

	p, sink := newScribeProvider(t, upstream.URL)
	callScribe(t, p, "")

	if len(sink.events) != 1 {
		t.Fatalf("recorded %d events, want the quoted call kept", len(sink.events))
	}
	if ev := sink.events[0]; ev.CostUSD != 0.00018182 || !ev.CostKnown {
		t.Errorf("cost = %v (known=%v), want the quoted 0.00018182", ev.CostUSD, ev.CostKnown)
	}
}

// The drop has to happen AFTER the caller's hints are folded in.
//
// A consumer that decoded the audio knows the duration Scribe never reports, and
// declaring it is the documented way to correct exactly this reconstruction.
// Checking for emptiness before Apply would throw the call away one line before
// its quantity arrived — and the row would vanish for the callers doing the most
// to be metered accurately.
func TestDeclaredDurationRescuesAWordlessTranscript(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, wordlessScribeBody)
	}))
	defer upstream.Close()

	p, sink := newScribeProvider(t, upstream.URL)
	callScribe(t, p, `{"audio_seconds":0.8}`)

	if len(sink.events) != 1 {
		t.Fatalf("recorded %d events, want the declared duration to keep the row", len(sink.events))
	}
	if ev := sink.events[0]; ev.AudioSeconds != 0.8 {
		t.Errorf("audio seconds = %v, want the declared 0.8", ev.AudioSeconds)
	}
}
