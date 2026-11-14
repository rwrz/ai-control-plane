// Package relay is the AI-aware reverse proxy: it forwards each provider's
// native protocol untouched, injects the real upstream credential, and tees the
// (streaming or not) response to extract token usage and price it. Traefik sits
// in front for TLS/routing/WS passthrough; all AI awareness lives here.
package relay

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rwrz/ai-control-plane/internal/pricing"
	"github.com/rwrz/ai-control-plane/internal/relay/extract"
)

// Meta is the attribution carried per request. For Phase 1 (HTTP/SSE) it rides
// in X-Usage-* headers; Phase 2 (WS audio) introduces virtual keys.
type Meta struct {
	Tenant  string
	User    string
	Feature string
	Module  string
	// EntityType and EntityID name the unit of work whose calls are summed
	// together — a coaching session, a call, an agent conversation. Both or
	// neither: an id without a type cannot be grouped with anything.
	EntityType string
	EntityID   string
	Provider   string
	// ModelHint is the caller-declared model, used ONLY when the response does
	// not name one. Whisper's verbose_json and every TTS response (raw audio)
	// are silent about the model, so without a hint those calls can be measured
	// but not priced. Where the provider does report a model, it wins — a hint
	// is a fallback, never an override.
	ModelHint string
	// Hints are the caller's request parameters minus any binary payload, from
	// X-Usage-Request. They supply billable facts no response reports — Scribe's
	// real audio duration, ElevenLabs' keyterms surcharge — and can only fill
	// gaps, never overwrite a provider-measured quantity. See extract.Hints.
	Hints *extract.Hints
}

// Usage sources. How a row was obtained decides how much it can be trusted,
// and the two must stay distinguishable rather than being silently mixed:
// "what we measured" and "what someone told us" answer different questions,
// and only the first can be reconciled against a provider invoice.
const (
	// SourceRelayed: the control plane proxied the call and read the quantity
	// out of the provider's own bytes.
	SourceRelayed = "relayed"
	// SourceReported: a consumer asserted it from its own accounting, because
	// proxying it would have cost more than the measurement is worth — a
	// real-time voice path where an extra network hop lands on the user.
	SourceReported = "reported"
)

// Event is one priced usage record handed to a Sink.
type Event struct {
	Meta
	// Source is SourceRelayed or SourceReported. Empty is treated as relayed,
	// so the relay's own call sites need not set it.
	Source string
	// ClientEventID is a reporting consumer's idempotency key, empty for
	// relayed events, which are measured exactly once by construction.
	ClientEventID     string
	Modality          string
	Model             string
	InputTokens       int64
	CachedInputTokens int64
	OutputTokens      int64
	TotalTokens       int64
	AudioSeconds      float64 // stt: audio processed
	Characters        int64   // tts: characters synthesised
	// SurchargeMultiplier scales cost for provider options that bill above the
	// base rate. Kept beside the cost it produced so a row can be re-priced
	// elsewhere — a sidecar forwards quantities, and the multiplier is the one
	// part of the price that is not derivable from them.
	SurchargeMultiplier float64
	// ProviderCostUSD is the price the provider stated for this call, when it
	// stated one — see extract.Usage.ProviderCostUSD. Carried on the Event so a
	// SIDECAR can forward it: the centre re-prices from quantities by design, and
	// without this the provider's own figure would be recomputed away on the hop.
	ProviderCostUSD *float64
	CostUSD         float64
	CostKnown       bool
	OccurredAt      time.Time
}

// SourceOf resolves an event's source, defaulting an unset one to relayed.
//
// The relay's own call sites predate the column and leave it empty, so empty
// must mean measured here. Note that the WIRE format defaults the opposite way
// (see usage.ReportedEvent.Source): a sender that says nothing has measured
// nothing. The two defaults are deliberately opposed and must not be confused —
// this one is for events produced in-process, that one for events arriving from
// outside.
func SourceOf(ev Event) string {
	if ev.Source == "" {
		return SourceRelayed
	}
	return ev.Source
}

// Sink persists usage events. Implementations must be non-blocking / fast — the
// relay calls Record off the response path.
type Sink interface {
	Record(context.Context, Event)
}

type metaKey struct{}

// reqUsageKey carries usage read from the request body (TTS) forward to
// modifyResponse, so it is only billed once the call is known to have succeeded.
type reqUsageKey struct{}

// Provider is one upstream (openai, gemini, ...) fronted by a reverse proxy.
type Provider struct {
	name           string
	extract        func(path string) extract.Extractor
	extractRequest func(path string) extract.RequestExtractor
	streamMeter    extract.StreamMeterFor
	statedCost     func(path string) func(http.Header) (float64, bool)
	pricing        *pricing.Config
	sink           Sink
	log            *slog.Logger
	proxy          *httputil.ReverseProxy
	credential     bool // an upstream key was configured for this provider
	keyring        *Keyring
	authMode       string // AuthEnforce or AuthShadow
	sockets        *activeSockets

	// WebSocket relaying bypasses the ReverseProxy and dials the upstream
	// itself, so it needs the same three facts the Director closes over.
	target     *url.URL
	prefix     string
	injectAuth func(*http.Request)
}

// ProviderConfig configures a Provider.
type ProviderConfig struct {
	Name   string
	Target string // e.g. https://api.openai.com
	// Extract selects the response extractor for an upstream path. It is a
	// function of the path because one host can bill several ways: OpenAI's
	// /v1/chat/completions is tokens-as-LLM while /v1/audio/transcriptions is
	// seconds-as-STT, and gpt-4o-transcribe returns a token payload the LLM
	// extractor would happily mis-read as a chat completion. Use staticExtract
	// for a provider with a single shape.
	Extract func(path string) extract.Extractor
	// ExtractRequest selects a REQUEST-body extractor for a path, for TTS
	// providers whose cost is the text submitted. Path-selected like Extract,
	// so a Scribe audio upload on the same host is never buffered and parsed as
	// a TTS payload. Optional; nil for everything else.
	ExtractRequest func(path string) extract.RequestExtractor
	// StreamMeter selects a meter for a WebSocket conversation on a path. Nil
	// means this provider has no sockets; a nil RESULT means this path has none.
	// Either way the socket is still relayed, just unmetered — a provider whose
	// audio the relay cannot price should still carry the caller's audio.
	StreamMeter extract.StreamMeterFor
	// StatedCost reads a price the provider put in its own response headers,
	// which outranks anything priced from pricing.toml (see Usage.ProviderCostUSD).
	//
	// Path-selected like the extractors because one host does not answer the
	// same way everywhere: ElevenLabs states a fiat cost on Scribe but only a
	// credit count on text-to-speech, and a credit count is not money until you
	// also know the account's rate. Optional; nil for every provider that quotes
	// no price, which is most of them — Deepgram's socket carries no cost field
	// at all.
	StatedCost func(path string) func(http.Header) (float64, bool)
	// InjectAuth sets the real upstream credential. A nil value means no key was
	// configured, and the provider refuses requests rather than forwarding an
	// empty credential — the upstream would otherwise answer with its own auth
	// error, which reads at the caller like a broken relay. Deriving this from
	// the injector itself keeps it from drifting out of sync with a separate flag.
	InjectAuth func(*http.Request)
	// Keyring authenticates callers. A disabled keyring lets every request
	// through, which is only safe while the relay is unreachable from outside.
	Keyring *Keyring
	// AuthMode is AuthEnforce (default) or AuthShadow.
	AuthMode string
	// Sockets counts in-flight WebSocket relays so shutdown can wait for them.
	Sockets *activeSockets
}

func newProvider(cfg ProviderConfig, prices *pricing.Config, sink Sink, log *slog.Logger) (*Provider, error) {
	target, err := url.Parse(cfg.Target)
	if err != nil {
		return nil, err
	}
	prefix := "/v1/relay/" + cfg.Name
	p := &Provider{
		name:           cfg.Name,
		extract:        cfg.Extract,
		extractRequest: cfg.ExtractRequest,
		streamMeter:    cfg.StreamMeter,
		statedCost:     cfg.StatedCost,
		pricing:        prices,
		sink:           sink,
		log:            log.With("provider", cfg.Name),
		credential:     cfg.InjectAuth != nil,
		keyring:        cfg.Keyring,
		authMode:       cfg.AuthMode,
		sockets:        cfg.Sockets,
		target:         target,
		prefix:         prefix,
		injectAuth:     cfg.InjectAuth,
	}
	if p.streamMeter == nil {
		p.streamMeter = func(*url.URL) extract.StreamMeter { return nil }
	}
	p.proxy = &httputil.ReverseProxy{
		FlushInterval: -1, // stream SSE chunks immediately
		Director: func(req *http.Request) {
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			req.Host = target.Host
			req.URL.Path = strings.TrimPrefix(req.URL.Path, prefix)
			req.Header.Del("Authorization") // drop caller/virtual credential

			// Drop the caller's Accept-Encoding so OUR transport owns
			// compression. Go only decompresses transparently when it added the
			// header itself; forwarding the caller's (every SDK sends
			// "gzip") hands modifyResponse a gzipped body that fails to parse —
			// metering silently records nothing while the caller, decompressing
			// on its own end, still sees a perfectly good response.
			req.Header.Del("Accept-Encoding")

			// Guarded in ServeHTTP; checked again so a Provider driven directly
			// cannot silently forward an unauthenticated request.
			if cfg.InjectAuth != nil {
				cfg.InjectAuth(req)
			}
		},
		ModifyResponse: p.modifyResponse,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			// A caller that hangs up mid-stream is not an upstream failure, and
			// on a voice path it is routine: every barge-in cancels the
			// in-flight generation. Logging it at ERROR trains whoever reads
			// these logs to skip the level that matters.
			//
			// Usage is not lost when this happens — the SSE tee records what it
			// parsed when the body is closed, and both Gemini and OpenAI report
			// usage cumulatively per chunk, so an interrupted generation is
			// still billed for what it produced.
			if errors.Is(err, context.Canceled) {
				p.log.Debug("caller cancelled request", "path", r.URL.Path)
				return
			}
			p.log.Error("upstream error", "err", err, "path", r.URL.Path)
			// Only meaningful before the response has begun. Once bytes are
			// streaming, the status is long since sent and this would be a
			// "superfluous WriteHeader" on the way out.
			w.WriteHeader(http.StatusBadGateway)
		},
	}
	return p, nil
}

// ServeHTTP injects attribution into the request context, strips the X-Usage-*
// headers so they never reach the provider, and proxies.
func (p *Provider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Fail here rather than forwarding an empty credential. Upstreams answer
	// that with their own auth error (Gemini: "Method doesn't allow unregistered
	// callers"), which surfaces at the caller as an opaque 403 and sends whoever
	// is debugging looking at the wrong end of the connection.
	if !p.credential {
		p.log.Error("no upstream credential configured; refusing to relay",
			"module", r.Header.Get("X-Usage-Module"), "path", r.URL.Path)
		http.Error(w, "relay: no credential configured for provider "+p.name,
			http.StatusServiceUnavailable)
		return
	}

	// Read identity and credential through whichever channel carries them, and
	// strip both from the request, before anything else touches a provider
	// credential. See attribution.go for why there are three channels.
	a := readAttribution(r, p.prefix)
	authTenant, authed := p.keyring.Lookup(a.credential)
	if p.keyring.Enabled() && !authed {
		if p.authMode == AuthShadow {
			// Named here, unlike the enforcing branch, because shadow mode
			// exists precisely to find these callers: the module and tenant it
			// declares are what tell you which deployment still needs a key.
			p.log.Warn("unauthenticated request served (auth mode: shadow)",
				"path", r.URL.Path, "remote", r.RemoteAddr,
				"declared_tenant", a.meta.Tenant, "declared_module", a.meta.Module,
				"presented_credential", a.credential != "")
			authed = false // fall through to the caller-declared attribution
		} else {
			// Deliberately terse: distinguishing "no credential" from "wrong
			// credential" tells an attacker whether a guess was well-formed.
			p.log.Warn("rejected unauthenticated request", "path", r.URL.Path,
				"remote", r.RemoteAddr, "presented_credential", a.credential != "")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	meta := a.meta
	meta.Provider = p.name
	// An authenticated key decides the tenant, overriding whatever the caller
	// claimed. Without this a leaked key could book its spend against another
	// consumer, and per-consumer limits would mean nothing.
	if authed {
		meta.Tenant = authTenant
	}

	// Audio arrives over WebSocket, which the ReverseProxy cannot meter: it
	// forwards a 101 and copies bytes with no way to read frames.
	if isWebSocketUpgrade(r) {
		p.serveWebSocket(w, r, meta)
		return
	}

	// Before anything reads the body: give the transport a way to replay this
	// request, so a connection dying under it is retried instead of failing the
	// call. ReverseProxy's Clone carries GetBody through to the outbound
	// request. See rewind.go — this is the fix for both the HTTP/2 GOAWAY and
	// the HTTP/1.1 stale-keep-alive failures on Deepgram's long-lived calls.
	makeRewindable(r)

	ctx := context.WithValue(r.Context(), metaKey{}, meta)
	if u := p.usageFromRequest(r); u != nil {
		ctx = context.WithValue(ctx, reqUsageKey{}, u)
	}
	p.proxy.ServeHTTP(w, r.WithContext(ctx))
}

// maxRequestExtractBytes caps how much of a request body is buffered to read
// usage from it. TTS payloads are small JSON — the text being synthesised — so
// this only has to be generous, not large. Audio UPLOADS (STT) are megabytes and
// are never extracted from, so they never reach this path.
const maxRequestExtractBytes = 1 << 20 // 1 MiB

// usageFromRequest reads billable quantities out of the request body for TTS
// providers, restoring the body so the upstream still receives it intact.
//
// Returns nil unless this provider bills on the request. The result is stashed
// for modifyResponse rather than recorded here: a request that the provider then
// rejects must not be billed for synthesis that never happened.
func (p *Provider) usageFromRequest(r *http.Request) *extract.Usage {
	if p.extractRequest == nil || r.Body == nil {
		return nil
	}
	ex := p.extractRequest(r.URL.Path)
	if ex == nil {
		return nil
	}
	// Read one byte past the cap so an oversized body is detectable.
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestExtractBytes+1))
	if err != nil {
		// Put back what was read; the upstream will surface any real failure.
		r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), r.Body))
		p.log.Error("could not read request body for usage extraction", "err", err)
		return nil
	}
	if len(body) > maxRequestExtractBytes {
		r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(body), r.Body))
		p.log.Error("request body too large to meter; call will be unmetered",
			"limit_bytes", maxRequestExtractBytes)
		return nil
	}

	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))

	u, ok := ex(body)
	if !ok {
		return nil
	}
	return u
}

// maxErrorBodyBytes bounds how much of a rejected response is read for logging.
// Provider error envelopes are a few hundred bytes; this only has to be
// generous enough to hold one, not to hold a response.
const maxErrorBodyBytes = 2 << 10 // 2 KiB

// prefixedBody re-serves an already-read prefix ahead of the rest of a body,
// while still closing the ORIGINAL body. io.NopCloser would drop that Close and
// leak the upstream connection.
type prefixedBody struct {
	io.Reader
	io.Closer
}

// errorDetail reads a bounded prefix of a rejected response for the log line
// and puts it back, so the caller still receives the provider's error payload
// intact — the SDKs parse it.
//
// Nothing here may fail the response: this runs on an error path that is
// already degraded, and turning a provider's 400 into a relay-level failure
// would hide the very thing being logged.
func (p *Provider) errorDetail(resp *http.Response) string {
	if resp.Body == nil {
		return ""
	}
	// A read error is deliberately ignored: whatever arrived before it still
	// usually names the problem, and the caller's copy fails on its own terms
	// either way.
	prefix, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
	if len(prefix) > 0 {
		// MultiReader rather than replacing the body outright: the remainder
		// may still be arriving, and this leaves Content-Length and every other
		// header describing exactly the bytes the caller ends up receiving.
		resp.Body = prefixedBody{
			Reader: io.MultiReader(bytes.NewReader(prefix), resp.Body),
			Closer: resp.Body,
		}
	}
	return strings.TrimSpace(string(prefix))
}

func (p *Provider) modifyResponse(resp *http.Response) error {
	reqCtx := resp.Request.Context()
	meta, _ := reqCtx.Value(metaKey{}).(Meta)
	if resp.StatusCode != http.StatusOK {
		// A non-200 carries no usage, but staying silent about it means an
		// upstream rejection (bad key, quota, malformed request) is invisible
		// here and only visible to the caller — the relay looks idle while every
		// call fails.
		//
		// The status alone is not enough. On 2026-08-04 this line repeated
		// status=400 for two and a half hours without ever saying why, and the
		// cause had to be found by replaying the backend's outbound body
		// against a local server. OpenAI had been answering with the reason in
		// plain text the whole time:
		//
		//	{"error":{"message":"Unrecognized request argument supplied:
		//	 reasoning_effort", ...}}
		//
		// So log a bounded prefix of the body. The cap is small on purpose: an
		// error envelope is a few hundred bytes, and some providers quote the
		// offending request back, which is caller content that does not belong
		// in our logs wholesale.
		p.log.Error("upstream rejected request",
			"status", resp.StatusCode, "path", resp.Request.URL.Path,
			"tenant", meta.Tenant, "module", meta.Module, "feature", meta.Feature,
			"detail", p.errorDetail(resp))
		return nil
	}
	// TTS: the billable quantity was read from the request, and the response is
	// audio. Record it now that the call is known to have succeeded, and return
	// WITHOUT touching the body — buffering a synthesised MP3 into memory would
	// be pure waste, and there is nothing in it to read anyway.
	if u, ok := reqCtx.Value(reqUsageKey{}).(*extract.Usage); ok && u != nil {
		p.stampStatedCost(resp, u)
		p.record(meta, u)
		return nil
	}

	// Which extractor applies depends on the endpoint; nil means this path has
	// no usage to read (e.g. a TTS call whose request body did not parse). That
	// is deliberately a no-op rather than a body read, so a non-JSON response is
	// never buffered.
	ex := p.extract(resp.Request.URL.Path)
	if ex == nil {
		p.log.Warn("no usage extractor for path; call is unmetered",
			"path", resp.Request.URL.Path, "module", meta.Module)
		return nil
	}

	if strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		resp.Body = extract.NewSSETee(resp.Body, ex, func(u *extract.Usage) {
			p.stampStatedCost(resp, u)
			p.record(meta, u)
		})
		return nil
	}
	// Non-streaming: buffer the (complete) JSON body, extract, restore.
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return err
	}
	if u, ok := ex(body); ok {
		p.stampStatedCost(resp, u)
		p.record(meta, u)
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
	return nil
}

// stampStatedCost records a price the provider quoted in its own response
// headers, for the providers that quote one.
//
// Applied to all three metering paths rather than only the one that needs it
// today: a provider that states a cost on a streamed or request-metered call
// would otherwise be silently ignored, and the failure would look exactly like
// having no header at all. The hook is nil for every provider but ElevenLabs,
// so this is a nil check on every other path.
func (p *Provider) stampStatedCost(resp *http.Response, u *extract.Usage) {
	if u == nil || p.statedCost == nil || resp == nil || resp.Request == nil {
		return
	}
	read := p.statedCost(resp.Request.URL.Path)
	if read == nil {
		return
	}
	if cost, ok := read(resp.Header); ok {
		u.ProviderCostUSD = &cost
	}
}

// PriceUsage turns a measured or reported quantity into a cost.
//
// Exported and standalone because there are now two ways usage arrives — the
// relay measures it from provider bytes, and a consumer outside the cluster
// reports it from its own accounting — and they must be priced by the same
// rules. Consumers deliberately send QUANTITIES, never money: the moment a
// consumer computes cost, there are two pricing tables to keep in step and a
// rate change means redeploying it.
//
// known=false means no rate exists for the model. Callers must record the row
// anyway: the usage is real, only the price is missing, and booking $0 would
// hide spend that actually happened.
func PriceUsage(prices *pricing.Config, model, modality string, u *extract.Usage) (cost float64, known bool) {
	// Bill in the unit the provider actually reported, not the one its modality
	// suggests: OpenAI's gpt-4o-transcribe is STT billed in tokens, while
	// whisper-1 is STT billed in seconds, and both arrive here as "stt".
	//
	// A price the PROVIDER stated wins outright, and returns before both the
	// rate table and the surcharge below. Two reasons it must return early
	// rather than merely seed `cost`:
	//
	//   - the surcharge multiplier would DOUBLE-COUNT. The stated figure already
	//     contains every surcharge — a 12s clip is 4 credits plain and 5 with a
	//     keyterm — so scaling it again by our own inferred multiplier bills the
	//     entity and speaker-role premiums twice.
	//   - known must be true even for a model no rate covers. That is the point:
	//     scribe_v2_realtime has no pricing.toml entry and would otherwise land
	//     unpriced, when the provider just said what it cost.
	if u.ProviderCostUSD != nil {
		return *u.ProviderCostUSD, true
	}

	switch {
	case modality == extract.ModalityTTS:
		cost, known = prices.CostCharacters(model, u.Characters)
	case modality == extract.ModalitySTT && u.InputTokens == 0 && u.OutputTokens == 0:
		cost, known = prices.CostAudioSeconds(model, u.AudioSeconds)
	default:
		// llm and image both land here: both are token-billed. An image model's
		// output_per_mtok is its image-output rate, so the same math prices it.
		cost, known = prices.Cost(model, u.InputTokens, u.CachedInputTokens, u.OutputTokens)
	}

	// Provider options that bill above the base rate. Applied to cost rather
	// than folded into the quantity, so audio_seconds stays the real measured
	// duration and a row can still be reconciled against an invoice.
	if m := u.SurchargeMultiplier; m > 0 && m != 1 {
		cost *= m
	}
	return cost, known
}

func (p *Provider) record(meta Meta, u *extract.Usage) {
	if u == nil {
		return
	}
	// Fold in what the caller declared before pricing. Hints can only fill gaps
	// the provider left or replace a relay-side inference — never overwrite a
	// measured quantity — so this cannot inflate a bill.
	meta.Hints.Apply(u)

	// Nothing to bill and nobody quoting a price: there is no row here to write.
	//
	// This is not the "unpriced" case below, which is a real quantity missing a
	// rate and must be recorded. It is a call that yielded no measurable
	// quantity at all — a Scribe transcript with no timed words, which is what
	// a VAD segment of noise rather than speech comes back as, since Scribe
	// reports no duration and the relay reconstructs one from those timings.
	// The ledger cannot price it, cannot reconcile it, and cannot show anything
	// about it; a forwarding sidecar could only ship it to a centre that
	// rejects it. Dropping it here keeps that out of both.
	//
	// It has to come AFTER Hints.Apply: a caller that declares the real audio
	// duration turns exactly this row into a billable one, and checking first
	// would discard the call before its quantity arrived.
	//
	// A stated provider cost keeps the row regardless of the counts, INCLUDING
	// a stated zero. "The provider says this cost nothing" is a measurement;
	// "we found nothing to measure" is not, and collapsing the two would hide
	// the difference in a table meant to be reconciled against an invoice.
	//
	// Dropping quietly does cost a signal, and it is worth naming: the centre's
	// rejection was — accidentally — how an extractor that stopped finding
	// quantities would have announced itself. Debug keeps it reachable for
	// whoever goes looking, and a counter is the right home for it once this
	// service has metrics. A Warn is not: it would fire on every noise-only
	// segment and be muted inside a day.
	if u.ProviderCostUSD == nil && u.InputTokens == 0 && u.OutputTokens == 0 &&
		u.CachedInputTokens == 0 && u.Characters == 0 && u.AudioSeconds == 0 {
		p.log.Debug("call carried no billable quantity; not recording",
			"modality", u.Modality, "module", meta.Module, "provider", meta.Provider)
		return
	}

	// The provider's own model id wins; the caller's hint only fills a gap.
	// Whisper's verbose_json and every TTS response name no model at all.
	model := pricing.NormalizeModel(u.Model)
	if model == "" {
		model = pricing.NormalizeModel(meta.ModelHint)
	}

	modality := u.Modality
	if modality == "" {
		modality = extract.ModalityLLM
	}

	cost, known := PriceUsage(p.pricing, model, modality, u)
	if !known {
		// Unpriced is recorded, never dropped and never silently zeroed — the
		// call happened and the usage is real; only the rate is missing.
		p.log.Warn("no price for model; recording usage as unpriced",
			"model", model, "modality", modality, "module", meta.Module)
	}

	ev := Event{
		Meta:                meta,
		Modality:            modality,
		Model:               model,
		InputTokens:         u.InputTokens,
		CachedInputTokens:   u.CachedInputTokens,
		OutputTokens:        u.OutputTokens,
		TotalTokens:         u.TotalTokens,
		AudioSeconds:        u.AudioSeconds,
		Characters:          u.Characters,
		SurchargeMultiplier: u.SurchargeMultiplier,
		ProviderCostUSD:     u.ProviderCostUSD,
		CostUSD:             cost,
		CostKnown:           known,
		OccurredAt:          time.Now(),
	}
	p.sink.Record(context.Background(), ev)
}

// Relay routes /v1/relay/<provider>/… to the matching Provider.
type Relay struct {
	providers map[string]*Provider
	keyring   *Keyring
	// sockets is shared by every provider, so a drain waits on all of them
	// rather than per-provider.
	sockets *activeSockets
}

// Keyring exposes the caller keyring so the usage-ingest endpoint can
// authenticate the same consumers with the same credentials. One key per
// consumer, whether it relays its calls or reports them.
func (r *Relay) Keyring() *Keyring { return r.keyring }

// RouteOpenRouter is the path segment the OpenRouter provider is mounted on:
// /v1/relay/openrouter/... . The Go backend hard-codes the matching path when it
// builds its base URL, so this is a cross-repo contract — renaming one side
// stops metering that provider rather than failing a build.
const RouteOpenRouter = "openrouter"

// openRouterTarget is the bare host. OpenRouter serves under /api/v1/..., and
// the Director forwards whatever follows the route prefix verbatim.
const openRouterTarget = "https://openrouter.ai"

// Keys holds the upstream credentials the relay injects. A struct rather than
// positional arguments because provider count only grows, and swapping two
// same-typed keys at a call site is a mistake the compiler cannot catch.
// An empty field means that provider is unconfigured and will refuse requests.
type Keys struct {
	OpenAI     string
	Google     string
	OpenRouter string
	Deepgram   string
	ElevenLabs string
	Inworld    string
	// AuthKeys is the caller keyring spec, "key:tenant,key:tenant". Empty
	// leaves the relay unauthenticated.
	AuthKeys string
	// AuthMode is AuthEnforce (default) or AuthShadow. See auth.go: shadow
	// serves unauthenticated callers while naming them, so keys can be rolled
	// out to consumers without the relay failing their calls in the meantime.
	AuthMode string
}

// staticExtract adapts a single extractor to the per-path selector, for
// providers where every endpoint bills the same way.
func staticExtract(e extract.Extractor) func(string) extract.Extractor {
	return func(string) extract.Extractor { return e }
}

// openAIExtractorFor picks between OpenAI's billing shapes by path. The
// non-LLM endpoints must be matched FIRST: gpt-4o-transcribe and gpt-image-1
// both return a `usage` object with input_tokens/output_tokens that OpenAIExtract
// would accept and record as an LLM call, hiding STT/image spend inside LLM
// numbers — and image output tokens bill at $40/1M, so mislabelling them is
// expensive as well as wrong.
func openAIExtractorFor(path string) extract.Extractor {
	switch {
	case strings.Contains(path, "/audio/transcriptions") || strings.Contains(path, "/audio/translations"):
		return extract.OpenAITranscriptionExtract
	case strings.Contains(path, "/images/generations") || strings.Contains(path, "/images/edits"):
		return extract.OpenAIImageExtract
	default:
		return extract.OpenAIExtract
	}
}

// elevenLabsExtractorFor returns a response extractor only for Scribe. TTS is
// billed from its request, so returning nil here keeps the relay from buffering
// a synthesised audio body looking for usage that does not exist.
func elevenLabsExtractorFor(path string) extract.Extractor {
	if strings.Contains(path, "/speech-to-text") {
		return extract.ElevenLabsSTTExtract
	}
	return nil
}

// elevenLabsRequestExtract meters TTS from the submitted text, and ONLY on the
// TTS paths. Without the guard a Scribe upload — multipart audio, potentially
// megabytes — would be buffered and JSON-parsed on every request just to fail.
func elevenLabsRequestExtract(path string) extract.RequestExtractor {
	if strings.Contains(path, "/text-to-speech") {
		return extract.ElevenLabsTTSRequest
	}
	return nil
}

// elevenLabsStatedCost returns the header reader only for Scribe, the one
// ElevenLabs path that quotes a price in money.
//
// Text-to-speech returns character-cost but no fiat-cost-before-overages, and a
// credit count alone is not a price: converting it would need this account's
// per-credit rate, which is exactly the kind of hidden second rate table the
// cost engine exists to avoid. So TTS keeps being priced from pricing.toml.
func elevenLabsStatedCost(path string) func(http.Header) (float64, bool) {
	if strings.Contains(path, "/speech-to-text") {
		return extract.ElevenLabsStatedCost
	}
	return nil
}

// deepgramRequestExtract meters Deepgram's HTTP synthesis endpoint from the
// text submitted. Transcription (/v1/listen) reports its own duration and must
// not be routed here — an audio upload would be buffered for nothing.
func deepgramRequestExtract(path string) extract.RequestExtractor {
	if strings.Contains(path, "/speak") {
		return extract.DeepgramSpeakRequest
	}
	return nil
}

// inworldRequestExtract meters Inworld synthesis from the text submitted.
// Inworld answers with a stream of JSON carrying base64 audio and no usage of
// any kind, so the request is the only place the bill is visible.
func inworldRequestExtract(path string) extract.RequestExtractor {
	if strings.Contains(path, "/voice") {
		return extract.InworldTTSRequest
	}
	return nil
}

// New builds a Relay for every configured provider.
func New(prices *pricing.Config, sink Sink, log *slog.Logger, keys Keys) (*Relay, error) {
	openAIKey, googleKey := keys.OpenAI, keys.Google
	deepgramKey, elevenLabsKey := keys.Deepgram, keys.ElevenLabs
	r := &Relay{providers: map[string]*Provider{}}

	// A nil injector marks an unconfigured provider. The relay holds the real
	// credentials — callers' keys are dropped in the Director — so an absent key
	// here means that provider cannot serve anyone.
	var injectOpenAI, injectGemini func(*http.Request)
	if openAIKey != "" {
		injectOpenAI = func(req *http.Request) {
			req.Header.Set("Authorization", "Bearer "+openAIKey)
		}
	}
	if googleKey != "" {
		injectGemini = func(req *http.Request) {
			req.Header.Set("x-goog-api-key", googleKey)
		}
	}
	var injectOpenRouter func(*http.Request)
	if keys.OpenRouter != "" {
		injectOpenRouter = func(req *http.Request) {
			req.Header.Set("Authorization", "Bearer "+keys.OpenRouter)
		}
	}
	var injectDeepgram, injectElevenLabs, injectInworld func(*http.Request)
	if deepgramKey != "" {
		injectDeepgram = func(req *http.Request) {
			req.Header.Set("Authorization", "Token "+deepgramKey)
		}
	}
	if elevenLabsKey != "" {
		injectElevenLabs = func(req *http.Request) {
			req.Header.Set("xi-api-key", elevenLabsKey)
		}
	}
	// Inworld issues a base64 "Basic" key that is used verbatim, so what the
	// deploy supplies is the whole credential rather than a secret to wrap.
	if keys.Inworld != "" {
		injectInworld = func(req *http.Request) {
			req.Header.Set("Authorization", "Basic "+keys.Inworld)
		}
	}

	keyring, err := ParseKeyring(keys.AuthKeys)
	if err != nil {
		return nil, err
	}
	authMode, err := ParseAuthMode(keys.AuthMode)
	if err != nil {
		return nil, err
	}
	r.keyring = keyring
	if keyring.Enabled() {
		log.Info("caller authentication enabled",
			"tenants", keyring.Tenants(), "mode", authMode)
		if authMode == AuthShadow {
			// Worth its own line: this is a temporary state for rolling keys
			// out, and a relay left in it is an open proxy to paid accounts.
			log.Warn("auth mode is SHADOW: unauthenticated callers are SERVED, not rejected")
		}
	} else {
		// The relay holds every provider key. Unauthenticated is only tenable
		// while nothing outside the cluster can reach it.
		log.Warn("caller authentication DISABLED; do not expose this service externally")
	}

	specs := []ProviderConfig{
		{
			Name:       "openai",
			Target:     "https://api.openai.com",
			Extract:    openAIExtractorFor,
			InjectAuth: injectOpenAI,
		},
		{
			Name:       "gemini",
			Target:     "https://generativelanguage.googleapis.com",
			Extract:    staticExtract(extract.GeminiExtract),
			InjectAuth: injectGemini,
		},
		{
			Name:    "deepgram",
			Target:  "https://api.deepgram.com",
			Extract: staticExtract(extract.DeepgramExtract),
			// Synthesis bills the text sent, transcription the audio returned.
			ExtractRequest: deepgramRequestExtract,
			// Live transcription and streaming synthesis are both WebSocket,
			// and live transcription is the highest-volume audio path we have.
			StreamMeter: extract.DeepgramStreamMeterFor,
			InjectAuth:  injectDeepgram,
		},
		{
			Name:   "elevenlabs",
			Target: "https://api.elevenlabs.io",
			// Scribe (STT) answers with JSON; TTS answers with audio and is
			// metered from its request instead.
			Extract:        elevenLabsExtractorFor,
			ExtractRequest: elevenLabsRequestExtract,
			StreamMeter:    extract.ElevenLabsStreamMeterFor,
			// Scribe quotes its own price; take it over our reconstruction.
			StatedCost: elevenLabsStatedCost,
			InjectAuth: injectElevenLabs,
		},
		{
			// The voice agent's primary TTS, and until now entirely unmetered. Inworld
			// streams synthesised audio back as base64 JSON with no usage
			// payload, so the submitted text is the only billable signal.
			Name:    "inworld",
			Target:  "https://api.inworld.ai",
			Extract: func(string) extract.Extractor { return nil },
			// Two paths, and the WebSocket one is the one that matters: the
			// LiveKit agent pipeline uses SynthesizeStream, which speaks
			// /tts/v1/voice:streamBidirectional. Only the one-shot synthesize()
			// call takes the HTTP route. Registering just the request extractor
			// would have metered nothing for the primary voice while relaying
			// its audio perfectly.
			ExtractRequest: inworldRequestExtract,
			StreamMeter:    extract.InworldStreamMeterFor,
			InjectAuth:     injectInworld,
		},
		{
			// A gateway, not a vendor: one credential fronts many models, which
			// is what makes it usable for evaluating candidates without opening
			// an account per vendor.
			//
			// Its API is OpenAI Chat Completions verbatim, so OpenAIExtract
			// reads its usage unchanged. That is the CHAT shape specifically —
			// prompt_tokens/completion_tokens. The Responses API names the same
			// numbers input_tokens/output_tokens, and the wrong reader returns a
			// silent zero rather than an error, billing every call as free.
			//
			// Unlike openAIExtractorFor there is no per-path switch: this route
			// is LLM-only. OpenRouter's audio and image endpoints would need
			// their own extractors before being relayed here, or their spend
			// would be recorded as LLM tokens.
			//
			// Target is the bare host — OpenRouter serves under /api/v1/... where
			// OpenAI serves under /v1/... — so callers address
			// /v1/relay/openrouter/api/v1/chat/completions.
			Name:       RouteOpenRouter,
			Target:     openRouterTarget,
			Extract:    staticExtract(extract.OpenAIExtract),
			InjectAuth: injectOpenRouter,
		},
	}
	r.sockets = &activeSockets{}
	for i := range specs {
		specs[i].Keyring = keyring
		specs[i].AuthMode = authMode
		specs[i].Sockets = r.sockets
	}

	for _, s := range specs {
		p, err := newProvider(s, prices, sink, log)
		if err != nil {
			return nil, err
		}
		// Say so at boot. A missing key is a deploy-time mistake, and the only
		// other signal is a 403 from the upstream at first use.
		if s.InjectAuth == nil {
			log.Warn("provider has no API key; requests to it will be refused",
				"provider", s.Name)
		}
		r.providers[s.Name] = p
	}
	return r, nil
}

// Register mounts every route the relay serves on mux.
//
// It registers itself rather than returning a handler for the caller to mount,
// because the relay does not serve a single prefix: Inworld has to live at the
// ROOT (see below). A caller that mounted a returned handler under
// "/v1/relay/" would leave that route permanently unreachable, and the symptom
// is a 404 only on the one provider — which is exactly what happened before
// this was a Register.
func (r *Relay) Register(mux *http.ServeMux) {
	// Inworld is mounted at the root, which is an exception worth
	// explaining. Its SDK builds request URLs with
	// urljoin(base_url, "/tts/v1/voice:stream") — a LEADING slash, which makes
	// urljoin discard the base URL's path entirely and keep only scheme+host.
	// A "/v1/relay/inworld" prefix therefore cannot survive, however the client
	// is configured.
	//
	// Since the SDK does preserve the host, serving Inworld's own path at our
	// root is the one arrangement that works without patching the plugin.
	// Attribution then has to travel in headers, which Inworld's client does
	// support via an injected http_session. "/tts/" is Inworld's own namespace
	// and collides with nothing the relay serves.
	if p, ok := r.providers["inworld"]; ok {
		mux.Handle("/tts/", p)
	}

	mux.HandleFunc("/v1/relay/", func(w http.ResponseWriter, req *http.Request) {
		// /v1/relay/<provider>/...
		rest := strings.TrimPrefix(req.URL.Path, "/v1/relay/")
		name, _, _ := strings.Cut(rest, "/")
		p, ok := r.providers[name]
		if !ok {
			http.Error(w, "unknown provider: "+name, http.StatusNotFound)
			return
		}
		p.ServeHTTP(w, req)
	})
}
