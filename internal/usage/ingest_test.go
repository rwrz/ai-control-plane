package usage

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/rwrz/ai-control-plane/internal/pricing"
	"github.com/rwrz/ai-control-plane/internal/relay"
)

type capturingSink struct {
	mu     sync.Mutex
	events []relay.Event
}

func (s *capturingSink) Record(_ context.Context, ev relay.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, ev)
}

func (s *capturingSink) all() []relay.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]relay.Event(nil), s.events...)
}

func newTestIngest(t *testing.T, keys string) (*Ingest, *capturingSink) {
	t.Helper()

	keyring, err := relay.ParseKeyring(keys)
	if err != nil {
		t.Fatalf("parse keyring: %v", err)
	}
	sink := &capturingSink{}
	prices := &pricing.Config{Models: map[string]pricing.ModelPrice{
		"gemini-2.5-flash":  {InputPerMtok: 0.30, CachedInputPerMtok: 0.075, OutputPerMtok: 2.50},
		"nova-3":            {PerAudioHour: 0.29},
		"eleven_turbo_v2_5": {Per1kCharacters: 0.09},
	}}
	return NewIngest(sink, prices, keyring, slog.New(slog.NewTextHandler(io.Discard, nil))), sink
}

// post sends a batch and returns the response.
func post(t *testing.T, in *Ingest, key, body string) *http.Response {
	t.Helper()

	mux := http.NewServeMux()
	in.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/v1/usage/events", strings.NewReader(body))
	if key != "" {
		req.Header.Set("X-Relay-Key", key)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec.Result()
}

// Consumers report quantities; the control plane prices them. This is the whole
// point of the split — a consumer that computed cost would be a second pricing
// table to keep in step.
func TestIngestPricesReportedQuantities(t *testing.T) {
	in, sink := newTestIngest(t, "coach-key:voice_agent")

	resp := post(t, in, "coach-key", `{"events":[
		{"client_event_id":"sess-7:llm","user":"user-42","entity_type":"session","entity_id":"sess-7",
		 "feature":"coaching_llm","module":"coach","provider":"google",
		 "model":"gemini-2.5-flash","modality":"llm",
		 "input_tokens":1000000,"output_tokens":1000000},
		{"client_event_id":"sess-7:stt","user":"user-42","entity_type":"session","entity_id":"sess-7",
		 "provider":"deepgram","model":"nova-3","modality":"stt","audio_seconds":3600}
	]}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	events := sink.all()
	if len(events) != 2 {
		t.Fatalf("recorded %d events, want 2", len(events))
	}

	llm := events[0]
	// 1M input at $0.30 + 1M output at $2.50.
	if llm.CostUSD != 2.80 || !llm.CostKnown {
		t.Errorf("llm cost = %v (known=%v), want 2.80", llm.CostUSD, llm.CostKnown)
	}
	if llm.User != "user-42" || llm.EntityType != "session" || llm.EntityID != "sess-7" || llm.Feature != "coaching_llm" {
		t.Errorf("attribution lost: %+v", llm.Meta)
	}

	// One hour of audio at $0.29/hour.
	if stt := events[1]; stt.CostUSD != 0.29 || !stt.CostKnown {
		t.Errorf("stt cost = %v (known=%v), want 0.29", stt.CostUSD, stt.CostKnown)
	}
}

// A sidecar built before the entity pair still sends "session". Until every one
// is upgraded, its rows must keep grouping — as the entity that replaced it.
func TestIngestMapsLegacySessionToEntity(t *testing.T) {
	in, sink := newTestIngest(t, "coach-key:voice_agent")

	post(t, in, "coach-key", `{"events":[
		{"client_event_id":"sess-7:llm","user":"user-42","session":"sess-7",
		 "provider":"google","model":"gemini-2.5-flash","modality":"llm","input_tokens":10}
	]}`)

	events := sink.all()
	if len(events) != 1 {
		t.Fatalf("recorded %d events, want 1", len(events))
	}
	if got := events[0]; got.EntityType != "session" || got.EntityID != "sess-7" {
		t.Errorf("entity = (%q, %q), want (session, sess-7)", got.EntityType, got.EntityID)
	}
}

// A reported row must never be mistaken for a measured one. Only measured rows
// can be reconciled against a provider invoice, so the distinction has to
// survive into the ledger.
func TestIngestLabelsRowsAsReported(t *testing.T) {
	in, sink := newTestIngest(t, "coach-key:voice_agent")

	post(t, in, "coach-key", `{"events":[{"provider":"deepgram","model":"nova-3",
		"modality":"stt","audio_seconds":10}]}`)

	events := sink.all()
	if len(events) != 1 {
		t.Fatalf("recorded %d events, want 1", len(events))
	}
	if events[0].Source != relay.SourceReported {
		t.Errorf("source = %q, want %q", events[0].Source, relay.SourceReported)
	}
}

// The tenant comes from the authenticated key, never the body — the same rule
// the relay applies. Otherwise a leaked key could book spend against another
// consumer, and per-consumer limits would mean nothing.
func TestIngestTenantComesFromKeyNotBody(t *testing.T) {
	in, sink := newTestIngest(t, "coach-key:voice_agent")

	// "tenant" is not even a field; asserting one has to be impossible, not
	// merely overridden.
	post(t, in, "coach-key", `{"events":[{"tenant":"backend","provider":"deepgram",
		"model":"nova-3","modality":"stt","audio_seconds":10}]}`)

	if got := sink.all()[0].Tenant; got != "voice_agent" {
		t.Errorf("tenant = %q, want voice_agent from the key", got)
	}
}

// The relay may run unauthenticated while it is only reachable in-cluster: the
// worst case there is unauthorised spend on our own providers. An open INGEST
// endpoint is different in kind — anyone could write arbitrary rows into the
// billing ledger — so it refuses to serve at all without a keyring.
func TestIngestRefusesWhenNoKeysConfigured(t *testing.T) {
	in, sink := newTestIngest(t, "")

	resp := post(t, in, "", `{"events":[{"provider":"deepgram","model":"nova-3",
		"modality":"stt","audio_seconds":10}]}`)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 when no keys are configured", resp.StatusCode)
	}
	if len(sink.all()) != 0 {
		t.Error("wrote to the ledger with no authentication configured")
	}
}

func TestIngestRejectsBadCredential(t *testing.T) {
	in, sink := newTestIngest(t, "coach-key:voice_agent")

	resp := post(t, in, "wrong-key", `{"events":[{"provider":"deepgram","model":"nova-3",
		"modality":"stt","audio_seconds":10}]}`)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
	if len(sink.all()) != 0 {
		t.Error("wrote to the ledger for an unauthenticated caller")
	}
}

// One malformed row must not discard the rest of the batch: the other providers
// in that session really ran and really cost money.
func TestIngestAcceptsGoodRowsAlongsideBad(t *testing.T) {
	in, sink := newTestIngest(t, "coach-key:voice_agent")

	resp := post(t, in, "coach-key", `{"events":[
		{"provider":"deepgram","model":"nova-3","modality":"stt","audio_seconds":10},
		{"provider":"deepgram","model":"nova-3","modality":"telepathy","audio_seconds":10},
		{"provider":"deepgram","model":"nova-3","modality":"stt"},
		{"provider":"elevenlabs","model":"eleven_turbo_v2_5","modality":"tts","characters":1000}
	]}`)

	var body IngestResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Accepted != 2 || body.Rejected != 2 {
		t.Errorf("accepted=%d rejected=%d, want 2 and 2", body.Accepted, body.Rejected)
	}
	if len(body.Errors) != 2 {
		t.Fatalf("errors = %v, want one per rejected row", body.Errors)
	}
	// The caller has to be able to tell WHICH rows failed.
	if !strings.Contains(body.Errors[0], "events[1]") || !strings.Contains(body.Errors[1], "events[2]") {
		t.Errorf("errors do not identify the rows: %v", body.Errors)
	}
	if len(sink.all()) != 2 {
		t.Errorf("recorded %d events, want the 2 valid ones", len(sink.all()))
	}
}

// An unknown model is recorded unpriced, never dropped and never billed as
// zero: the usage really happened, only the rate is missing, and a dropped row
// hides spend while an unpriced one is visible.
func TestIngestRecordsUnknownModelUnpriced(t *testing.T) {
	in, sink := newTestIngest(t, "coach-key:voice_agent")

	post(t, in, "coach-key", `{"events":[{"provider":"inworld",
		"model":"inworld-tts-9.9-imaginary","modality":"tts","characters":500}]}`)

	events := sink.all()
	if len(events) != 1 {
		t.Fatalf("recorded %d events, want the usage kept", len(events))
	}
	if events[0].CostKnown {
		t.Error("cost_known should be false for a model with no rate")
	}
	if events[0].Characters != 500 {
		t.Errorf("characters = %d, want the quantity preserved", events[0].Characters)
	}
}

// occurred_at is when the usage happened, not when it was reported. A session
// reporting at its end must not pile an hour of spend onto the closing minute.
func TestIngestPreservesOccurredAt(t *testing.T) {
	in, sink := newTestIngest(t, "coach-key:voice_agent")

	post(t, in, "coach-key", `{"events":[{"provider":"deepgram","model":"nova-3",
		"modality":"stt","audio_seconds":10,"occurred_at":"2026-07-22T08:15:00Z"}]}`)

	if got := sink.all()[0].OccurredAt.UTC().Format("2006-01-02T15:04:05Z"); got != "2026-07-22T08:15:00Z" {
		t.Errorf("occurred_at = %q, want the reported time", got)
	}
}

// A sidecar's quoted price must survive the hop to the centre.
//
// The surrounding rule is that senders ship quantities and the centre prices
// them, which is exactly what makes this easy to break: dropping the field
// looks like enforcing the rule, and the failure is invisible — the row still
// lands, just re-derived from a rate table that cannot express what ElevenLabs
// actually charged.
func TestIngestHonoursAProviderQuotedPrice(t *testing.T) {
	in, sink := newTestIngest(t, "sidecar-key:voice_agent")

	// scribe_v2 is deliberately absent from the test rate table: a quoted price
	// must price a row the centre could not have priced at all.
	resp := post(t, in, "sidecar-key", `{"events":[
		{"client_event_id":"sess-9:stt","user":"user-42","entity_type":"session","entity_id":"sess-9",
		 "feature":"live_transcription","module":"coach","provider":"elevenlabs",
		 "model":"scribe_v2","modality":"stt","audio_seconds":1.799,
		 "surcharge_multiplier":1.2,"provider_cost_usd":0.00018182,"source":"relayed"}
	]}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	events := sink.all()
	if len(events) != 1 {
		t.Fatalf("recorded %d events, want 1", len(events))
	}
	ev := events[0]

	// The quoted figure, not a recomputation — and not scaled again by the
	// surcharge the provider had already included.
	if ev.CostUSD != 0.00018182 {
		t.Errorf("cost = %v, want 0.00018182 (the price ElevenLabs quoted)", ev.CostUSD)
	}
	if !ev.CostKnown {
		t.Error("CostKnown = false for a model with no rate but a quoted price")
	}
	// The quantity still lands: it is what reconciles against an invoice line.
	if ev.AudioSeconds != 1.799 {
		t.Errorf("audio seconds = %v, want 1.799", ev.AudioSeconds)
	}
	if ev.Source != relay.SourceRelayed {
		t.Errorf("source = %q, want %q", ev.Source, relay.SourceRelayed)
	}
}

// Without a quote nothing changes: the centre prices from its own table, which
// stays the rule for every provider that does not state a price.
func TestIngestStillPricesWhenNoQuoteIsSent(t *testing.T) {
	in, sink := newTestIngest(t, "sidecar-key:voice_agent")

	resp := post(t, in, "sidecar-key", `{"events":[
		{"client_event_id":"sess-9:dg","provider":"deepgram","model":"nova-3",
		 "modality":"stt","audio_seconds":3600,"source":"relayed"}
	]}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ev := sink.all()[0]; ev.CostUSD != 0.29 || !ev.CostKnown {
		t.Errorf("cost = %v (known=%v), want 0.29 from the rate table", ev.CostUSD, ev.CostKnown)
	}
}

// ReportedEventFrom is the sidecar's encode step, and it had no test at all.
//
// It is the one place where following the "senders ship quantities, never money"
// rule too literally silently breaks billing: dropping ProviderCostUSD here
// looks like enforcing the rule, and the row still arrives — priced from a rate
// table that cannot reproduce what ElevenLabs charged. The centre-side test
// above cannot catch it, because the field never reaches the centre to be
// honoured.
func TestReportedEventFromCarriesTheQuoteButNotTheComputedCost(t *testing.T) {
	quoted := 0.00018182
	ev := relay.Event{
		Meta: relay.Meta{
			Tenant: "voice_agent", User: "user-42", EntityType: "session", EntityID: "sess-9",
			Feature: "live_transcription", Module: "coach", Provider: "elevenlabs",
		},
		Source: relay.SourceRelayed, ClientEventID: "sess-9:stt",
		Model: "scribe_v2", Modality: "stt", AudioSeconds: 1.799,
		SurchargeMultiplier: 1.2,
		ProviderCostUSD:     &quoted,
		CostUSD:             quoted,
		CostKnown:           true,
	}

	got := ReportedEventFrom(ev)

	// The quote travels: the centre cannot re-derive it.
	if got.ProviderCostUSD == nil {
		t.Fatal("ProviderCostUSD was dropped; the centre will re-price from its rate table")
	}
	if *got.ProviderCostUSD != quoted {
		t.Errorf("quote = %v, want %v", *got.ProviderCostUSD, quoted)
	}
	// The quantities travel.
	if got.AudioSeconds != 1.799 || got.SurchargeMultiplier != 1.2 {
		t.Errorf("quantities lost: seconds=%v surcharge=%v", got.AudioSeconds, got.SurchargeMultiplier)
	}
	if got.Source != relay.SourceRelayed || got.ClientEventID != "sess-9:stt" {
		t.Errorf("provenance lost: source=%q id=%q", got.Source, got.ClientEventID)
	}

	// And the wire form still carries no field for a sender-computed cost or a
	// sender-claimed tenant — the rule this is an exception to, not a repeal of.
	blob, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got.EntityType != "session" || got.EntityID != "sess-9" {
		t.Errorf("entity lost: (%q, %q)", got.EntityType, got.EntityID)
	}
	// "session": is the legacy key the centre still reads; a sender writing it
	// would keep the alias alive forever.
	for _, banned := range []string{`"cost_usd"`, `"cost_known"`, `"tenant"`, `"session":`} {
		if strings.Contains(string(blob), banned) {
			t.Errorf("wire format leaked %s: %s", banned, blob)
		}
	}
	// A provider that quotes nothing puts nothing on the wire.
	ev.ProviderCostUSD = nil
	if blob, _ := json.Marshal(ReportedEventFrom(ev)); strings.Contains(string(blob), "provider_cost_usd") {
		t.Errorf("unquoted row still carried provider_cost_usd: %s", blob)
	}
}

// The row that motivated the quantity rule's exception, end to end.
//
// A Scribe call on a VAD segment that caught noise rather than speech comes
// back with an empty transcript: no timed words, and Scribe reports no duration
// of its own, so the relay has nothing to reconstruct one from. The row arrives
// with every quantity at zero and the price ElevenLabs itself quoted attached.
//
// Rejecting it on the counts — which is what shipped, and what was throwing
// away roughly one the voice agent row in six — discards real spend at the door, in
// the one case where the sender is carrying the most accurate number in the
// exchange.
func TestIngestKeepsAQuotedPriceWithNoQuantity(t *testing.T) {
	in, sink := newTestIngest(t, "sidecar-key:voice_agent")

	resp := post(t, in, "sidecar-key", `{"events":[
		{"client_event_id":"sess-9:stt-noise","user":"user-42","entity_type":"session","entity_id":"sess-9",
		 "feature":"live_transcription","module":"coach","provider":"elevenlabs",
		 "model":"scribe_v2","modality":"stt","audio_seconds":0,
		 "provider_cost_usd":0.00018182,"source":"relayed"}
	]}`)

	var body IngestResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Accepted != 1 || body.Rejected != 0 {
		t.Fatalf("accepted=%d rejected=%d (%v), want the quoted row kept",
			body.Accepted, body.Rejected, body.Errors)
	}

	events := sink.all()
	if len(events) != 1 {
		t.Fatalf("recorded %d events, want 1", len(events))
	}
	if events[0].CostUSD != 0.00018182 || !events[0].CostKnown {
		t.Errorf("cost = %v (known=%v), want the quoted 0.00018182",
			events[0].CostUSD, events[0].CostKnown)
	}
}

// A stated ZERO is still a measurement — the provider said this call cost
// nothing — and must not be confused with a row that quoted no price at all.
// omitempty is why this is worth pinning: it keeps a zero quote off the wire in
// the OTHER direction, so only the pointer distinguishes the two here.
func TestIngestKeepsAQuotedZero(t *testing.T) {
	in, sink := newTestIngest(t, "sidecar-key:voice_agent")

	post(t, in, "sidecar-key", `{"events":[{"provider":"elevenlabs","model":"scribe_v2",
		"modality":"stt","provider_cost_usd":0,"source":"relayed"}]}`)

	events := sink.all()
	if len(events) != 1 {
		t.Fatalf("recorded %d events, want the quoted-zero row kept", len(events))
	}
	if !events[0].CostKnown {
		t.Error("CostKnown = false: a quoted zero is a known price, not a missing one")
	}
}

// The original rule stands for everyone else: no quantity and nobody quoting a
// price is a broken caller, and accepting it would pad the ledger with rows that
// say nothing.
func TestIngestStillRejectsAnEmptyUnquotedRow(t *testing.T) {
	in, sink := newTestIngest(t, "coach-key:voice_agent")

	resp := post(t, in, "coach-key", `{"events":[
		{"provider":"deepgram","model":"nova-3","modality":"stt"}]}`)

	var body IngestResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Rejected != 1 || len(sink.all()) != 0 {
		t.Errorf("rejected=%d recorded=%d, want the empty row refused",
			body.Rejected, len(sink.all()))
	}
}

// A negative quote would turn spend into a credit against a column that is
// summed. The relay screens its own header read, but a reported row arrives
// from outside and is checked here on its own.
func TestIngestRejectsANegativeQuote(t *testing.T) {
	in, sink := newTestIngest(t, "sidecar-key:voice_agent")

	resp := post(t, in, "sidecar-key", `{"events":[{"provider":"elevenlabs",
		"model":"scribe_v2","modality":"stt","audio_seconds":2,"provider_cost_usd":-1}]}`)

	var body IngestResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Rejected != 1 || len(sink.all()) != 0 {
		t.Errorf("rejected=%d recorded=%d, want the negative quote refused",
			body.Rejected, len(sink.all()))
	}
}

// A rejection has to say WHICH call it threw away. An index alone addresses
// nothing a reader can look up: a batch mixes every provider a session used, and
// diagnosing this line in production meant eliminating extractors by hand to
// work out which call "events[9]" had been.
func TestIngestRejectionNamesTheRow(t *testing.T) {
	in, _ := newTestIngest(t, "coach-key:voice_agent")

	resp := post(t, in, "coach-key", `{"events":[
		{"provider":"elevenlabs","model":"scribe_v2","modality":"stt"}]}`)

	var body IngestResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Errors) != 1 {
		t.Fatalf("errors = %v, want one", body.Errors)
	}
	for _, want := range []string{"events[0]", "elevenlabs", "scribe_v2", "stt"} {
		if !strings.Contains(body.Errors[0], want) {
			t.Errorf("error %q does not name %q", body.Errors[0], want)
		}
	}
}

// The fields naming the row are caller-supplied text that lands in a log line
// and in the response body, so they are bounded — and cut by runes, since a
// half-written character is worse than a truncated one. An absent field prints
// as "?" rather than as a gap, because a rejection is often ABOUT the field that
// is missing.
func TestDescribeBoundsCallerSuppliedFields(t *testing.T) {
	long := strings.Repeat("é", 200)
	got := ReportedEvent{Provider: "elevenlabs", Model: long}.describe()

	if strings.Contains(got, strings.Repeat("é", 100)) {
		t.Errorf("model was not bounded: %q", got)
	}
	if !strings.HasSuffix(got, " ?") {
		t.Errorf("describe() = %q, want the empty modality shown as ?", got)
	}
	if !utf8.ValidString(got) {
		t.Errorf("describe() split a multi-byte character: %q", got)
	}
}
