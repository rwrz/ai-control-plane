package usage

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/rwrz/ai-control-plane/internal/pricing"
	"github.com/rwrz/ai-control-plane/internal/relay"
	"github.com/rwrz/ai-control-plane/internal/relay/extract"
)

// Reported usage ingest.
//
// Most spend is metered by relaying the call and reading the provider's own
// bytes, which is the only way to know rather than be told. But relaying is not
// always payable: putting the control plane in the path of a real-time voice
// session adds a network hop to every audio frame, and latency on a
// conversation is the product. Where that is the case a consumer meters itself
// and reports here instead.
//
// Two rules keep the compromise honest:
//
//  1. Consumers report QUANTITIES, never money. Pricing stays here, against one
//     pricing.toml. If a consumer computed cost there would be two rate tables
//     to keep in step and a price change would mean redeploying a voice agent.
//  2. Reported rows are labelled as such (relay.SourceReported) and never
//     silently mixed with measured ones. "What we measured" and "what we were
//     told" answer different questions, and only the first can be reconciled
//     against a provider invoice.
//
// The tenant comes from the authenticated key, never the body — the same rule
// the relay applies, so a leaked key cannot book spend against another
// consumer.
//
// The same endpoint receives a SIDECAR relay's output (see internal/forward).
// A sidecar did read the provider's bytes, so it reports source="relayed" and
// its rows stay labelled as measured. That is why Source is a wire field rather
// than a constant: the honesty of the column comes from the row's origin, not
// from which door it came through.

// IngestPath is the route this handler serves. Exported so a forwarding sidecar
// composes the same URL it will be received at, rather than repeating a literal.
const IngestPath = "/v1/usage/events"

// maxIngestBatchBytes caps a request body. A voice session reports a handful of
// rows (one per provider+model it used), so this is generous by a wide margin
// and exists only to bound what one caller can make the server allocate.
const maxIngestBatchBytes = 1 << 20 // 1 MiB

// MaxIngestEvents caps rows per request, for the same reason. Exported so a
// forwarding sidecar can size its batches to fit rather than discovering the
// limit as a 413.
const MaxIngestEvents = 500

// ReportedEvent is one usage row asserted by a consumer.
//
// It is exported, and is the ONLY definition of this wire format, because both
// halves of the conversation live in this repository: a sidecar relay encodes it
// (internal/forward) and the central control plane decodes it here. A private
// copy on either side would be free to drift, and a drifted field does not fail
// — it silently arrives as a zero value and under-bills.
type ReportedEvent struct {
	// ClientEventID makes a retry safe. A consumer that times out learns
	// nothing about whether the write landed, and a voice agent retrying at
	// session end would otherwise double-bill the session. Strongly
	// recommended; without one, retries duplicate.
	ClientEventID string `json:"client_event_id"`

	User       string `json:"user"`
	EntityType string `json:"entity_type,omitempty"`
	EntityID   string `json:"entity_id,omitempty"`
	// Session is read, never written: sidecars built before the entity pair
	// still send it. See relay.ResolveEntity.
	Session string `json:"session,omitempty"`
	Feature string `json:"feature"`
	Module  string `json:"module"`

	Provider string `json:"provider"`
	Model    string `json:"model"`
	Modality string `json:"modality"`

	InputTokens       int64   `json:"input_tokens"`
	CachedInputTokens int64   `json:"cached_input_tokens"`
	OutputTokens      int64   `json:"output_tokens"`
	AudioSeconds      float64 `json:"audio_seconds"`
	Characters        int64   `json:"characters"`

	// SurchargeMultiplier scales the computed cost for provider options that
	// bill above the base rate (ElevenLabs' keyterms). It is sent rather than
	// folded into a quantity so audio_seconds stays the real measured duration.
	// Zero means none.
	SurchargeMultiplier float64 `json:"surcharge_multiplier,omitempty"`

	// ProviderCostUSD is a price the PROVIDER stated in its own response, and is
	// the one exception to "senders ship quantities, never money" below.
	//
	// The rule exists so a rate change is a redeploy of the centre rather than
	// of every consumer. This does not break it: it is not the sender's opinion
	// of a rate, it is the provider's own figure for that one call, which no
	// rate table here can reproduce — ElevenLabs quantises Scribe into whole
	// ~2.9752s credits and adds an invisible 20% for keyterms. Recomputing it
	// centrally would discard the most accurate number in the exchange.
	//
	// A POINTER so "cost nothing" and "quoted nothing" stay distinguishable;
	// omitempty keeps it off the wire entirely for the providers that are
	// silent, which is most of them.
	ProviderCostUSD *float64 `json:"provider_cost_usd,omitempty"`

	// Source is how the row was obtained: "relayed" (the sender proxied the
	// call and read the provider's own bytes) or "reported" (the sender
	// asserted it from its own accounting). Empty means reported, which is the
	// safe default — a consumer that says nothing has not measured anything.
	//
	// A caller claiming "relayed" is claiming to have read the provider's
	// bytes, which only a sidecar relay does. The claim is unverifiable, but it
	// is also self-scoped: a key can only ever mislabel its own tenant's rows.
	Source string `json:"source,omitempty"`

	// Metadata is what the caller declared about the request, stored verbatim.
	// For a forwarded row it carries the X-Usage-Request hints the sidecar
	// received, so an audit at the centre sees the same claim the sidecar
	// priced against.
	Metadata json.RawMessage `json:"metadata,omitempty"`

	// OccurredAt is when the usage happened, not when it was reported. A
	// session reporting at its end must not pile an hour of spend onto the
	// minute it finished.
	OccurredAt time.Time `json:"occurred_at"`
}

// IngestRequest is one batch of reported usage.
type IngestRequest struct {
	Events []ReportedEvent `json:"events"`
}

// IngestResponse reports per-row outcomes. A batch is never all-or-nothing:
// one malformed row must not discard spend that really happened.
type IngestResponse struct {
	Accepted int      `json:"accepted"`
	Rejected int      `json:"rejected"`
	Errors   []string `json:"errors,omitempty"`
}

// ReportedEventFrom flattens a priced event into the wire format.
//
// The COMPUTED cost is deliberately dropped. A sender ships QUANTITIES and the
// receiving control plane prices them against its own pricing.toml, so there is
// exactly one rate table: a price correction is a redeploy of the centre, never
// of a voice box someone has to reach by hand. Tenant is dropped for a different
// reason — the receiver takes it from the authenticated key and would ignore
// anything sent here.
//
// ProviderCostUSD is the deliberate exception and does travel: it is not a rate
// the sender applied, it is the price the PROVIDER quoted for that call, and
// dropping it would force the centre to recompute a figure it cannot derive
// (see ReportedEvent.ProviderCostUSD).
func ReportedEventFrom(ev relay.Event) ReportedEvent {
	e := ReportedEvent{
		ClientEventID:       ev.ClientEventID,
		User:                ev.User,
		EntityType:          ev.EntityType,
		EntityID:            ev.EntityID,
		Feature:             ev.Feature,
		Module:              ev.Module,
		Provider:            ev.Provider,
		Model:               ev.Model,
		Modality:            ev.Modality,
		InputTokens:         ev.InputTokens,
		CachedInputTokens:   ev.CachedInputTokens,
		OutputTokens:        ev.OutputTokens,
		AudioSeconds:        ev.AudioSeconds,
		Characters:          ev.Characters,
		SurchargeMultiplier: ev.SurchargeMultiplier,
		ProviderCostUSD:     ev.ProviderCostUSD,
		// Resolved, never passed through. The two ends default an empty source
		// in OPPOSITE directions — a producer's empty means relayed (the relay's
		// own call sites predate the column), a wire empty means reported (a
		// consumer that says nothing has measured nothing). Sending "" would
		// cross that boundary and relabel every measured row as asserted.
		Source:     relay.SourceOf(ev),
		OccurredAt: ev.OccurredAt,
	}
	if ev.Hints != nil && len(ev.Hints.Raw) > 0 {
		e.Metadata = ev.Hints.Raw
	}
	return e
}

// validate rejects a row that cannot be priced or attributed. It deliberately
// does NOT reject a model with no known rate: that is recorded unpriced, which
// is visible, rather than dropped, which is not.
func (e ReportedEvent) validate() error {
	if e.Provider == "" {
		return errors.New("provider is required")
	}
	if e.Model == "" {
		return errors.New("model is required")
	}
	switch e.Modality {
	case extract.ModalityLLM, extract.ModalitySTT, extract.ModalityTTS, extract.ModalityImage:
	default:
		return fmt.Errorf("modality must be one of llm/stt/tts/image, got %q", e.Modality)
	}
	switch e.Source {
	case "", relay.SourceRelayed, relay.SourceReported:
	default:
		return fmt.Errorf("source must be one of relayed/reported, got %q", e.Source)
	}
	// A price the PROVIDER stated is a billable quantity in its own right, and
	// outranks the count-based check below — PriceUsage prefers it over the rate
	// table anyway, so a row carrying one is never unpriceable.
	//
	// It has to be checked FIRST because the case that needs it has no counts at
	// all. ElevenLabs quotes a fiat cost on every Scribe call, including one
	// whose transcript has no timed words — a VAD segment that caught noise
	// rather than speech. Scribe's duration is reconstructed from those timings,
	// so the row arrives as all-zero quantities with real money attached, and
	// rejecting it on the counts drops the spend before pricing ever sees the
	// figure. Measured against the voice agent in production: roughly one such row per
	// six accepted, every one of them silently discarded.
	//
	// Validity is re-checked here rather than trusted from the sender: the relay
	// screens its own header read, but ingest is a separate trust boundary and
	// this figure is written straight into a column that gets summed. A negative
	// quote is the reachable case — it would turn spend into a credit. NaN and
	// ±Inf cannot survive JSON decoding into a float64 at all, so they are shut
	// out for the day this struct is validated from something other than a
	// decoded request body.
	if e.ProviderCostUSD != nil {
		if c := *e.ProviderCostUSD; c < 0 || math.IsNaN(c) || math.IsInf(c, 0) {
			return errors.New("provider_cost_usd must be a non-negative finite number")
		}
	} else if e.InputTokens == 0 && e.OutputTokens == 0 &&
		e.CachedInputTokens == 0 && e.Characters == 0 && e.AudioSeconds == 0 {
		// A row with nothing billable in it and no stated price is a bug at the
		// caller, not a free call. Accepting it would quietly inflate the row
		// count while adding nothing, and hide the fact that the caller's
		// accounting is broken.
		return errors.New("event carries no billable quantity")
	}
	if e.InputTokens < 0 || e.OutputTokens < 0 || e.CachedInputTokens < 0 ||
		e.Characters < 0 || e.AudioSeconds < 0 {
		return errors.New("quantities must not be negative")
	}
	// A negative multiplier would turn spend into a credit. Anything positive is
	// allowed: a surcharge is the provider's rule, and hard-coding a ceiling here
	// would reject a real one we have not met yet.
	if e.SurchargeMultiplier < 0 {
		return errors.New("surcharge_multiplier must not be negative")
	}
	if len(e.Metadata) > 0 && !json.Valid(e.Metadata) {
		return errors.New("metadata must be valid JSON")
	}
	return nil
}

// describe names one row for a rejection message: provider, model and
// modality, which together say WHICH call was thrown away.
//
// Every field is caller-supplied text and the result reaches both a log line
// and the response body, so each is bounded. An absent field prints as "?" —
// a rejection is frequently ABOUT a missing field, and an empty gap in the
// message would hide the very thing being reported.
func (e ReportedEvent) describe() string {
	return fmt.Sprintf("%s/%s %s",
		clipField(e.Provider), clipField(e.Model), clipField(e.Modality))
}

// clipField bounds one caller-supplied field to a length worth logging. It cuts
// by RUNES rather than bytes so a multi-byte character is never split into
// mojibake in the middle of a log line.
func clipField(s string) string {
	const max = 48
	if s == "" {
		return "?"
	}
	if r := []rune(s); len(r) > max {
		return string(r[:max]) + "…"
	}
	return s
}

// Ingest accepts reported usage, prices it, and hands it to the sink.
type Ingest struct {
	sink    relay.Sink
	prices  *pricing.Config
	keyring *relay.Keyring
	log     *slog.Logger
	now     func() time.Time
}

// NewIngest builds the ingest handler. The keyring is the relay's, so a
// consumer presents the same credential whether it is relaying or reporting.
func NewIngest(sink relay.Sink, prices *pricing.Config, keyring *relay.Keyring, log *slog.Logger) *Ingest {
	return &Ingest{
		sink:    sink,
		prices:  prices,
		keyring: keyring,
		log:     log.With("component", "usage-ingest"),
		now:     time.Now,
	}
}

// Register mounts the ingest route.
func (i *Ingest) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST "+IngestPath, i.handle)
}

func (i *Ingest) handle(w http.ResponseWriter, r *http.Request) {
	tenant, ok := i.authenticate(w, r)
	if !ok {
		return
	}

	var req IngestRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxIngestBatchBytes))
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if len(req.Events) == 0 {
		writeError(w, http.StatusBadRequest, "events is required and must not be empty")
		return
	}
	if len(req.Events) > MaxIngestEvents {
		writeError(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("at most %d events per request", MaxIngestEvents))
		return
	}

	resp := IngestResponse{}
	for n, e := range req.Events {
		if err := e.validate(); err != nil {
			// One bad row does not reject the batch: a voice session's other
			// providers still happened and still cost money. Report which
			// failed so the caller can fix it.
			//
			// Name the row, not only its index. A batch mixes every provider a
			// session used, and an index addresses nothing a reader can look up:
			// diagnosing this line for the voice agent meant eliminating extractors by
			// hand to work out which call it had been. Provider, model and
			// modality are what turn "events[9]" into a place to look.
			resp.Rejected++
			resp.Errors = append(resp.Errors,
				fmt.Sprintf("events[%d] %s: %v", n, e.describe(), err))
			continue
		}
		i.sink.Record(r.Context(), i.toEvent(tenant, e))
		resp.Accepted++
	}

	if resp.Rejected > 0 {
		i.log.Warn("rejected reported usage rows",
			"tenant", tenant, "accepted", resp.Accepted, "rejected", resp.Rejected,
			"errors", resp.Errors)
	}
	writeJSON(w, http.StatusOK, resp)
}

// authenticate resolves the caller's tenant from its credential.
//
// Unlike the relay, this refuses to serve an unauthenticated caller even when
// no keyring is configured. The relay can run open while it is unreachable from
// outside the cluster, because the worst case is unauthorised spend on our own
// providers. An open ingest endpoint is different in kind: anyone could write
// arbitrary rows into the billing ledger, and nothing downstream would know.
func (i *Ingest) authenticate(w http.ResponseWriter, r *http.Request) (string, bool) {
	if !i.keyring.Enabled() {
		i.log.Error("usage ingest requires RELAY_AUTH_KEYS; refusing request")
		writeError(w, http.StatusServiceUnavailable,
			"usage ingest is not configured; no caller keys are set")
		return "", false
	}
	tenant, ok := i.keyring.Lookup(relay.CredentialFrom(r))
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return "", false
	}
	return tenant, true
}

// toEvent prices a reported row and turns it into a ledger event.
func (i *Ingest) toEvent(tenant string, e ReportedEvent) relay.Event {
	model := pricing.NormalizeModel(e.Model)
	u := &extract.Usage{
		Model:               model,
		Modality:            e.Modality,
		InputTokens:         e.InputTokens,
		CachedInputTokens:   e.CachedInputTokens,
		OutputTokens:        e.OutputTokens,
		AudioSeconds:        e.AudioSeconds,
		Characters:          e.Characters,
		SurchargeMultiplier: e.SurchargeMultiplier,
		ProviderCostUSD:     e.ProviderCostUSD,
	}
	cost, known := relay.PriceUsage(i.prices, model, e.Modality, u)
	if !known {
		i.log.Warn("no price for reported model; recording usage as unpriced",
			"model", model, "modality", e.Modality, "provider", e.Provider, "tenant", tenant)
	}

	occurredAt := e.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = i.now()
	}

	// An unset source means the sender never claimed to have measured anything.
	source := e.Source
	if source == "" {
		source = relay.SourceReported
	}

	meta := relay.Meta{
		Tenant:   tenant, // from the key, never the body
		User:     e.User,
		Feature:  e.Feature,
		Module:   e.Module,
		Provider: e.Provider,
	}
	meta.EntityType, meta.EntityID = relay.ResolveEntity(e.EntityType, e.EntityID, e.Session)
	// Hints are carried only for the ledger's metadata column: what the original
	// caller declared, stored beside the cost derived from it. They are NOT
	// re-applied to the quantities — the sender already folded them in before
	// measuring, and applying them twice would inflate the row.
	if len(e.Metadata) > 0 {
		meta.Hints = &extract.Hints{Raw: e.Metadata}
	}

	return relay.Event{
		Meta:                meta,
		Source:              source,
		ClientEventID:       e.ClientEventID,
		Modality:            e.Modality,
		Model:               model,
		InputTokens:         e.InputTokens,
		CachedInputTokens:   e.CachedInputTokens,
		OutputTokens:        e.OutputTokens,
		TotalTokens:         e.InputTokens + e.OutputTokens,
		AudioSeconds:        e.AudioSeconds,
		Characters:          e.Characters,
		SurchargeMultiplier: e.SurchargeMultiplier,
		CostUSD:             cost,
		CostKnown:           known,
		OccurredAt:          occurredAt,
	}
}
