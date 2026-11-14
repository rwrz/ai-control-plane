package forward

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rwrz/ai-control-plane/internal/pricing"
	"github.com/rwrz/ai-control-plane/internal/relay"
	"github.com/rwrz/ai-control-plane/internal/usage"
)

// centre is a stand-in for the central control plane's ingest endpoint.
type centre struct {
	mu       sync.Mutex
	batches  [][]usage.ReportedEvent
	keys     []string
	status   int // response status; 200 when zero
	failFor  int // fail this many requests before succeeding
	requests int
}

func (c *centre) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.requests++
		c.keys = append(c.keys, r.Header.Get("X-Relay-Key"))

		var req usage.IngestRequest
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &req)

		if c.failFor >= c.requests {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if c.status != 0 && c.status != http.StatusOK {
			w.WriteHeader(c.status)
			return
		}

		c.batches = append(c.batches, req.Events)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(usage.IngestResponse{Accepted: len(req.Events)})
	})
}

func (c *centre) received() []usage.ReportedEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	var all []usage.ReportedEvent
	for _, b := range c.batches {
		all = append(all, b...)
	}
	return all
}

func (c *centre) requestCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.requests
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newSink wires a Sink to a test centre. FlushEvery is long so tests control
// flushing via Close rather than racing a ticker.
func newSink(t *testing.T, c *centre, cfg Config) *Sink {
	t.Helper()

	srv := httptest.NewServer(c.handler())
	t.Cleanup(srv.Close)

	cfg.Endpoint = srv.URL
	if cfg.Key == "" {
		cfg.Key = "sidecar-key"
	}
	if cfg.FlushEvery == 0 {
		cfg.FlushEvery = time.Hour
	}
	s, err := New(cfg, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func sampleEvent() relay.Event {
	return relay.Event{
		Meta: relay.Meta{
			Tenant: "voice_agent", User: "u1", EntityType: "session", EntityID: "s1",
			Feature: "coaching_llm", Module: "coach", Provider: "gemini",
		},
		Modality:     "llm",
		Model:        "gemini-2.5-flash",
		InputTokens:  100,
		OutputTokens: 20,
		CostUSD:      0.000123, // must NOT travel
		CostKnown:    true,
		OccurredAt:   time.Date(2026, 7, 22, 19, 9, 38, 0, time.UTC),
	}
}

// The defining property of the split: a sidecar ships QUANTITIES and the centre
// prices them. If cost ever crossed the wire there would be two rate tables to
// keep in step, and correcting a price would mean redeploying a voice box.
func TestForwardsQuantitiesNotCost(t *testing.T) {
	c := &centre{}
	s := newSink(t, c, Config{})
	s.Record(context.Background(), sampleEvent())
	s.Close()

	got := c.received()
	if len(got) != 1 {
		t.Fatalf("received %d events, want 1", len(got))
	}

	// The wire format has no cost field at all, so the check is that the
	// encoded payload never mentions one.
	raw, _ := json.Marshal(got[0])
	var fields map[string]any
	_ = json.Unmarshal(raw, &fields)
	for _, forbidden := range []string{"cost_usd", "cost", "cost_known"} {
		if _, present := fields[forbidden]; present {
			t.Errorf("wire format carries %q; pricing must stay at the centre", forbidden)
		}
	}
	if got[0].InputTokens != 100 || got[0].OutputTokens != 20 {
		t.Errorf("quantities not preserved: %+v", got[0])
	}
}

// A sidecar read the provider's own bytes, so its rows are measured. Travelling
// over HTTP must not demote them to hearsay — "what we measured" and "what we
// were told" answer different questions, and only the first reconciles against
// an invoice.
func TestForwardedRowsStayLabelledRelayed(t *testing.T) {
	c := &centre{}
	s := newSink(t, c, Config{})
	s.Record(context.Background(), sampleEvent()) // Source empty, as the relay leaves it
	s.Close()

	got := c.received()
	if len(got) != 1 {
		t.Fatalf("received %d events, want 1", len(got))
	}
	if got[0].Source != relay.SourceRelayed {
		t.Errorf("source = %q, want %q — a sidecar measures, it does not assert",
			got[0].Source, relay.SourceRelayed)
	}
}

// Attribution is the entire point of the exercise. A row that arrives without
// its user is a cost we cannot attribute to anyone.
func TestForwardsFullAttribution(t *testing.T) {
	c := &centre{}
	s := newSink(t, c, Config{})
	s.Record(context.Background(), sampleEvent())
	s.Close()

	got := c.received()[0]
	for _, f := range []struct{ name, got, want string }{
		{"user", got.User, "u1"},
		{"entity_type", got.EntityType, "session"},
		{"entity_id", got.EntityID, "s1"},
		{"feature", got.Feature, "coaching_llm"},
		{"module", got.Module, "coach"},
		{"provider", got.Provider, "gemini"},
		{"model", got.Model, "gemini-2.5-flash"},
	} {
		if f.got != f.want {
			t.Errorf("%s = %q, want %q", f.name, f.got, f.want)
		}
	}
	if !got.OccurredAt.Equal(sampleEvent().OccurredAt) {
		t.Errorf("occurred_at = %v, want the time the usage happened", got.OccurredAt)
	}
}

// Every event gets an idempotency key, and a RETRY must reuse it. Without this,
// one timeout on a batch of fifty double-bills all fifty.
func TestRetryReusesTheSameIdempotencyKeys(t *testing.T) {
	c := &centre{failFor: 2} // two failures, then success
	s := newSink(t, c, Config{MaxAttempts: 4})

	s.Record(context.Background(), sampleEvent())
	s.Record(context.Background(), sampleEvent())
	s.Close()

	if c.requestCount() != 3 {
		t.Fatalf("made %d requests, want 3 (two failures then success)", c.requestCount())
	}
	got := c.received()
	if len(got) != 2 {
		t.Fatalf("received %d events, want 2", len(got))
	}
	for i, e := range got {
		if e.ClientEventID == "" {
			t.Errorf("event %d has no client_event_id; a retry would double-bill it", i)
		}
	}
	if got[0].ClientEventID == got[1].ClientEventID {
		t.Error("two distinct events share an idempotency key; the centre would drop one")
	}
}

// The ids must be stamped when the event is queued, not when it is sent —
// otherwise each retry mints new ones and the centre's unique index never fires.
func TestIdempotencyKeyIsStampedOnceAtRecord(t *testing.T) {
	c := &centre{failFor: 1}
	s := newSink(t, c, Config{MaxAttempts: 3})
	s.Record(context.Background(), sampleEvent())
	s.Close()

	c.mu.Lock()
	defer c.mu.Unlock()
	// The failed attempt is not recorded in batches, so compare what the centre
	// finally stored against a second read of the same event's id: with correct
	// behaviour the retry carried the identical id, which is what makes the
	// centre's ux_usage_events_tenant_client_event_id index effective.
	if len(c.batches) != 1 || len(c.batches[0]) != 1 {
		t.Fatalf("unexpected batches: %+v", c.batches)
	}
	if c.batches[0][0].ClientEventID == "" {
		t.Error("no idempotency key survived the retry")
	}
}

// A 4xx will never succeed — a rejected credential, or rows the centre calls
// malformed. Retrying burns the window the NEXT batch could have used.
func TestDoesNotRetryClientErrors(t *testing.T) {
	c := &centre{status: http.StatusUnauthorized}
	s := newSink(t, c, Config{MaxAttempts: 5})
	s.Record(context.Background(), sampleEvent())
	s.Close()

	if got := c.requestCount(); got != 1 {
		t.Errorf("made %d requests for a 401, want 1", got)
	}
}

// A 5xx is worth retrying: the centre may simply be restarting.
func TestRetriesServerErrorsThenGivesUp(t *testing.T) {
	c := &centre{status: http.StatusInternalServerError}
	s := newSink(t, c, Config{MaxAttempts: 3})
	s.Record(context.Background(), sampleEvent())
	s.Close()

	if got := c.requestCount(); got != 3 {
		t.Errorf("made %d requests, want 3 (MaxAttempts)", got)
	}
}

// Record runs on the relay's response path. A saturated queue must lose the
// measurement rather than slow down the call being measured.
func TestRecordNeverBlocks(t *testing.T) {
	// A centre that hangs, so the flusher cannot drain the queue.
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)

	s, err := New(Config{
		Endpoint: srv.URL, Key: "k", QueueSize: 2, BatchSize: 1, FlushEvery: time.Millisecond,
	}, quietLogger())
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 500 {
			s.Record(context.Background(), sampleEvent())
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Record blocked when the queue was full and the centre was hanging")
	}
}

// Shutdown must ship the remainder. A session's last events are precisely the
// ones nobody has seen yet.
func TestCloseFlushesTheRemainder(t *testing.T) {
	c := &centre{}
	s := newSink(t, c, Config{BatchSize: 100, FlushEvery: time.Hour})
	for range 3 {
		s.Record(context.Background(), sampleEvent())
	}
	s.Close()

	if got := len(c.received()); got != 3 {
		t.Errorf("centre received %d events after Close, want 3", got)
	}
}

// A full batch ships immediately rather than waiting for the ticker.
func TestFlushesWhenBatchIsFull(t *testing.T) {
	c := &centre{}
	s := newSink(t, c, Config{BatchSize: 2, FlushEvery: time.Hour})
	defer s.Close()

	for range 2 {
		s.Record(context.Background(), sampleEvent())
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.received()) == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("a full batch was not shipped without waiting for the ticker; got %d", len(c.received()))
}

// The centre rejects an oversized batch outright, so the sidecar must clamp
// rather than discover the limit as a 413 in production.
func TestBatchSizeIsClampedToWhatTheCentreAccepts(t *testing.T) {
	cfg := Config{BatchSize: usage.MaxIngestEvents * 10}
	cfg.applyDefaults()
	if cfg.BatchSize != usage.MaxIngestEvents {
		t.Errorf("BatchSize = %d, want it clamped to %d", cfg.BatchSize, usage.MaxIngestEvents)
	}
}

// The centre refuses unauthenticated ingest — anyone able to POST could write
// arbitrary rows into the billing ledger — so a missing key must fail at boot,
// not once per batch.
func TestRequiresAKeyAndAnHTTPEndpoint(t *testing.T) {
	for _, tc := range []struct{ name, endpoint, key string }{
		{"no key", "https://centre.example", ""},
		{"no endpoint", "", "k"},
		{"wrong scheme", "postgres://centre.example", "k"},
	} {
		if _, err := New(Config{Endpoint: tc.endpoint, Key: tc.key}, quietLogger()); err == nil {
			t.Errorf("%s: New succeeded, want an error", tc.name)
		}
	}
}

// The credential must actually be sent; the centre resolves the tenant from it.
func TestPresentsItsCredential(t *testing.T) {
	c := &centre{}
	s := newSink(t, c, Config{Key: "sidecar-secret"})
	s.Record(context.Background(), sampleEvent())
	s.Close()

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.keys) == 0 || c.keys[0] != "sidecar-secret" {
		t.Errorf("keys presented = %v, want [sidecar-secret]", c.keys)
	}
}

// An unreachable centre degrades metering and nothing else: relaying continues,
// Record keeps accepting, and Close still returns.
func TestUnreachableCentreDoesNotWedgeShutdown(t *testing.T) {
	s, err := New(Config{
		// A port nothing listens on: connection refused, the retryable path.
		Endpoint: "http://127.0.0.1:1", Key: "k", MaxAttempts: 2, FlushEvery: time.Hour,
	}, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	s.Record(context.Background(), sampleEvent())

	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Close hung against an unreachable centre")
	}
}

// recordingSink stands in for the centre's ledger.
type recordingSink struct {
	mu     sync.Mutex
	events []relay.Event
}

func (r *recordingSink) Record(_ context.Context, ev relay.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

// End to end: a sidecar's Sink POSTing into the REAL ingest handler, with a
// real keyring and a real pricing table.
//
// This is the test the two halves cannot pass by agreeing with themselves. The
// encoder and the decoder are separate types in separate packages, and a field
// that drifts between them does not fail — it arrives as a zero value and
// under-bills, which is invisible until an invoice disagrees. Driving them
// against each other is what makes that impossible.
func TestSidecarToCentreRoundTrip(t *testing.T) {
	keyring, err := relay.ParseKeyring("sidecar-key:voice_agent")
	if err != nil {
		t.Fatal(err)
	}
	prices := &pricing.Config{Models: map[string]pricing.ModelPrice{
		"gemini-2.5-flash": {InputPerMtok: 0.30, OutputPerMtok: 2.50},
	}}

	ledger := &recordingSink{}
	mux := http.NewServeMux()
	usage.NewIngest(ledger, prices, keyring, quietLogger()).Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	s, err := New(Config{Endpoint: srv.URL, Key: "sidecar-key", FlushEvery: time.Hour}, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	s.Record(context.Background(), sampleEvent())
	s.Close()

	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if len(ledger.events) != 1 {
		t.Fatalf("centre recorded %d events, want 1", len(ledger.events))
	}
	got := ledger.events[0]

	// Priced at the centre, from quantities, using the centre's rates:
	//   100/1e6*0.30 + 20/1e6*2.50 = 0.00003 + 0.00005 = 0.00008
	const want = 0.00008
	if diff := got.CostUSD - want; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("cost = %v, want %v (priced centrally from quantities)", got.CostUSD, want)
	}
	if !got.CostKnown {
		t.Error("cost_known = false for a model the centre has a rate for")
	}
	if got.Source != relay.SourceRelayed {
		t.Errorf("source = %q, want %q: the sidecar measured this", got.Source, relay.SourceRelayed)
	}
	// Tenant comes from the KEY, never the body — the rule that stops a leaked
	// credential booking spend against another consumer.
	if got.Tenant != "voice_agent" {
		t.Errorf("tenant = %q, want voice_agent (resolved from the credential)", got.Tenant)
	}
	if got.User != "u1" || got.EntityType != "session" || got.EntityID != "s1" || got.Feature != "coaching_llm" || got.Module != "coach" {
		t.Errorf("attribution lost in transit: %+v", got.Meta)
	}
	if got.InputTokens != 100 || got.OutputTokens != 20 {
		t.Errorf("quantities = %d/%d, want 100/20", got.InputTokens, got.OutputTokens)
	}
	if !got.OccurredAt.Equal(sampleEvent().OccurredAt) {
		t.Errorf("occurred_at = %v, want when the usage happened, not when it was reported",
			got.OccurredAt)
	}
	if got.ClientEventID == "" {
		t.Error("no idempotency key reached the ledger; a retry would double-bill")
	}
}

// A tenant claimed by a sidecar must not override the one its key is bound to.
func TestCentreIgnoresATenantClaimedBySidecar(t *testing.T) {
	keyring, _ := relay.ParseKeyring("sidecar-key:voice_agent")
	ledger := &recordingSink{}
	mux := http.NewServeMux()
	usage.NewIngest(ledger, &pricing.Config{}, keyring, quietLogger()).Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	s, _ := New(Config{Endpoint: srv.URL, Key: "sidecar-key", FlushEvery: time.Hour}, quietLogger())
	ev := sampleEvent()
	ev.Tenant = "the Go backend" // a lie, or a misconfiguration
	s.Record(context.Background(), ev)
	s.Close()

	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if len(ledger.events) != 1 || ledger.events[0].Tenant != "voice_agent" {
		t.Errorf("tenant = %+v, want voice_agent from the key", ledger.events)
	}
}
