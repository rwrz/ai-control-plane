// Package forward implements relay.Sink by shipping metered usage to a central
// ai-control-plane over HTTP, instead of writing it to a database directly.
//
// This is what turns one binary into a SIDECAR: the same relay, deployed next
// to a consumer that cannot afford a network hop, reporting what it measured to
// the control plane that owns the ledger.
//
// # Why a sidecar exists at all
//
// Relaying is normally the whole point — proxying a call is the only way to
// know what it cost rather than be told. But on a real-time voice session the
// relay sits between the agent and the provider for every audio frame, and
// latency there is the product. In production the voice agent runs on a dedicated
// voice host while the control plane lives in GCP, so relaying across that gap
// would put a cross-provider round trip inside a conversation.
//
// A sidecar keeps the measurement and removes the distance: the relay runs on
// the same machine, the extra hop is loopback, and only the priced summary —
// kilobytes per session, entirely off the response path — crosses the network.
// The consumer's configuration difference is one variable:
//
//	production   AI_RELAY_URL=http://127.0.0.1:8080     (sidecar, loopback)
//	staging      AI_RELAY_URL=https://<relay-host>      (remote, in GCP)
//
// # What crosses the wire
//
// Quantities, never money. The centre prices what it receives against its own
// pricing.toml, so there is exactly one rate table: correcting a price is a
// redeploy of the control plane, never of a voice box someone has to reach by
// hand. See usage.ReportedEventFrom.
//
// Rows keep their provenance. A sidecar genuinely read the provider's bytes, so
// it reports source="relayed" and its rows stay labelled as measured rather
// than being demoted to hearsay by the act of travelling over HTTP.
//
// # Failure is absorbed, never propagated
//
// Every failure mode here degrades metering and nothing else. Record never
// blocks, the queue drops when saturated, an unreachable centre retries and
// then gives up. A voice session must not stutter because a usage POST is slow,
// and it must not fail because one is failing.
//
// The buffer is memory only. A sidecar killed mid-session loses what it had not
// yet shipped — bounded by FlushEvery, so seconds of metering. Spooling to disk
// would trade that for a durability story (corruption, growth, cleanup) far
// larger than the thing it protects.
package forward

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gofrs/uuid/v5"

	"github.com/rwrz/ai-control-plane/internal/relay"
	"github.com/rwrz/ai-control-plane/internal/usage"
)

// Defaults, sized for a voice agent: a coaching session produces a handful of
// events per minute, so these keep the centre's row age under a minute without
// making a request per event.
const (
	defaultBatchSize   = 50
	defaultFlushEvery  = 15 * time.Second
	defaultQueueSize   = 4096
	defaultTimeout     = 10 * time.Second
	defaultMaxAttempts = 4
)

// retryBackoff is the pause before each retry. Bounded in total (~7s) so a
// failing centre cannot stall shutdown, and so a batch that will never succeed
// is abandoned rather than blocking the ones behind it.
var retryBackoff = []time.Duration{500 * time.Millisecond, 2 * time.Second, 5 * time.Second}

// Config configures a forwarding sink. Only Endpoint and Key are required.
type Config struct {
	// Endpoint is the central control plane's base URL. The ingest path is
	// appended, so this is a host, not a full URL.
	Endpoint string
	// Key is the credential presented to the centre. It resolves the TENANT
	// every forwarded row is booked against — the centre ignores any tenant in
	// the body — so it must be a key issued for this consumer.
	Key string

	BatchSize   int
	FlushEvery  time.Duration
	QueueSize   int
	Timeout     time.Duration
	MaxAttempts int

	// Client overrides the HTTP client, for tests.
	Client *http.Client
}

func (c *Config) applyDefaults() {
	if c.BatchSize <= 0 {
		c.BatchSize = defaultBatchSize
	}
	// The centre rejects an oversized batch outright, so clamp rather than
	// discovering the limit as a 413 in production.
	if c.BatchSize > usage.MaxIngestEvents {
		c.BatchSize = usage.MaxIngestEvents
	}
	if c.FlushEvery <= 0 {
		c.FlushEvery = defaultFlushEvery
	}
	if c.QueueSize <= 0 {
		c.QueueSize = defaultQueueSize
	}
	if c.Timeout <= 0 {
		c.Timeout = defaultTimeout
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = defaultMaxAttempts
	}
}

// Sink batches priced events and POSTs them to a central control plane.
type Sink struct {
	endpoint    string
	key         string
	client      *http.Client
	batchSize   int
	flushEvery  time.Duration
	maxAttempts int
	log         *slog.Logger

	ch   chan relay.Event
	done chan struct{}

	// failing tracks a run of flush failures so an unreachable centre produces
	// one line per failure rather than one per event, and so recovery is
	// announced with what it cost.
	failing    bool
	lostEvents int

	// tenants remembers which tenants have been observed, to warn ONCE if more
	// than one appears. A sidecar serves one consumer by construction, and the
	// centre books every row against the key's tenant — so a second tenant here
	// is silently collapsed into the first, which is a misconfiguration worth
	// naming rather than a merge worth performing.
	tenants map[string]struct{}
}

// New builds a forwarding sink and starts its background writer.
func New(cfg Config, log *slog.Logger) (*Sink, error) {
	cfg.applyDefaults()

	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("forward: endpoint is required")
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("forward: endpoint: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("forward: endpoint must be http(s), got %q", u.Scheme)
	}
	// The centre refuses unauthenticated ingest outright — anyone able to POST
	// could otherwise write arbitrary rows into the billing ledger — so a
	// missing key is a boot-time error here rather than a 503 per batch later.
	if cfg.Key == "" {
		return nil, fmt.Errorf("forward: key is required; the centre resolves the tenant from it")
	}

	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: cfg.Timeout}
	}

	s := &Sink{
		endpoint:    strings.TrimSuffix(cfg.Endpoint, "/") + usage.IngestPath,
		key:         cfg.Key,
		client:      client,
		batchSize:   cfg.BatchSize,
		flushEvery:  cfg.FlushEvery,
		maxAttempts: cfg.MaxAttempts,
		log:         log.With("component", "forward"),
		ch:          make(chan relay.Event, cfg.QueueSize),
		done:        make(chan struct{}),
		tenants:     map[string]struct{}{},
	}
	go s.run()
	return s, nil
}

// Record queues an event. Non-blocking: it is called from the relay's response
// path, where the correct response to a saturated queue is to lose the
// measurement, not to slow down the call being measured.
func (s *Sink) Record(_ context.Context, ev relay.Event) {
	// Stamp an idempotency key here rather than at flush, so a retried batch
	// carries the SAME ids and the centre's unique index recognises it. Without
	// this, one timeout on a batch of fifty double-bills fifty rows.
	if ev.ClientEventID == "" {
		if id, err := uuid.NewV7(); err == nil {
			ev.ClientEventID = id.String()
		}
		// If the generator fails the event still ships, just without retry
		// protection — losing the row entirely would be the worse trade.
	}

	select {
	case s.ch <- ev:
	default:
		s.log.Warn("forward queue full, dropping event",
			"tenant", ev.Tenant, "provider", ev.Provider)
	}
}

// Close drains the queue and ships what is left.
func (s *Sink) Close() {
	close(s.ch)
	<-s.done
}

func (s *Sink) run() {
	defer close(s.done)

	ticker := time.NewTicker(s.flushEvery)
	defer ticker.Stop()

	batch := make([]relay.Event, 0, s.batchSize)
	for {
		select {
		case ev, ok := <-s.ch:
			if !ok {
				// Shutdown: ship the remainder rather than dropping a session's
				// last events, which are the ones nobody has seen yet.
				s.flush(batch)
				return
			}
			s.noteTenant(ev.Tenant)
			batch = append(batch, ev)
			if len(batch) >= s.batchSize {
				s.flush(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				s.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

// noteTenant warns once when a second tenant appears. See Sink.tenants.
func (s *Sink) noteTenant(tenant string) {
	if tenant == "" {
		return
	}
	if _, seen := s.tenants[tenant]; seen {
		return
	}
	s.tenants[tenant] = struct{}{}
	if len(s.tenants) > 1 {
		s.log.Warn("more than one tenant seen by a forwarding relay; "+
			"the centre books every row against the tenant of RELAY_REPORT_KEY, "+
			"so these are being merged",
			"tenants", s.tenantNames())
	}
}

func (s *Sink) tenantNames() []string {
	out := make([]string, 0, len(s.tenants))
	for t := range s.tenants {
		out = append(out, t)
	}
	return out
}

// flush ships one batch, retrying transient failures.
func (s *Sink) flush(batch []relay.Event) {
	if len(batch) == 0 {
		return
	}

	events := make([]usage.ReportedEvent, 0, len(batch))
	for _, ev := range batch {
		events = append(events, usage.ReportedEventFrom(ev))
	}
	body, err := json.Marshal(usage.IngestRequest{Events: events})
	if err != nil {
		// Unreachable in practice, and not retryable if it happens.
		s.log.Error("could not encode usage batch", "err", err, "events", len(events))
		return
	}

	for attempt := range s.maxAttempts {
		if attempt > 0 {
			time.Sleep(retryBackoff[min(attempt-1, len(retryBackoff)-1)])
		}
		retryable, err := s.post(body)
		if err == nil {
			s.noteSuccess(len(events))
			return
		}
		if !retryable {
			// A 4xx means this batch will never be accepted: a rejected
			// credential, or rows the centre considers malformed. Retrying
			// wastes the window in which the NEXT batch could have succeeded.
			s.noteFailure(len(events), err, false)
			return
		}
	}
	s.noteFailure(len(events), fmt.Errorf("gave up after %d attempts", s.maxAttempts), true)
}

// post sends one batch. It reports whether the failure is worth retrying.
func (s *Sink) post(body []byte) (retryable bool, err error) {
	req, err := http.NewRequest(http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Relay-Key", s.key)

	resp, err := s.client.Do(req)
	if err != nil {
		// Network-level: the centre may simply be restarting.
		return true, err
	}
	defer resp.Body.Close()

	// Read the body regardless of status: on success it reports per-row
	// rejections that a 200 alone would hide, and on failure it is the only
	// explanation available.
	payload, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))

	switch {
	case resp.StatusCode == http.StatusOK:
		var out usage.IngestResponse
		if json.Unmarshal(payload, &out) == nil && out.Rejected > 0 {
			// The batch was accepted, so this is not a retryable condition —
			// but rows were dropped at the centre and the sidecar is the only
			// place that can say which fields produced them.
			s.log.Error("central control plane rejected rows from a forwarded batch",
				"rejected", out.Rejected, "accepted", out.Accepted, "errors", out.Errors)
		}
		return false, nil

	case resp.StatusCode == http.StatusTooManyRequests,
		resp.StatusCode >= 500:
		return true, fmt.Errorf("centre returned %d: %s", resp.StatusCode, truncate(payload))

	default:
		return false, fmt.Errorf("centre returned %d: %s", resp.StatusCode, truncate(payload))
	}
}

// noteSuccess announces recovery, with what the outage cost.
func (s *Sink) noteSuccess(n int) {
	if !s.failing {
		return
	}
	s.log.Info("usage forwarding recovered", "events_lost", s.lostEvents)
	s.failing, s.lostEvents = false, 0
	_ = n
}

// noteFailure logs the first failure of a run in full and counts the rest, so a
// centre that is down for an hour does not produce an hour of identical lines.
func (s *Sink) noteFailure(n int, err error, retried bool) {
	s.lostEvents += n
	if s.failing {
		return
	}
	s.failing = true
	s.log.Error("could not forward usage to the central control plane; "+
		"relaying is unaffected, this batch is lost",
		"err", err, "events_lost", n, "retried", retried, "endpoint", s.endpoint)
}

func truncate(b []byte) string {
	const max = 200
	s := strings.TrimSpace(string(b))
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}
