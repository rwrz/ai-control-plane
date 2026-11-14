// Package ledger persists priced usage events to CockroachDB via Bob. It
// implements relay.Sink with a buffered, async writer so metering never blocks
// the relay's response path.
package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/aarondl/opt/omit"
	"github.com/aarondl/opt/omitnull"
	"github.com/shopspring/decimal"
	"github.com/stephenafamo/bob"
	"github.com/stephenafamo/bob/types"

	"github.com/rwrz/ai-control-plane/internal/infra/bob/models"
	"github.com/rwrz/ai-control-plane/internal/relay"
)

// Repo writes usage_events. Construct with New; call Close on shutdown to drain.
type Repo struct {
	exec bob.Executor
	log  *slog.Logger
	ch   chan relay.Event
	done chan struct{}

	// Repeated-failure collapsing. A ledger failure is almost never transient
	// and almost always systemic — a schema behind the binary, a revoked
	// password — so it recurs on every single event. Left alone it emits one
	// ERROR per AI call for as long as the condition lasts, which buries every
	// other line in the log and makes the FIRST occurrence, the one carrying
	// the diagnosis, unfindable.
	lastErr      string
	suppressed   int
	lastLogged   time.Time
	failureSince time.Time
}

// repeatLogEvery bounds how often one recurring failure is repeated. Long
// enough to stay quiet through a busy session, short enough that a log tailed
// live still shows the problem is ongoing.
const repeatLogEvery = time.Minute

// New starts the background writer. exec is a Bob executor (bobpgx.NewPool).
func New(exec bob.Executor, log *slog.Logger) *Repo {
	r := &Repo{
		exec: exec,
		log:  log.With("component", "ledger"),
		ch:   make(chan relay.Event, 1024),
		done: make(chan struct{}),
	}
	go r.run()
	return r
}

// Record is non-blocking (called on the relay hot path). Drops + logs if the
// buffer is saturated rather than stalling a response.
func (r *Repo) Record(_ context.Context, ev relay.Event) {
	select {
	case r.ch <- ev:
	default:
		r.log.Warn("ledger buffer full, dropping event", "tenant", ev.Tenant, "user", ev.User)
	}
}

// Close stops accepting events and waits for the buffer to drain.
func (r *Repo) Close() {
	close(r.ch)
	<-r.done
}

func (r *Repo) run() {
	defer close(r.done)
	for ev := range r.ch {
		err := r.insert(context.Background(), ev)
		if isDuplicate(err) {
			// The idempotency index rejecting a row is the mechanism WORKING,
			// not a failure: a consumer or sidecar retried something that had
			// already landed, and the retry was correctly absorbed. Logged at
			// ERROR this reads as lost data and provokes exactly the wrong
			// response — and retries are routine, so it would read that way
			// often.
			r.log.Debug("duplicate usage event ignored (idempotency key already recorded)",
				"client_event_id", ev.ClientEventID, "tenant", ev.Tenant)
			err = nil
		}
		r.report(err, ev)
	}
	r.flushSuppressed()
}

// isDuplicate reports whether err is a unique-constraint violation (23505).
//
// Matched by SQLSTATE rather than by constraint name so it keeps working if the
// index is ever renamed, and without depending on the generated dberrors
// helpers, which assert a lib/pq error type while the runtime driver is pgx.
func isDuplicate(err error) bool {
	if err == nil {
		return false
	}
	var pgErr interface{ SQLState() string }
	return errors.As(err, &pgErr) && pgErr.SQLState() == "23505"
}

// report logs an insert outcome, collapsing an identical error that keeps
// recurring into one line per repeatLogEvery. A change of error — including a
// return to success — flushes the count first, so the log states how many
// events were lost rather than leaving it to be counted by hand.
func (r *Repo) report(err error, ev relay.Event) {
	now := time.Now()

	if err == nil {
		if r.lastErr != "" {
			r.flushSuppressed()
			r.log.Info("ledger writes recovered",
				"failing_for", now.Sub(r.failureSince).Round(time.Second))
			r.lastErr = ""
		}
		return
	}

	msg := err.Error()
	if msg == r.lastErr {
		r.suppressed++
		if now.Sub(r.lastLogged) < repeatLogEvery {
			return
		}
		r.log.Error("insert usage_event (still failing)",
			"err", err, "events_lost", r.suppressed,
			"failing_for", now.Sub(r.failureSince).Round(time.Second))
		r.suppressed, r.lastLogged = 0, now
		return
	}

	// A different error: close out the previous one before starting a new run.
	r.flushSuppressed()
	r.log.Error("insert usage_event", "err", err,
		"tenant", ev.Tenant, "provider", ev.Provider)
	r.lastErr, r.suppressed, r.lastLogged, r.failureSince = msg, 0, now, now
}

// flushSuppressed reports events lost while a repeated error was being
// collapsed, so the count is never silently dropped on recovery or shutdown.
func (r *Repo) flushSuppressed() {
	if r.suppressed == 0 {
		return
	}
	r.log.Error("further usage events lost to the same error",
		"err", r.lastErr, "events_lost", r.suppressed)
	r.suppressed = 0
}

// billingUnit records which quantity the cost was actually derived from, so a
// row can be reconciled against an invoice without re-deriving it from the
// modality. It is not simply a function of modality: OpenAI's gpt-4o-transcribe
// is stt billed in tokens, while whisper-1 is stt billed in seconds.
func billingUnit(ev relay.Event) string {
	switch {
	case ev.Characters > 0:
		return "characters"
	case ev.AudioSeconds > 0:
		return "seconds"
	default:
		return "tokens"
	}
}

func (r *Repo) insert(ctx context.Context, ev relay.Event) error {
	setter := &models.UsageEventSetter{
		Tenant:            omit.From(ev.Tenant),
		Feature:           omit.From(ev.Feature),
		Module:            omit.From(ev.Module),
		Provider:          omit.From(ev.Provider),
		Model:             omit.From(ev.Model),
		Modality:          omit.From(ev.Modality),
		Unit:              omit.From(billingUnit(ev)),
		InputTokens:       omit.From(ev.InputTokens),
		CachedInputTokens: omit.From(ev.CachedInputTokens),
		OutputTokens:      omit.From(ev.OutputTokens),
		Characters:        omit.From(ev.Characters),
		AudioSeconds:      omit.From(decimal.NewFromFloat(ev.AudioSeconds)),
		CostUsd:           omit.From(decimal.NewFromFloat(ev.CostUSD)),
		CostKnown:         omit.From(ev.CostKnown),
		OccurredAt:        omit.From(ev.OccurredAt),
		Source:            omit.From(relay.SourceOf(ev)),
	}
	// Only reporting consumers supply one; a relayed row is measured exactly
	// once by construction and needs no idempotency key.
	if ev.ClientEventID != "" {
		setter.ClientEventID = omitnull.From(ev.ClientEventID)
	}
	// Record what the caller declared, verbatim. Two reasons: a hinted quantity
	// (Scribe's duration) is only auditable if the claim is stored alongside the
	// cost derived from it, and parameters the relay does not bill on yet are
	// preserved for later analysis rather than discarded at the door.
	if ev.Hints != nil && len(ev.Hints.Raw) > 0 {
		setter.Metadata = omitnull.From(types.NewJSON[json.RawMessage](ev.Hints.Raw))
	}
	if ev.User != "" {
		setter.SubjectUser = omitnull.From(ev.User)
	}
	if ev.EntityType != "" {
		setter.EntityType = omitnull.From(ev.EntityType)
	}
	if ev.EntityID != "" {
		setter.EntityID = omitnull.From(ev.EntityID)
	}
	_, err := models.UsageEvents.Insert(setter).One(ctx, r.exec)
	return err
}
