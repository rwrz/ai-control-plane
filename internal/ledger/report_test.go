package ledger

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/rwrz/ai-control-plane/internal/relay"
)

// newReporter builds a Repo with only the logging fields set. report() needs no
// database, which is the point: this is log behaviour, not persistence.
func newReporter() (*Repo, *bytes.Buffer) {
	var buf bytes.Buffer
	return &Repo{log: slog.New(slog.NewTextHandler(&buf, nil))}, &buf
}

func countLines(buf *bytes.Buffer, substr string) int {
	n := 0
	for _, line := range strings.Split(buf.String(), "\n") {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

// A ledger failure is systemic, not transient: a schema behind the binary fails
// EVERY insert. Staging emitted one ERROR per AI call for nineteen hours, which
// buried the line that actually explained it.
func TestRepeatedFailureIsLoggedOnce(t *testing.T) {
	r, buf := newReporter()
	err := errors.New(`ERROR: column "source" does not exist (SQLSTATE 42703)`)

	for range 50 {
		r.report(err, relay.Event{Meta: relay.Meta{Tenant: "voice_agent", Provider: "gemini"}})
	}

	if got := countLines(buf, "insert usage_event"); got != 1 {
		t.Errorf("logged %d times for one recurring error, want 1", got)
	}
	if got := countLines(buf, "42703"); got == 0 {
		t.Error("the one line logged does not carry the diagnosis")
	}
	if r.suppressed != 49 {
		t.Errorf("suppressed = %d, want 49 — the lost events must still be counted", r.suppressed)
	}
}

// Collapsing must never lose the count. On recovery the log has to say how many
// events went missing, or the gap in the ledger is unexplained.
func TestRecoveryReportsWhatWasLost(t *testing.T) {
	r, buf := newReporter()
	err := errors.New("connection refused")

	for range 10 {
		r.report(err, relay.Event{})
	}
	r.report(nil, relay.Event{}) // the write succeeds again

	if got := countLines(buf, "events_lost=9"); got != 1 {
		t.Errorf("recovery did not report the 9 suppressed events:\n%s", buf.String())
	}
	if got := countLines(buf, "ledger writes recovered"); got != 1 {
		t.Errorf("recovery was not announced:\n%s", buf.String())
	}
	if r.lastErr != "" {
		t.Error("failure state survived a successful write")
	}
}

// A DIFFERENT error is a different problem and must not be swallowed by the
// suppression window of the previous one.
func TestDifferentErrorBreaksSuppression(t *testing.T) {
	r, buf := newReporter()

	for range 5 {
		r.report(errors.New("first problem"), relay.Event{})
	}
	r.report(errors.New("second problem"), relay.Event{})

	if got := countLines(buf, "second problem"); got != 1 {
		t.Errorf("the second error was suppressed along with the first:\n%s", buf.String())
	}
	if got := countLines(buf, "events_lost=4"); got != 1 {
		t.Errorf("the first error's count was not flushed before the second:\n%s", buf.String())
	}
}

// Suppression is time-bounded, so a failure that lasts hours still shows up in
// a log tailed live rather than going silent after the first line.
func TestOngoingFailureIsRestatedPeriodically(t *testing.T) {
	r, buf := newReporter()
	err := errors.New("still broken")

	r.report(err, relay.Event{})
	r.report(err, relay.Event{})
	if got := countLines(buf, "still failing"); got != 0 {
		t.Fatal("restated before the window elapsed")
	}

	// Age the window rather than sleeping through it.
	r.lastLogged = r.lastLogged.Add(-repeatLogEvery - time.Second)
	r.report(err, relay.Event{})

	if got := countLines(buf, "still failing"); got != 1 {
		t.Errorf("ongoing failure never restated:\n%s", buf.String())
	}
}

// Shutdown must not swallow the tail of a suppressed run.
func TestFlushOnShutdownReportsTheRemainder(t *testing.T) {
	r, buf := newReporter()
	for range 7 {
		r.report(errors.New("boom"), relay.Event{})
	}
	r.flushSuppressed() // what run() does when the channel closes

	if got := countLines(buf, "events_lost=6"); got != 1 {
		t.Errorf("shutdown did not report the suppressed remainder:\n%s", buf.String())
	}
}
