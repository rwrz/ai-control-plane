package usage

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rwrz/ai-control-plane/internal/relay"
)

// groupColumn is the only place a caller-supplied string can influence SQL
// structure. Anything not in the whitelist must be rejected before the query is
// built, so an injection attempt can never become part of the statement.
func TestGroupColumnWhitelist(t *testing.T) {
	valid := map[string]string{
		"user":     "subject_user",
		"feature":  "feature",
		"module":   "module",
		"model":    "model",
		"provider": "provider",
		"modality": "modality",
	}
	for dimension, wantCol := range valid {
		got, err := groupColumn(dimension)
		if err != nil {
			t.Errorf("groupColumn(%q) errored: %v", dimension, err)
		}
		if got != wantCol {
			t.Errorf("groupColumn(%q) = %q, want %q", dimension, got, wantCol)
		}
	}

	if got, err := groupColumn(""); err != nil || got != "" {
		t.Errorf(`groupColumn("") = %q, %v; want "", nil (means: no grouping)`, got, err)
	}

	rejected := []string{
		"tenant DROP TABLE usage_events",
		"subject_user; DELETE FROM usage_events",
		"cost_usd", // a real column, but not a groupable dimension
		"1",        // ordinal reference
		"USER",     // case must match exactly
		"user, cost_usd",
	}
	for _, dimension := range rejected {
		if _, err := groupColumn(dimension); err == nil {
			t.Errorf("groupColumn(%q) was accepted; must be rejected", dimension)
		}
	}
}

func TestParseQueryDefaultsToLast24h(t *testing.T) {
	req := httptest.NewRequest("GET", "/v1/usage/summary?tenant=backend", nil)

	before := time.Now().UTC()
	q, err := parseQuery(req, "backend")
	if err != nil {
		t.Fatalf("parseQuery: %v", err)
	}
	after := time.Now().UTC()

	if q.Tenant != "backend" {
		t.Errorf("Tenant = %q, want backend", q.Tenant)
	}
	if q.To.Before(before) || q.To.After(after) {
		t.Errorf("To = %v, want ~now", q.To)
	}
	if want := q.To.Add(-defaultWindow); !q.From.Equal(want) {
		t.Errorf("From = %v, want %v (to - 24h)", q.From, want)
	}
}

func TestParseQueryValidation(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{name: "explicit window", url: "/v1/usage/summary?tenant=t&from=2026-07-01T00:00:00Z&to=2026-07-02T00:00:00Z"},
		{name: "with grouping", url: "/v1/usage/summary?tenant=t&group_by=module"},
		{name: "with user filter", url: "/v1/usage/summary?tenant=t&user=user-1"},

		// The tenant now comes from the authenticated key, so omitting it in
		// the URL is the NORMAL case rather than an error.
		{name: "tenant omitted, taken from the key", url: "/v1/usage/summary"},
		{name: "empty tenant param ignored", url: "/v1/usage/summary?tenant="},
		{name: "tenant matching the key is accepted", url: "/v1/usage/summary?tenant=t"},
		{name: "tenant other than the key's is refused", url: "/v1/usage/summary?tenant=someone-else", wantErr: true},
		{name: "unknown group_by", url: "/v1/usage/summary?tenant=t&group_by=cost_usd", wantErr: true},
		{name: "non-RFC3339 from", url: "/v1/usage/summary?tenant=t&from=2026-07-01", wantErr: true},
		{name: "to before from", url: "/v1/usage/summary?tenant=t&from=2026-07-02T00:00:00Z&to=2026-07-01T00:00:00Z", wantErr: true},
		{name: "zero-length window", url: "/v1/usage/summary?tenant=t&from=2026-07-01T00:00:00Z&to=2026-07-01T00:00:00Z", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseQuery(httptest.NewRequest("GET", tc.url, nil), "t")
			if tc.wantErr && err == nil {
				t.Error("expected an error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// The overall total is folded from the grouped rows rather than queried
// separately, so the fold must be exact.
func TestAddTotals(t *testing.T) {
	var got Totals
	addTotals(&got, Totals{Calls: 2, UnpricedCalls: 1, InputTokens: 10, CachedInputTokens: 4, OutputTokens: 3, CostUSD: 0.5})
	addTotals(&got, Totals{Calls: 3, UnpricedCalls: 0, InputTokens: 20, CachedInputTokens: 0, OutputTokens: 7, CostUSD: 0.25})

	want := Totals{Calls: 5, UnpricedCalls: 1, InputTokens: 30, CachedInputTokens: 4, OutputTokens: 10, CostUSD: 0.75}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

// TestSummaryRequiresACallerKey pins tenant scoping: without a valid key nobody
// reads anything, and a key can never read another tenant's per-user spend.
func TestSummaryRequiresACallerKey(t *testing.T) {
	keyring, err := relay.ParseKeyring("secret-backend:backend,secret-coach:voice_agent")
	if err != nil {
		t.Fatalf("parse keyring: %v", err)
	}
	h := NewHandler(nil, keyring, slog.New(slog.NewTextHandler(io.Discard, nil)))

	tests := []struct {
		name       string
		key        string
		query      string
		wantStatus int
	}{
		{name: "no credential", query: "?tenant=backend", wantStatus: http.StatusUnauthorized},
		{name: "wrong credential", key: "guessed", query: "", wantStatus: http.StatusUnauthorized},
		// The pre-fix exploit, verbatim: name someone else's tenant and read it.
		{name: "key cannot read another tenant", key: "secret-coach", query: "?tenant=backend",
			wantStatus: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/v1/usage/summary"+tc.query, nil)
			if tc.key != "" {
				req.Header.Set("X-Relay-Key", tc.key)
			}
			rec := httptest.NewRecorder()
			h.summary(rec, req)

			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body)
			}
		})
	}
}

// TestSummaryRefusesWhenNoKeysAreConfigured — an unconfigured keyring fails
// closed, matching the ingest endpoint. Failing open would make per-user spend
// readable by anyone who can reach the host.
func TestSummaryRefusesWhenNoKeysAreConfigured(t *testing.T) {
	keyring, _ := relay.ParseKeyring("")
	h := NewHandler(nil, keyring, slog.New(slog.NewTextHandler(io.Discard, nil)))

	rec := httptest.NewRecorder()
	h.summary(rec, httptest.NewRequest("GET", "/v1/usage/summary?tenant=backend", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 — an unconfigured keyring must not serve", rec.Code)
	}
}

// TestSummaryAcceptsTheKeyInTheQueryChannel — the same three credential
// channels the relay accepts, because a caller presents ONE key whether it is
// relaying a call, reporting one, or reading its own numbers back.
func TestSummaryAcceptsTheKeyInTheQueryChannel(t *testing.T) {
	keyring, _ := relay.ParseKeyring("secret-backend:backend")
	h := NewHandler(nil, keyring, slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := httptest.NewRequest("GET", "/v1/usage/summary?relay_key=secret-backend&group_by=nonsense", nil)
	rec := httptest.NewRecorder()
	h.summary(rec, req)

	// 400 for the bad group_by, NOT 401: the credential was accepted.
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (the key should have authenticated)", rec.Code)
	}
}
