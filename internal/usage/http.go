package usage

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/rwrz/ai-control-plane/internal/relay"
)

// defaultWindow is the lookback applied when `from` is omitted.
const defaultWindow = 24 * time.Hour

// ErrBadRequest marks caller errors so the handler can answer 400 rather than
// 500. Query construction validates before touching the database.
var ErrBadRequest = errors.New("bad request")

// Handler serves the usage read API.
type Handler struct {
	repo    *Repo
	keyring *relay.Keyring
	log     *slog.Logger
}

// NewHandler builds the read-side HTTP handler.
//
// The keyring is what makes this endpoint tenant-scoped, and it is required
// rather than optional: without it, any caller that can reach the host could
// read every tenant's per-user spend.
func NewHandler(repo *Repo, keyring *relay.Keyring, log *slog.Logger) *Handler {
	return &Handler{repo: repo, keyring: keyring, log: log.With("component", "usage-api")}
}

// Register mounts the usage routes on mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/usage/summary", h.summary)
}

// summary answers GET /v1/usage/summary.
//
//	tenant    required
//	from,to   RFC3339; default = last 24h. from inclusive, to exclusive.
//	group_by  optional: user|feature|module|model|provider|modality
//	user      optional filter
func (h *Handler) summary(w http.ResponseWriter, r *http.Request) {
	tenant, ok := h.authenticate(w, r)
	if !ok {
		return
	}

	q, err := parseQuery(r, tenant)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	out, err := h.repo.Summary(r.Context(), q)
	if err != nil {
		// Repo validation errors are the caller's fault; anything else is ours.
		if errors.Is(err, ErrBadRequest) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		h.log.Error("usage summary failed", "err", err, "tenant", q.Tenant)
		writeError(w, http.StatusInternalServerError, "failed to query usage")
		return
	}

	writeJSON(w, http.StatusOK, out)
}

// authenticate resolves the caller's tenant from its credential.
//
// The tenant comes from the KEY, never from the query string. That is the whole
// point: this endpoint's stated contract is "one tenant, enforced server-side"
// — the property that distinguishes it from the Grafana dashboard, which reads
// every tenant and only filters — and a `?tenant=` read off the URL would
// enforce nothing at all.
//
// Like the ingest endpoint, an unconfigured keyring is refused rather than
// waved through. A read-only endpoint failing closed costs a curl; failing open
// would make per-user spend readable by anyone who can reach the host.
func (h *Handler) authenticate(w http.ResponseWriter, r *http.Request) (string, bool) {
	if !h.keyring.Enabled() {
		h.log.Error("usage summary requires RELAY_AUTH_KEYS; refusing request")
		writeError(w, http.StatusServiceUnavailable,
			"usage summary is not configured; no caller keys are set")
		return "", false
	}
	tenant, ok := h.keyring.Lookup(relay.CredentialFrom(r))
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return "", false
	}
	return tenant, true
}

func parseQuery(r *http.Request, tenant string) (Query, error) {
	v := r.URL.Query()

	q := Query{
		Tenant:  tenant,
		GroupBy: v.Get("group_by"),
		User:    v.Get("user"),
	}
	// A tenant in the URL is honoured only when it agrees with the key. Silently
	// overriding it would answer a question the caller did not ask, with data
	// labelled as the tenant they named.
	if named := v.Get("tenant"); named != "" && named != tenant {
		return q, fmt.Errorf("this key is bound to tenant %q and cannot read %q", tenant, named)
	}
	if _, err := groupColumn(q.GroupBy); err != nil {
		return q, err
	}

	to, err := parseTime(v.Get("to"), time.Now().UTC())
	if err != nil {
		return q, errors.New("`to` must be RFC3339, e.g. 2026-07-21T00:00:00Z")
	}
	from, err := parseTime(v.Get("from"), to.Add(-defaultWindow))
	if err != nil {
		return q, errors.New("`from` must be RFC3339, e.g. 2026-07-21T00:00:00Z")
	}
	if !to.After(from) {
		return q, errors.New("`to` must be after `from`")
	}
	q.From, q.To = from, to

	return q, nil
}

func parseTime(raw string, fallback time.Time) (time.Time, error) {
	if raw == "" {
		return fallback, nil
	}
	return time.Parse(time.RFC3339, raw)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
