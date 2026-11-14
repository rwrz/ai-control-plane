// Package usage answers cost questions over the usage_events ledger: what did a
// tenant spend in a window, and how does that break down by user, feature,
// module, model, provider or modality.
package usage

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/stephenafamo/bob"
	"github.com/stephenafamo/bob/dialect/psql"
	"github.com/stephenafamo/scan"
)

// Dimensions that a summary can be grouped by, mapped to their column. This
// whitelist is the ONLY place an API-supplied grouping reaches SQL: the value is
// used to look up a compile-time constant, never interpolated, so an unknown
// dimension is rejected rather than becoming part of the query.
var dimensionColumns = map[string]string{
	"user":     "subject_user",
	"feature":  "feature",
	"module":   "module",
	"model":    "model",
	"provider": "provider",
	"modality": "modality",
}

// Dimensions lists the groupable dimensions, sorted — for error messages and docs.
func Dimensions() []string {
	out := make([]string, 0, len(dimensionColumns))
	for d := range dimensionColumns {
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

// Totals is the aggregate over a set of usage events.
type Totals struct {
	Calls int64 `json:"calls"`
	// UnpricedCalls counts events recorded with cost_known=false — a model with
	// no pricing entry. Their real cost is unknown, NOT zero, so CostUSD
	// understates actual spend whenever this is non-zero. Always surfaced so a
	// missing price shows up as a gap rather than as free usage.
	UnpricedCalls     int64   `json:"unpriced_calls"`
	InputTokens       int64   `json:"input_tokens"`
	CachedInputTokens int64   `json:"cached_input_tokens"`
	OutputTokens      int64   `json:"output_tokens"`
	CostUSD           float64 `json:"cost_usd"`
}

// Group is one row of a grouped summary. Key is empty when the underlying
// column is empty or NULL — i.e. usage that arrived unattributed.
type Group struct {
	Key string `json:"key"`
	Totals
}

// Summary is the response: the window that was queried, the overall totals, and
// (when grouped) the per-key breakdown sorted by cost descending.
type Summary struct {
	Tenant  string    `json:"tenant"`
	From    time.Time `json:"from"`
	To      time.Time `json:"to"`
	GroupBy string    `json:"group_by,omitempty"`
	Total   Totals    `json:"total"`
	Groups  []Group   `json:"groups,omitempty"`
}

// Query selects the events to summarize. From is inclusive, To exclusive.
type Query struct {
	Tenant  string
	From    time.Time
	To      time.Time
	GroupBy string // "" = totals only
	User    string // optional: restrict to one subject_user
}

// Repo reads summaries from the ledger.
type Repo struct{ exec bob.Executor }

// NewRepo builds a read-side repo over a Bob executor.
func NewRepo(exec bob.Executor) *Repo { return &Repo{exec: exec} }

// row is the scan target. group_key is selected only when grouping, so it is
// nullable-by-absence; scan fills the zero value when the column is missing.
type row struct {
	GroupKey          string  `db:"group_key"`
	Calls             int64   `db:"calls"`
	UnpricedCalls     int64   `db:"unpriced_calls"`
	InputTokens       int64   `db:"input_tokens"`
	CachedInputTokens int64   `db:"cached_input_tokens"`
	OutputTokens      int64   `db:"output_tokens"`
	CostUSD           float64 `db:"cost_usd"`
}

// aggregateSQL is the fixed aggregate select list. COALESCE keeps an empty
// window returning zeros rather than NULLs.
const aggregateSQL = `
	count(*) AS calls,
	coalesce(sum(CASE WHEN cost_known THEN 0 ELSE 1 END), 0) AS unpriced_calls,
	coalesce(sum(input_tokens), 0) AS input_tokens,
	coalesce(sum(cached_input_tokens), 0) AS cached_input_tokens,
	coalesce(sum(output_tokens), 0) AS output_tokens,
	coalesce(sum(cost_usd), 0) AS cost_usd`

// Summary aggregates the ledger for one tenant and window. Grouped results are
// sorted by cost descending; the overall total is folded from the same rows, so
// grouping never costs a second query.
//
// The statement is assembled as text rather than through Bob's expression DSL —
// a dynamic GROUP BY over aggregates is what the "raw only when the structure
// cannot be expressed" carve-out exists for. Injection safety does not rely on
// that choice: every value is bound ($1…$n) and the only interpolated identifier
// is a compile-time constant looked up from dimensionColumns.
func (r *Repo) Summary(ctx context.Context, q Query) (*Summary, error) {
	if q.Tenant == "" {
		return nil, fmt.Errorf("tenant is required")
	}
	if !q.To.After(q.From) {
		return nil, fmt.Errorf("`to` must be after `from`")
	}

	groupCol, err := groupColumn(q.GroupBy)
	if err != nil {
		return nil, err
	}

	// Placeholders are "?" — Bob renders them to $1…$n positionally, so args
	// must be appended in the same order their "?" appears in the statement.
	// Tenant scoping is mandatory: a summary must never span tenants.
	args := []any{q.Tenant, q.From, q.To}
	sql := "SELECT "
	if groupCol != "" {
		// COALESCE so NULL subject_user (system-initiated calls) lands in the
		// "" bucket instead of a separate null one.
		sql += "coalesce(" + groupCol + ", '') AS group_key, "
	}
	sql += aggregateSQL + `
		FROM usage_events
		WHERE tenant = ? AND occurred_at >= ? AND occurred_at < ?`

	if q.User != "" {
		args = append(args, q.User)
		sql += " AND subject_user = ?"
	}
	if groupCol != "" {
		sql += " GROUP BY coalesce(" + groupCol + ", '')"
	}

	rows, err := bob.All(ctx, r.exec, psql.RawQuery(sql, args...), scan.StructMapper[row]())
	if err != nil {
		return nil, fmt.Errorf("query usage summary: %w", err)
	}

	out := &Summary{Tenant: q.Tenant, From: q.From, To: q.To, GroupBy: q.GroupBy}
	for _, rw := range rows {
		t := Totals{
			Calls:             rw.Calls,
			UnpricedCalls:     rw.UnpricedCalls,
			InputTokens:       rw.InputTokens,
			CachedInputTokens: rw.CachedInputTokens,
			OutputTokens:      rw.OutputTokens,
			CostUSD:           rw.CostUSD,
		}
		addTotals(&out.Total, t)
		if groupCol != "" {
			out.Groups = append(out.Groups, Group{Key: rw.GroupKey, Totals: t})
		}
	}

	sort.Slice(out.Groups, func(i, j int) bool {
		if out.Groups[i].CostUSD != out.Groups[j].CostUSD {
			return out.Groups[i].CostUSD > out.Groups[j].CostUSD
		}
		return out.Groups[i].Key < out.Groups[j].Key // stable for equal cost
	})

	return out, nil
}

func addTotals(dst *Totals, src Totals) {
	dst.Calls += src.Calls
	dst.UnpricedCalls += src.UnpricedCalls
	dst.InputTokens += src.InputTokens
	dst.CachedInputTokens += src.CachedInputTokens
	dst.OutputTokens += src.OutputTokens
	dst.CostUSD += src.CostUSD
}

// groupColumn resolves an API dimension to its column, or "" for no grouping.
func groupColumn(dimension string) (string, error) {
	if dimension == "" {
		return "", nil
	}
	col, ok := dimensionColumns[dimension]
	if !ok {
		return "", fmt.Errorf("unknown group_by %q (want one of %v)", dimension, Dimensions())
	}
	return col, nil
}
