# Grafana dashboard — AI usage & cost

Phase 1's dashboard is Grafana reading CockroachDB directly. The cluster already
runs Grafana, so this adds a datasource, a read-only DB
user and one dashboard — no new infrastructure and no frontend code.

## Why it queries the database, not `/v1/usage/summary`

A deliberate split, worth understanding before changing it:

| | Grafana | `GET /v1/usage/summary` |
|---|---|---|
| Audience | internal ops | programmatic callers, future per-tenant UI |
| Scope | **all tenants** (a `tenant` template variable filters, it does not enforce) | one tenant, enforced server-side |
| Access | read-only DB role | HTTP + `X-Relay-Key`; the key decides the tenant |

The consequence: **tenant scoping is not enforced in Grafana.** Whoever can open
the dashboard can select any tenant. That is fine while "tenant" distinguishes
our own systems (`backend`, `voice_agent`) and Grafana is staff-only. It stops
being fine the moment a real external tenant exists — at that point the customer
view must move to the API, which enforces scoping in one place.

## Setup

**1. Create the read-only user** (edit the password first):

```bash
cockroach sql --url "$ADMIN_DB_URL" --database=aicontrolplane < deploy/grafana/readonly_user.sql
```

It can `SELECT` on `usage_events` and nothing else. `usage_events` is an
append-only record of money spent and the relay is its only writer — a dashboard
must never be able to mutate it.

**2. Add the datasource** — Grafana → Connections → Add → **PostgreSQL**
(CockroachDB speaks the Postgres wire protocol):

| Field | Value |
|---|---|
| Host | `<cockroachdb-service>:26257` |
| Database | `aicontrolplane` |
| User | `grafana_ro` |
| TLS/SSL Mode | `require` |
| Version | 15 |

**3. Import the dashboard** — Dashboards → New → Import → upload
`dashboard.json`, then pick the datasource. It carries a fixed UID
(`ai-control-plane-usage`), so re-importing updates in place rather than
creating duplicates.

## Panels

Top row is the at-a-glance state; the rest is attribution.

- **Total cost / Calls** — spend and volume for the window.
- **Unpriced calls** — red whenever non-zero. These are calls whose model had no
  entry in `pricing.toml`, recorded as `cost_usd=0, cost_known=false`. Their real
  cost is *unknown, not zero*, so a non-zero count means **Total cost is an
  underestimate**. Fix by adding the model to `pricing.toml`.
- **Prompt cache hit rate** — share of input tokens served from the provider's
  prompt cache, which bills at roughly a tenth of the input rate. The cheapest
  lever on spend: a large shared prompt with a high hit rate is the win.
- **Unattributed calls** — calls with no user. Expected for cron and backfills;
  a sudden rise means some caller is dropping the request context before its AI
  call and per-user cost is being silently lost.
- **Cost over time by module**, **by module / feature / provider**, **top users**,
  **by model**.

### Entity economics

The bottom row answers "what does one unit of work cost" — one coaching session,
one call, one agent job. Every metered call can carry an entity, a
`(entity_type, entity_id)` pair, and every call sharing the pair is summed as one
unit, whichever service or tenant made it.

The **Entity type** dropdown filters **every panel**, not just this row, the same
way `module` does: pick a type and the totals, modules, models and users all
narrow to that kind of work. It defaults to `All`, and `(none)` is the spend no
entity claims yet, so the headline numbers never silently lose it. The dashboard
names no type itself; the vocabulary lives with the callers:

| `entity_type` | sent by | what one entity is |
|---|---|---|
| `session` | the voice agent (the conversation) and the backend's `session` module (its report) | a coaching session, report included |
| `agent_session` | the agents service | a flashcard/agent job |
| `channel` | the backend's `speechrecognition`, `transcription` and `report` workflow steps | a call: its transcription plus every participant's report |

Before the entity pair existed these were one `session_id` column shared by two
workloads with nothing else in common, and the only way to separate them was the
`tenant` dropdown. This row always leaves out calls with no entity, whatever the
dropdown says: the backend's request-shaped traffic belongs to no unit of work,
and folding it in would collapse every such call into a single enormous "entity".
Entities are grouped by the pair, because an id is only unique within its type.
With several types selected the row averages across them — useful as a blended
"cost per unit of work", but pick one type to size any one of them.

- **Avg cost per entity** and **Median cost per entity** — read them together.
  Entity cost is long-tailed, so the mean follows the tail and the median
  follows the typical entity; a mean far above the median means a few long
  entities are carrying the bill.
- **Entities** — the denominator for both, and the sanity check: if it
  moves when you did not expect it to, the averages moved for volume reasons, not
  price.
- **Avg entity length** — wall clock from an entity's first metered call to its
  last. For a `session` it is a **floor**, not the true length: the relay only
  sees a session while it is calling a provider, so silence before the first
  utterance and after the last is invisible. For a `channel` it measures
  something else entirely — a call is transcribed and reported *after* it ends,
  on demand, so this is how long that work was spread out, not how long anyone
  talked. It is here to tell "entities got longer" apart from "entities got
  pricier" when the cost moves.
- **Avg cost per entity over time** — each entity is bucketed by when it
  *started*, so it is counted once rather than smeared across every bucket it
  touched. Entities straddling the edge of the range are truncated by the time
  filter and read cheaper than they were: negligible over a day, material over an
  hour.
- **Where an entity's money goes** — each modality's share of the *average*
  entity. The denominator is every entity in the window, not just the ones that
  used that modality: a session that made no TTS call spent zero on TTS, and
  dividing by only the sessions that did use it would overstate what TTS adds to
  a typical session. `entities_using` is kept alongside so the two stay
  distinguishable — a modality used by half the entities at twice the cost looks
  identical in `avg_per_entity` to one used by all of them at half.
  For `session`, `stt` sits below `tts` and `llm` by a wide margin: those are
  sessions where the user never said anything. A real property of the workload,
  not a metering gap — do not go looking for missing STT rows.

⚠️ These panels honour the `modality`, `module` and `feature` dropdowns like
every other panel. That is deliberate, but it changes the question: with
`modality = tts` selected, **Avg cost per entity** is the average *TTS* cost of
an entity, not the average entity. And an entity spans modules by design — a
`channel` is `transcription` plus `report`, a `session` is `coach` plus
`session` — so a narrowed `module` or `feature` shows a slice of each entity,
not the entity. Leave all three on `All` for the headline number.

### Unit economics

- **Effective rate by model** — what each model actually charged per unit of
  work, computed from the ledger rather than read from `pricing.toml`. This is
  the number that moves when a provider changes price, a caller switches model,
  or a surcharge lands that the rate table does not know about. Each modality
  bills differently, so the rate carries its unit: `$/Mtok`, `$/Mchar`,
  `$/audio-hour`. Two models both priced "per character" are not comparable until
  you compute this — the spread between two TTS voices can be 6x, and no cost
  total shows it.
- **Prompt cache by module (LLM)** — the headline hit-rate stat is one number for
  the fleet; this splits it by module with spend beside it. Sorted by spend, not
  by rate, deliberately: a module at 0% that spends nothing is noise, and one at
  5% that is third on the bill is the whole point. `spend x (1 - hit_rate)` is
  roughly the money still on the table.
- **Cost by feature** — `feature` is the finest label callers attach (33 values
  against `module`'s 20) and the level at which a cost belongs to a piece of
  product. Grouped by feature *and* module, because a feature name is only unique
  within its module: `speech_synthesis` exists under both `coach` and `tts`.
  `cost_per_call` is what the bar gauge cannot show — a feature reaches the top
  of a cost ranking through volume or through price, and the two want different
  fixes.

⚠️ **`unit` is grouped, never `max(unit)`.** A model does not always bill one
way: `scribe_v2` records ~312k calls in `seconds` and ~134 in `tokens`, and
`max()` returns `tokens` because it sorts later. Both this panel and **By model**
did exactly that, reporting the biggest STT model's volume as a token count of
zero and hiding 1.8M seconds of audio. A model that genuinely bills one way still
produces exactly one row, so grouping costs nothing. The stray rows are real — a
call with a provider-stated cost and no measurable quantity records the default
unit — and a NULL rate against a non-zero cost is their signature.

## Variables

Every query is scoped by `tenant`, `user`, `module`, `feature` and `modality`
plus Grafana's time range, which is exactly the access path the
`(tenant, occurred_at DESC)` index serves. `feature` is in that index's
`INCLUDE` list, so filtering on it stays an index-only scan.

`feature` is the only dropdown whose options depend on the others: its query is
narrowed by the selected `tenant` and `module`. A feature name is only unique
within its module (`speech_synthesis` is both `coach` and `tts`; `transcription`
is both `pronunciation` and legacy `speechrecognition`), so pick the module
first — selecting a shared name with `module = All` sums every module that uses
it. Empty features show as `(unlabelled)`, the same bucket **Cost by feature**
uses, and selecting it matches exactly those rows.

`tenant`, `module`, `feature`, `modality` and `entity_type` are dropdowns.
`user` is deliberately **not**: it is a `textbox`, an empty free-text field
that filters only once you type an exact `subject_user` into it.

A query-backed dropdown was rejected because Grafana would have to enumerate the
options — `SELECT DISTINCT subject_user FROM usage_events` — on every dashboard
load, over the one column with unbounded cardinality. That is a full scan of an
append-only table to build a list of thousands of ids nobody scrolls through.
The textbox costs no query at all.

The predicate is written so an empty box is a no-op:

```sql
AND (${user:sqlstring} = '' OR subject_user = ${user:sqlstring})
```

- `:sqlstring` quotes the value and escapes `'` as `''`, which is the correct
  escaping for CRDB. Do not drop it for a bare `'$user'` — the datasource role is
  read-only, but a stray apostrophe in an id would still break every panel.
- The comparison is against the raw column, **not** the
  `coalesce(nullif(subject_user, ''), '(unattributed)')` wrapper the panels use
  for display. The wrapper would defeat
  `(tenant, subject_user, occurred_at DESC)`; the raw column keeps the tight
  index spans, so filtering to one user makes the dashboard faster, not slower.
  The consequence is that typing `(unattributed)` matches nothing — that is what
  the **Unattributed calls** stat is for.
- Filling the box also drives **Unattributed calls** to zero, by construction: a
  named user's calls are attributed. Panels that ignored the global filter would
  be worse — they would silently report a different scope than the one shown.

The five dropdowns refresh **on dashboard load**, not on time range change.
Their queries never reference the time range, so re-running them on every zoom
could not change the result — it only re-ran full-table `DISTINCT` scans
before any panel rendered. `feature` additionally re-runs whenever `tenant` or
`module` changes; Grafana does that for any variable that references another.


## Grafana gotchas

Both apply to any panel with `reduceOptions.values: true` — the bar gauges and
pie charts — and both were real bugs here, not hypotheticals:

- **`min: 0` on a bar gauge, always.** Given no explicit `min`, Grafana derives
  the range from the data. A single row means `min == max`, so the fill fraction
  `(v - min) / (max - min)` is `0/0` — the bar renders empty beside a perfectly
  correct value. Two rows is only marginally better: the cheaper one draws as
  empty because it *is* the minimum. Cost bars have to be measured from zero.
- **`displayName: "${__cell_0}"` is what keeps the row label.** Grafana
  deliberately drops the title when a panel resolves to exactly one value and no
  `displayName` is set — sensible for a stat panel, wrong for a one-row bar gauge
  or a one-slice pie, which then lose the module/feature/provider name entirely.
  `${__cell_0}` is the first column of each row, so it labels correctly whether
  there is one row or twenty. Don't "clean it up".

## CockroachDB gotchas

- **Cast both operands when dividing aggregates.** CRDB's `sum()` over `BIGINT`
  returns `DECIMAL`, and `float / decimal` is rejected — Postgres coerces it
  silently. The cache-hit-rate panel casts both sides for this reason. Any new
  ratio panel must do the same or it will error only against CRDB.
- **The dashboard depends on `idx_usage_events_dashboard_entity`** (migration 010, replacing 007's). It
  is keyed `(tenant, occurred_at DESC)` and `INCLUDE`s every column the panels
  read, so a panel is an index-only scan. Without it the key narrows to the right
  rows but each one needs an index join back to the primary index for its
  `cost_usd`, `modality`, `entity_id`: measured on production, one panel over
  seven days went **2.5s with the join against 84ms without** — and the dashboard
  runs 22 of them per load. The gap scales with the *window*, not the table: a
  day holds ~50k rows whatever the table's age, but a month already holds 1.5M.
  **A new panel that reads a column outside that `INCLUDE` list silently
  reintroduces the join for every panel on the dashboard** — add the column to
  the index in a new migration rather than leaving it out.
- `INCLUDE` rather than CockroachDB's `STORING` keyword: they mean the same thing
  here (CRDB reports the columns as `storing=t`) but `INCLUDE` is also Postgres
  11+ syntax, and migrations must apply to the disposable Postgres `make bobgen`
  introspects as well as to CRDB at runtime.
- **Grafana reads are historical, by role, not by query.**
  `readonly_user.sql` sets `default_transaction_use_follower_reads = true` on
  `grafana_ro`, so every panel reads a settled snapshot ~5s old. `usage_events`
  has one continuous writer and the dashboard runs 22 aggregate scans over it per
  load; a current-timestamp read bumps the timestamp cache (which can push a
  concurrent writer into a retry) and has to resolve any write intents it meets.
  A historical read does neither, and past the closed timestamp it can be served
  by a follower instead of the leaseholder.
  It is set on the **role** because it cannot be set in the panel SQL:
  `AS OF SYSTEM TIME` is only accepted in the `FROM` of a top-level `SELECT`, so
  it parses on a subquery but is a **syntax error** on a CTE — and *Where an
  entity's money goes* is a CTE. A role default also cannot be forgotten by the
  next panel someone adds.
- All 22 panel queries in this file have been executed against CockroachDB, not
  just Postgres — most recently after the `feature` filter was added to each of
  them, with every dropdown on `All`, with `feature` narrowed to a pair and to
  `(unlabelled)`, and with a `user` id containing an apostrophe.
  Do that for new panels too — the dialects diverge quietly.
