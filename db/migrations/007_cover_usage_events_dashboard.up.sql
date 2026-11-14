-- idx_usage_events_tenant_occurred_at narrows to the right rows, but stores
-- only (tenant, occurred_at, id). Every dashboard panel then aggregates columns
-- that live nowhere but the primary index, so each one pays an index join back
-- to the PK for every row in the window. Measured on production at 840k rows,
-- one panel over the voice agent's seven days of data:
--
--     with the index join      2.5s   (840ms of it in KV, 44 MiB read)
--     touching indexed cols     84ms
--
-- Thirty times, and the dashboard runs nineteen of these on every load. The
-- gap grows with the window, not with the table: a day holds ~50k rows whatever
-- the table's age, but a month already holds 1.5M and is heading for ten.
--
-- INCLUDE rather than CRDB's STORING keyword: both mean the same thing here
-- (CRDB reports these columns as storing=t) but INCLUDE is also Postgres 11+
-- syntax, and this file has to apply to the disposable Postgres `make bobgen`
-- introspects as well as to CockroachDB at runtime.
--
-- The list is every column the dashboard reads and nothing else. Deliberately
-- absent: virtual_key_id and metadata are 0% populated (Phase 2), source has a
-- single distinct value, and client_event_id is the ingest dedup key that no
-- panel groups or sums. Adding them would grow the index for no read.
--
-- idx_usage_events_tenant_occurred_at is now a strict prefix of this index and
-- is a candidate for removal — but not in the same migration that introduces
-- its replacement, because that leaves no cheap way back if this one misbehaves.
CREATE INDEX idx_usage_events_dashboard
    ON usage_events (tenant, occurred_at DESC)
    INCLUDE (subject_user, feature, module, session_id,
             provider, model, modality, unit,
             input_tokens, cached_input_tokens, output_tokens,
             characters, audio_seconds, cost_usd, cost_known);
