-- Every dashboard query is scoped to one tenant and a time window
-- (WHERE tenant = ? AND occurred_at >= ? AND occurred_at < ?), then grouped.
-- The only existing index is the primary key on a random UUID, which orders
-- rows by nothing useful, so each of those queries scans the whole append-only
-- table. DESC because dashboards read most-recent-first.
CREATE INDEX idx_usage_events_tenant_occurred_at
    ON usage_events (tenant, occurred_at DESC);
