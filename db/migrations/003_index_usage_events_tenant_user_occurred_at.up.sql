-- Per-user drill-down in the dashboard, and the per-user budget checks that
-- Phase 4 enforcement will run on the hot path. subject_user is nullable
-- (system-initiated calls have no user); those rows simply sort together.
CREATE INDEX idx_usage_events_tenant_subject_user_occurred_at
    ON usage_events (tenant, subject_user, occurred_at DESC);
