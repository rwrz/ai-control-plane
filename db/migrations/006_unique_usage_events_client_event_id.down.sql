-- CockroachDB: DROP INDEX, never DROP CONSTRAINT.
DROP INDEX IF EXISTS ux_usage_events_tenant_client_event_id;
