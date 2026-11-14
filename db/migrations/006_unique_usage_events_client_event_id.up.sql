-- Makes the idempotency key actually enforce idempotency. Scoped by tenant so
-- two consumers cannot collide on each other's ids. Partial, so the many
-- relayed rows with no key do not all conflict on NULL.
CREATE UNIQUE INDEX IF NOT EXISTS ux_usage_events_tenant_client_event_id
    ON usage_events (tenant, client_event_id)
    WHERE client_event_id IS NOT NULL;
