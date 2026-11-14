-- Idempotency key supplied by a reporting consumer.
--
-- A consumer that reports usage over HTTP must be able to retry safely: a
-- timeout tells it nothing about whether the write landed, and a voice agent
-- retrying at session end would otherwise double-bill the session. NULL for
-- relayed rows, which are measured exactly once by construction.
ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS client_event_id TEXT NULL;
