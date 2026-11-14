-- See 008. After this migration, and BEFORE 010, run
-- scripts/backfill_usage_events_entity.sql (`make backfill-entity`): 012 drops
-- session_id, and every row it still holds alone is lost with it.
ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS entity_id TEXT NULL;
