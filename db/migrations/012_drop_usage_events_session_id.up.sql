-- Replaced by (entity_type, entity_id). Run only after the backfill script has
-- returned 0 (see 009): whatever session_id still holds alone is dropped here.
ALTER TABLE usage_events DROP COLUMN IF EXISTS session_id;
