-- Restores the column, not its data: the values live on as entity_id.
ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS session_id TEXT NULL;
