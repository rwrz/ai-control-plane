-- The covering index from 007, with the entity pair in place of session_id.
-- The per-entity panels group on those two columns, and a column missing from
-- INCLUDE sends every row in the window back to the primary index — the 30x
-- regression 007 exists to prevent. INCLUDE cannot be altered in place, so
-- this is a new index; 011 drops the old one once this one exists.
CREATE INDEX idx_usage_events_dashboard_entity
    ON usage_events (tenant, occurred_at DESC)
    INCLUDE (subject_user, feature, module, entity_type, entity_id,
             provider, model, modality, unit,
             input_tokens, cached_input_tokens, output_tokens,
             characters, audio_seconds, cost_usd, cost_known);
