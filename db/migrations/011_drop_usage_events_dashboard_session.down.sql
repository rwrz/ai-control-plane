-- 007's index, verbatim. Needs session_id, so 012's down must run first.
CREATE INDEX idx_usage_events_dashboard
    ON usage_events (tenant, occurred_at DESC)
    INCLUDE (subject_user, feature, module, session_id,
             provider, model, modality, unit,
             input_tokens, cached_input_tokens, output_tokens,
             characters, audio_seconds, cost_usd, cost_known);
