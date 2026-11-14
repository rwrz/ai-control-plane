-- What a row's spend belongs to, as a (type, id) pair: a coaching session, a
-- call, an agent conversation. session_id could only ever say "session", so
-- every other unit of work that wanted its calls summed together — a call's
-- transcription plus the reports generated from it — had nowhere to say so.
--
-- The type is a low-cardinality label the dashboard filters on ("session",
-- "agent_session", "channel"); the id is opaque and only ever grouped by.
-- Both are nullable: request-shaped traffic belongs to no entity, and must not
-- collapse into one enormous one.
ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS entity_type TEXT NULL;
