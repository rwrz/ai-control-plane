-- Portable DDL: valid on both PostgreSQL (used for bobgen codegen) and
-- CockroachDB (runtime). Avoid CRDB-only types (e.g. STRING) so the same
-- schema introspects under Postgres.
CREATE TABLE usage_events (
    id                  UUID        NOT NULL DEFAULT gen_random_uuid(),
    tenant              TEXT        NOT NULL,
    subject_user        TEXT        NULL,
    feature             TEXT        NOT NULL DEFAULT '',
    module              TEXT        NOT NULL DEFAULT '',
    session_id          TEXT        NULL,
    provider            TEXT        NOT NULL,
    model               TEXT        NOT NULL,
    modality            TEXT        NOT NULL,
    unit                TEXT        NOT NULL,
    input_tokens        BIGINT      NOT NULL DEFAULT 0,
    cached_input_tokens BIGINT      NOT NULL DEFAULT 0,
    output_tokens       BIGINT      NOT NULL DEFAULT 0,
    characters          BIGINT      NOT NULL DEFAULT 0,
    audio_seconds       NUMERIC     NOT NULL DEFAULT 0,
    cost_usd            NUMERIC     NOT NULL DEFAULT 0,
    cost_known          BOOLEAN     NOT NULL DEFAULT true,
    virtual_key_id      TEXT        NULL,
    metadata            JSONB       NULL,
    occurred_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_usage_events PRIMARY KEY (id)
);
