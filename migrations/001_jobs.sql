-- 001: the jobs table.
-- Claiming indexes come in Phase 1; leases in Phase 2; tenants FK in Phase 5.

CREATE TABLE jobs (
    id           bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id    text        NOT NULL,
    type         text        NOT NULL,
    payload      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    state        text        NOT NULL DEFAULT 'queued',
    attempts     integer     NOT NULL DEFAULT 0,
    max_attempts integer     NOT NULL DEFAULT 5,
    run_at       timestamptz NOT NULL DEFAULT now(),
    created_at   timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT jobs_state_check
        CHECK (state IN ('queued', 'running', 'succeeded', 'dead')),
    CONSTRAINT jobs_attempts_check     CHECK (attempts >= 0),
    CONSTRAINT jobs_max_attempts_check CHECK (max_attempts > 0)
);
