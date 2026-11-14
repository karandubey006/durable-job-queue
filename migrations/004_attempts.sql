-- 004: attempt history, and the DLQ listing index.

-- One row per claim. Inserted by the claim (outcome 'running') and finished by
-- complete, fail or the reaper, always in the same statement as the jobs
-- update (D21).
--
-- Keyed on lease_token: each claim takes a unique token (D12), and the token
-- is what every transition already holds when it finishes the attempt.
-- (job_id, attempt) is unique too, because attempts only ever goes up: a
-- replay grants more attempts instead of resetting the count (D22).
CREATE TABLE job_attempts (
    lease_token bigint      PRIMARY KEY,
    job_id      bigint      NOT NULL REFERENCES jobs (id),
    attempt     integer     NOT NULL,
    worker      text        NOT NULL,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    outcome     text        NOT NULL DEFAULT 'running',
    error       text,

    CONSTRAINT job_attempts_job_attempt_key UNIQUE (job_id, attempt),
    CONSTRAINT job_attempts_outcome_check
        CHECK (outcome IN ('running', 'succeeded', 'failed', 'lease_expired')),
    -- finished_at is set exactly when the attempt has an outcome.
    CONSTRAINT job_attempts_finished_check
        CHECK ((outcome = 'running') = (finished_at IS NULL))
);

-- (The UNIQUE constraint's index also serves a job's history in order:
-- WHERE job_id = ? ORDER BY attempt.)

-- The DLQ listing: a tenant's dead jobs in id order (D20). Partial, so it
-- holds only dead jobs.
CREATE INDEX jobs_dead_idx ON jobs (tenant_id, id) WHERE state = 'dead';

-- Jobs already running when this is applied have no attempt row. Finishing
-- them still works: the attempt-row update in the same statement just
-- matches nothing.
