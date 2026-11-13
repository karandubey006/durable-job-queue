-- 003: leases, fencing tokens and the reaper's index.

-- Every claim takes the next value as its fencing token (D12). Monotonic, so a
-- newer claim always holds a larger token than any older claim of any job.
CREATE SEQUENCE lease_token_seq AS bigint;

ALTER TABLE jobs
    ADD COLUMN lease_token      bigint,
    ADD COLUMN lease_expires_at timestamptz,
    ADD COLUMN last_error       text;

-- Jobs left 'running' by Phase 1 workers have no lease and so could never be
-- reaped. Give them one that has already expired: the reaper then handles
-- them like any other crashed job.
UPDATE jobs
SET    lease_token      = nextval('lease_token_seq'),
       lease_expires_at = now()
WHERE  state = 'running';

-- A running job must hold a lease, or nothing could ever reap it.
ALTER TABLE jobs ADD CONSTRAINT jobs_running_has_lease_check
    CHECK (state <> 'running' OR (lease_token IS NOT NULL AND lease_expires_at IS NOT NULL));

-- The reaper's lookup: running jobs whose lease has expired. Partial, so it
-- holds only running jobs (about one per worker), however long the history.
CREATE INDEX jobs_lease_idx ON jobs (lease_expires_at) WHERE state = 'running';
