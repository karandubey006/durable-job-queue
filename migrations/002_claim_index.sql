-- 002: the index the claim query uses.
-- Partial: only queued rows have an entry, so its size tracks the backlog,
-- not the table's history, and finishing a job writes nothing to it.
-- Column order matches the claim's ORDER BY run_at, id.

CREATE INDEX jobs_claim_idx ON jobs (run_at, id) WHERE state = 'queued';
