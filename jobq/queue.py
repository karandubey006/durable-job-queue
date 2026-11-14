"""The queue's state transitions. Every function is one SQL statement.

Connections must use dict rows (psycopg.rows.dict_row).

Callers pass a connection in autocommit mode, so each statement is its own
short transaction: row locks are held for milliseconds, never for the length
of a job. Ownership of a running job is a *lease* instead: a fencing token
plus an expiry time, both stored on the row.

Each transition also writes the job's attempt history (job_attempts) in the
same statement, using data-modifying CTEs (D21): the job row and its history
commit together or not at all. In a CTE chain, every sub-statement runs
exactly once and sees the same snapshot; a later one can read an earlier
one's RETURNING rows, which is how the history write is tied to the jobs
write.
"""

from psycopg import Connection
from psycopg.types.json import Jsonb

# The columns a job is returned with, by submit and by the API.
JOB_COLUMNS = (
    "id, tenant_id, type, payload, state, attempts, max_attempts, run_at, created_at,"
    " last_error, idempotency_key"
)

# Submission (D24). With an idempotency key, the UNIQUE (tenant_id,
# idempotency_key) constraint decides who's first, atomically:
#   - No conflict: the job is inserted and returned. A new job.
#   - Conflict: nothing is written and nothing is returned. If another
#     transaction is inserting the same key right now, this statement *waits*
#     for it to commit or roll back first, so a reported conflict always means
#     a committed job holds the key.
# DO NOTHING instead of a no-op DO UPDATE: a duplicate writes no new row
# version, so it leaves no dead tuples behind in the hottest table.
# Without a key (NULL), nothing ever conflicts: every submit is a new job.
SUBMIT_SQL = f"""
INSERT INTO jobs (tenant_id, type, payload, max_attempts, run_at, idempotency_key)
VALUES (%(tenant_id)s, %(type)s, %(payload)s, %(max_attempts)s,
        COALESCE(%(run_at)s, now()), %(idempotency_key)s)
ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
RETURNING {JOB_COLUMNS}
"""

# The job that won the key. In Read Committed every statement takes a fresh
# snapshot, so this sees the winner that SUBMIT_SQL just conflicted with.
# (Under Repeatable Read it wouldn't: the snapshot would predate the commit.)
SUBMITTED_SQL = f"""
SELECT {JOB_COLUMNS} FROM jobs
WHERE  tenant_id = %(tenant_id)s AND idempotency_key = %(idempotency_key)s
"""


# Picks the next runnable job, locks it, marks it running, gives it a fresh
# lease, records the attempt and returns the job, all in one statement.
#   - FOR UPDATE SKIP LOCKED: a row another worker is claiming right now is
#     skipped rather than waited on, so concurrent claimers never get the
#     same row and never queue up behind each other.
#   - `id = (subquery)` is a scalar subquery, which Postgres runs exactly
#     once. (`id IN (...)` can be planned as a join that re-runs the subquery.)
#   - attempts is incremented here, at claim time, so a job that crashes its
#     worker still uses up an attempt.
#   - lease_token: a new value from the sequence on every claim, so each claim
#     of the same job holds a different, larger token. Every later write by
#     this worker must present it (the fence).
#   - lease_expires_at uses the database's now(), so every lease is set and
#     checked against one clock and worker clock drift can't matter.
#   - run_at <= now(): a job backing off after a failure is just a queued row
#     whose run_at is in the future (D18). It waits here, with no timer.
#   - ORDER BY run_at, id matches jobs_claim_idx, and the WHERE clause matches
#     the index predicate, so the index serves both the filter and the sort.
#   - The `attempt` CTE inserts the history row from claimed's RETURNING. If
#     nothing was claimed, it inserts nothing.
CLAIM_SQL = """
WITH claimed AS (
    UPDATE jobs
    SET    state            = 'running',
           attempts         = attempts + 1,
           lease_token      = nextval('lease_token_seq'),
           lease_expires_at = now() + make_interval(secs => %(lease_seconds)s)
    WHERE  id = (
        SELECT id
        FROM   jobs
        WHERE  state = 'queued'
          AND  run_at <= now()
        ORDER  BY run_at, id
        LIMIT  1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING id, tenant_id, type, payload, attempts, max_attempts, lease_token
), attempt AS (
    INSERT INTO job_attempts (lease_token, job_id, attempt, worker)
    SELECT lease_token, id, attempts, %(worker)s FROM claimed
)
SELECT * FROM claimed
"""

# The fence, shared by heartbeat, complete and fail. A write is accepted only
# if this worker's claim is still the job's current one:
#   - lease_token = %(token)s: nobody has re-claimed the job since. A newer
#     claim would have replaced the token.
#   - state = 'running': the job hasn't been reaped back to 'queued'. A reaped
#     job keeps its old token until it's claimed again, so the token check
#     alone would let a holder whose lease was already revoked still ack,
#     overriding the reaper's decision (or a DLQ replay).
# The lease's expiry time is deliberately *not* checked. If the lease has
# expired but nobody has reaped or re-claimed the job, no one else holds it,
# so the holder's write is still safe.
FENCE = "id = %(id)s AND lease_token = %(token)s AND state = 'running'"

HEARTBEAT_SQL = f"""
UPDATE jobs
SET    lease_expires_at = now() + make_interval(secs => %(lease_seconds)s)
WHERE  {FENCE}
"""

# Finishing an attempt: `done` is the fenced jobs update. The history update
# only touches the attempt whose token `done` returned, so a rejected ack
# (done is empty) changes neither table.
COMPLETE_SQL = f"""
WITH done AS (
    UPDATE jobs SET state = 'succeeded'
    WHERE  {FENCE}
    RETURNING lease_token
), attempt AS (
    UPDATE job_attempts
    SET    outcome = 'succeeded', finished_at = now()
    WHERE  lease_token IN (SELECT lease_token FROM done)
)
SELECT lease_token FROM done
"""

# A failed attempt either retries or dies:
#   - retryable and attempts < max_attempts: back to 'queued' with
#     run_at = now() + delay. The claim won't see it until then (D18).
#   - otherwise (permanent error, or no attempts left): 'dead', the DLQ (D20).
# attempts was already counted at claim, so `attempts < max_attempts` means
# "at least one attempt left". The delay is computed by the caller (full
# jitter in Python) and ignored if the job dies. last_error is set either way.
FAIL_SQL = f"""
WITH failed AS (
    UPDATE jobs
    SET    state      = CASE WHEN %(retryable)s AND attempts < max_attempts
                             THEN 'queued' ELSE 'dead' END,
           run_at     = CASE WHEN %(retryable)s AND attempts < max_attempts
                             THEN now() + make_interval(secs => %(delay)s)
                             ELSE run_at END,
           last_error = %(error)s
    WHERE  {FENCE}
    RETURNING lease_token, state
), attempt AS (
    UPDATE job_attempts
    SET    outcome = 'failed', finished_at = now(), error = %(error)s
    WHERE  lease_token IN (SELECT lease_token FROM failed)
)
SELECT state FROM failed
"""

# Makes running jobs with an expired lease claimable again, or moves them to
# 'dead' if they have used up their attempts (D14). attempts was already
# counted when the job was claimed, so it isn't changed here. run_at is left
# alone, so a reaped job is picked up right away (D19: no backoff for
# expiries).
#
# The jobs UPDATE has the conditions in its own WHERE clause, so they are
# re-checked on the latest row version if the row is locked: if a heartbeat
# is extending this lease right now, the reaper waits for it, re-checks
# `lease_expires_at < now()` against the extended lease, and leaves the job
# alone. (The Phase 1 lesson: the condition has to sit in the statement
# that's re-checked.)
#
# The old lease_token stays on the row. The fence's state check is what
# rejects the dead worker's late acks. The attempt is closed as
# 'lease_expired'; its finished_at is when the reaper noticed, not when the
# worker actually died.
REAP_SQL = """
WITH reaped AS (
    UPDATE jobs
    SET    state      = CASE WHEN attempts < max_attempts THEN 'queued' ELSE 'dead' END,
           last_error = CASE WHEN attempts < max_attempts THEN last_error ELSE 'lease expired' END
    WHERE  state = 'running'
      AND  lease_expires_at < now()
    RETURNING id, state, attempts, lease_token
), attempt AS (
    UPDATE job_attempts
    SET    outcome = 'lease_expired', finished_at = now(), error = 'lease expired'
    WHERE  lease_token IN (SELECT lease_token FROM reaped)
)
SELECT id, state, attempts FROM reaped
"""

# DLQ replay (D22): only a dead job, and only the tenant's own. It continues
# the same job with more attempts: max_attempts = attempts + N, where N
# defaults to the job's current max_attempts. attempts keeps counting, so
# attempt numbers never repeat and the history reads as one story.
# last_error stays: it still describes the most recent attempt.
# The old lease_token stays too, but the fence's state check rejects any ack
# carrying it, and the next claim replaces it.
REPLAY_SQL = """
UPDATE jobs
SET    state        = 'queued',
       max_attempts = attempts + COALESCE(%(extra_attempts)s::integer, max_attempts),
       run_at       = now()
WHERE  id = %(id)s AND tenant_id = %(tenant_id)s AND state = 'dead'
RETURNING id
"""


class KeyReused(Exception):
    """The idempotency key already belongs to a job with a different request (D23)."""


def submit(
    conn: Connection,
    tenant_id: str,
    type_: str,
    payload: dict,
    max_attempts: int,
    run_at=None,
    idempotency_key: str | None = None,
) -> tuple[dict, bool]:
    """Enqueue a job. Returns (job, created): created is False when the key
    already had a job and that existing job is returned instead.

    Raises KeyReused if the existing job was submitted with a different type,
    payload or max_attempts (D23). run_at isn't compared: an omitted run_at
    means "now", which differs on every retry.
    """
    params = {
        "tenant_id": tenant_id,
        "type": type_,
        "payload": Jsonb(payload),
        "max_attempts": max_attempts,
        "run_at": run_at,
        "idempotency_key": idempotency_key,
    }
    job = conn.execute(SUBMIT_SQL, params).fetchone()
    if job is not None:
        return job, True
    existing = conn.execute(SUBMITTED_SQL, params).fetchone()
    if (existing["type"], existing["payload"], existing["max_attempts"]) != (
        type_, payload, max_attempts
    ):
        raise KeyReused(f"idempotency key {idempotency_key!r} was used for a different request")
    return existing, False


def claim(conn: Connection, lease_seconds: float, worker: str) -> dict | None:
    """Claim the next runnable job, or return None if there isn't one."""
    return conn.execute(CLAIM_SQL, {"lease_seconds": lease_seconds, "worker": worker}).fetchone()


def heartbeat(conn: Connection, job_id: int, token: int, lease_seconds: float) -> bool:
    """Extend the lease. False means it's lost: stop working on the job."""
    params = {"id": job_id, "token": token, "lease_seconds": lease_seconds}
    return conn.execute(HEARTBEAT_SQL, params).rowcount == 1


def complete(conn: Connection, job_id: int, token: int) -> bool:
    """Mark the job succeeded. False means the ack was rejected by the fence."""
    return conn.execute(COMPLETE_SQL, {"id": job_id, "token": token}).fetchone() is not None


def fail(
    conn: Connection, job_id: int, token: int, error: str, retryable: bool, delay: float
) -> str | None:
    """Record a failed attempt. Returns the job's new state ('queued' to retry
    after `delay` seconds, or 'dead'), or None if the fence rejected the ack."""
    params = {"id": job_id, "token": token, "error": error, "retryable": retryable, "delay": delay}
    row = conn.execute(FAIL_SQL, params).fetchone()
    return None if row is None else row["state"]


def reap(conn: Connection) -> list[dict]:
    """Requeue (or bury) every running job whose lease has expired."""
    return conn.execute(REAP_SQL).fetchall()


def replay(
    conn: Connection, job_id: int, tenant_id: str, extra_attempts: int | None = None
) -> bool:
    """Move a dead job back to queued with `extra_attempts` more attempts
    (default: as many as its current max_attempts)."""
    params = {"id": job_id, "tenant_id": tenant_id, "extra_attempts": extra_attempts}
    return conn.execute(REPLAY_SQL, params).fetchone() is not None
