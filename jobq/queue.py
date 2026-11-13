"""The queue's state transitions. Every function is one SQL statement.

Callers pass a connection in autocommit mode, so each statement is its own
short transaction: row locks are held for milliseconds, never for the length
of a job. Ownership of a running job is a *lease* instead: a fencing token
plus an expiry time, both stored on the row.
"""

from psycopg import Connection

# Picks the next runnable job, locks it, marks it running, gives it a fresh
# lease and returns it, all in one statement.
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
#   - ORDER BY run_at, id matches jobs_claim_idx, and the WHERE clause matches
#     the index predicate, so the index serves both the filter and the sort.
CLAIM_SQL = """
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
"""

# The fence, shared by heartbeat, complete and fail. A write is accepted only
# if this worker's claim is still the job's current one:
#   - lease_token = %(token)s: nobody has re-claimed the job since. A newer
#     claim would have replaced the token.
#   - state = 'running': the job hasn't been reaped back to 'queued'. A reaped
#     job keeps its old token until it's claimed again, so the token check
#     alone would let a zombie mark a queued job succeeded without running it.
# The lease's expiry time is deliberately *not* checked. If the lease has
# expired but nobody has reaped or re-claimed the job, no one else holds it,
# so the holder's write is still safe.
FENCE = "id = %(id)s AND lease_token = %(token)s AND state = 'running'"

HEARTBEAT_SQL = f"""
UPDATE jobs
SET    lease_expires_at = now() + make_interval(secs => %(lease_seconds)s)
WHERE  {FENCE}
"""

COMPLETE_SQL = f"""
UPDATE jobs SET state = 'succeeded'
WHERE  {FENCE}
"""

# Phase 2: any handler failure is final. Phase 3 adds retries with backoff.
FAIL_SQL = f"""
UPDATE jobs SET state = 'dead', last_error = %(error)s
WHERE  {FENCE}
"""

# Makes running jobs with an expired lease claimable again, or moves them to
# 'dead' if they have used up their attempts (D14). attempts was already
# counted when the job was claimed, so it isn't changed here.
#
# A plain UPDATE with the conditions in its own WHERE clause, so they are
# re-checked on the latest row version if the row is locked: if a heartbeat
# is extending this lease right now, the reaper waits for it, re-checks
# `lease_expires_at < now()` against the extended lease, and leaves the job
# alone. (The Phase 1 lesson: the condition has to sit in the statement
# that's re-checked.)
#
# The old lease_token stays on the row. The fence's state check is what
# rejects the dead worker's late acks.
REAP_SQL = """
UPDATE jobs
SET    state      = CASE WHEN attempts < max_attempts THEN 'queued' ELSE 'dead' END,
       last_error = CASE WHEN attempts < max_attempts THEN last_error ELSE 'lease expired' END
WHERE  state = 'running'
  AND  lease_expires_at < now()
RETURNING id, state, attempts
"""


def claim(conn: Connection, lease_seconds: float) -> dict | None:
    """Claim the next runnable job, or return None if there isn't one."""
    return conn.execute(CLAIM_SQL, {"lease_seconds": lease_seconds}).fetchone()


def heartbeat(conn: Connection, job_id: int, token: int, lease_seconds: float) -> bool:
    """Extend the lease. False means it's lost: stop working on the job."""
    params = {"id": job_id, "token": token, "lease_seconds": lease_seconds}
    return conn.execute(HEARTBEAT_SQL, params).rowcount == 1


def complete(conn: Connection, job_id: int, token: int) -> bool:
    """Mark the job succeeded. False means the ack was rejected by the fence."""
    return conn.execute(COMPLETE_SQL, {"id": job_id, "token": token}).rowcount == 1


def fail(conn: Connection, job_id: int, token: int, error: str) -> bool:
    """Mark the job dead. False means the ack was rejected by the fence."""
    params = {"id": job_id, "token": token, "error": error}
    return conn.execute(FAIL_SQL, params).rowcount == 1


def reap(conn: Connection) -> list[dict]:
    """Requeue (or bury) every running job whose lease has expired."""
    return conn.execute(REAP_SQL).fetchall()
