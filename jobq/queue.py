"""The queue's state transitions. Every function is one SQL statement.

Callers pass a connection in autocommit mode, so each statement is its own
short transaction: row locks are held for milliseconds, never for the length
of a job.
"""

from psycopg import Connection

# Picks the next runnable job, locks it, marks it running and returns it,
# all in one statement.
#   - FOR UPDATE SKIP LOCKED: a row another worker is claiming right now is
#     skipped rather than waited on, so concurrent claimers never get the
#     same row and never queue up behind each other.
#   - `id = (subquery)` is a scalar subquery, which Postgres runs exactly
#     once. (`id IN (...)` can be planned as a join that re-runs the subquery.)
#   - attempts is incremented here, at claim time, so a job that crashes its
#     worker still uses up an attempt.
#   - ORDER BY run_at, id matches jobs_claim_idx, and the WHERE clause matches
#     the index predicate, so the index serves both the filter and the sort.
CLAIM_SQL = """
UPDATE jobs
SET    state = 'running',
       attempts = attempts + 1
WHERE  id = (
    SELECT id
    FROM   jobs
    WHERE  state = 'queued'
      AND  run_at <= now()
    ORDER  BY run_at, id
    LIMIT  1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, tenant_id, type, payload, attempts, max_attempts
"""

# `AND state = 'running'` keeps finished jobs finished: completing or failing
# a job that isn't running changes nothing and returns False. Phase 2 replaces
# this with a check on the lease token.
COMPLETE_SQL = """
UPDATE jobs SET state = 'succeeded'
WHERE  id = %s AND state = 'running'
"""

# Phase 1: any failure is final. Phase 3 adds retries with backoff.
FAIL_SQL = """
UPDATE jobs SET state = 'dead'
WHERE  id = %s AND state = 'running'
"""


def claim(conn: Connection) -> dict | None:
    """Claim the next runnable job, or return None if there isn't one."""
    return conn.execute(CLAIM_SQL).fetchone()


def complete(conn: Connection, job_id: int) -> bool:
    return conn.execute(COMPLETE_SQL, (job_id,)).rowcount == 1


def fail(conn: Connection, job_id: int) -> bool:
    return conn.execute(FAIL_SQL, (job_id,)).rowcount == 1
