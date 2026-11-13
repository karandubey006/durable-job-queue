"""Claim, heartbeat, complete, fail and reap, tested one statement at a time."""

import threading
import time

import psycopg
import pytest
from psycopg.rows import dict_row

from jobq import queue

LEASE = 30


@pytest.fixture
def qconn(conn, db_url):
    """A dict-row autocommit connection, the way the worker uses it."""
    with psycopg.connect(db_url, autocommit=True, row_factory=dict_row) as qconn:
        yield qconn


def add_job(conn, run_at="now()", type_="noop", max_attempts=5):
    return conn.execute(
        f"INSERT INTO jobs (tenant_id, type, run_at, max_attempts)"
        f" VALUES ('t1', %s, {run_at}, %s) RETURNING id",
        (type_, max_attempts),
    ).fetchone()[0]


def row(conn, job_id):
    cur = conn.execute(
        "SELECT state, attempts, lease_token, last_error FROM jobs WHERE id = %s", (job_id,)
    )
    return dict(zip([c.name for c in cur.description], cur.fetchone()))


def state_of(conn, job_id):
    return row(conn, job_id)["state"]


def expire(conn, job_id):
    """Simulate the holder going silent until its lease ran out."""
    conn.execute(
        "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = %s", (job_id,)
    )


# --- claim ----------------------------------------------------------------


def test_claim_empty_queue_returns_none(qconn):
    assert queue.claim(qconn, LEASE) is None


def test_claim_marks_running_counts_attempt_and_grants_lease(conn, qconn):
    job_id = add_job(conn)

    job = queue.claim(qconn, LEASE)

    assert job["id"] == job_id
    assert job["attempts"] == 1
    assert job["lease_token"] is not None
    assert state_of(conn, job_id) == "running"
    lease_left = conn.execute(
        "SELECT extract(epoch FROM lease_expires_at - now()) FROM jobs WHERE id = %s", (job_id,)
    ).fetchone()[0]
    assert LEASE - 1 < lease_left <= LEASE
    assert queue.claim(qconn, LEASE) is None  # a running job can't be claimed again


def test_claim_skips_jobs_not_yet_due(conn, qconn):
    add_job(conn, run_at="now() + interval '1 hour'")
    assert queue.claim(qconn, LEASE) is None


def test_claim_order_is_run_at_then_id(conn, qconn):
    later = add_job(conn, run_at="now() - interval '1 minute'")
    # These three get the same run_at, so id breaks the tie.
    tied = [
        add_job(conn, run_at="'2026-01-01T00:00:00Z'::timestamptz") for _ in range(3)
    ]

    claimed = [queue.claim(qconn, LEASE)["id"] for _ in range(4)]

    assert claimed == [*tied, later]


def test_claim_skips_a_row_locked_by_another_transaction(conn, qconn, db_url):
    first = add_job(conn, run_at="now() - interval '2 minutes'")
    second = add_job(conn, run_at="now() - interval '1 minute'")

    with psycopg.connect(db_url) as other:  # not autocommit: holds the lock until rollback
        other.execute("SELECT id FROM jobs WHERE id = %s FOR UPDATE", (first,))

        job = queue.claim(qconn, LEASE)  # must not block, and must not take `first`

        assert job["id"] == second
        other.rollback()

    assert state_of(conn, first) == "queued"


def test_every_claim_gets_a_new_larger_token(conn, qconn):
    job_id = add_job(conn)
    first = queue.claim(qconn, LEASE)
    expire(conn, job_id)
    queue.reap(qconn)

    second = queue.claim(qconn, LEASE)

    assert second["id"] == job_id
    assert second["attempts"] == 2
    assert second["lease_token"] > first["lease_token"]


def test_claim_uses_the_partial_index_without_sorting(qconn):
    # The table is tiny, so the planner would happily seq-scan. Forbid that to
    # check the index *can* serve both the WHERE clause and the ORDER BY.
    qconn.execute("SET enable_seqscan = off")
    plan = "\n".join(
        r["QUERY PLAN"]
        for r in qconn.execute("EXPLAIN " + queue.CLAIM_SQL, {"lease_seconds": LEASE})
    )

    assert "jobs_claim_idx" in plan
    assert "Sort" not in plan


# --- heartbeat --------------------------------------------------------------


def test_heartbeat_extends_the_lease(conn, qconn):
    job_id = add_job(conn)
    job = queue.claim(qconn, 1)  # a 1 s lease...

    assert queue.heartbeat(qconn, job_id, job["lease_token"], LEASE) is True

    lease_left = conn.execute(
        "SELECT extract(epoch FROM lease_expires_at - now()) FROM jobs WHERE id = %s", (job_id,)
    ).fetchone()[0]
    assert lease_left > LEASE - 1  # ...now extended to 30 s


def test_heartbeat_with_a_stale_token_is_rejected(conn, qconn):
    job_id = add_job(conn)
    job = queue.claim(qconn, LEASE)

    assert queue.heartbeat(qconn, job_id, job["lease_token"] - 1, LEASE) is False


# --- complete / fail: the fence -----------------------------------------------


def test_complete_and_fail_only_apply_to_running_jobs(conn, qconn):
    done, failed = add_job(conn), add_job(conn)

    assert queue.complete(qconn, done, 0) is False  # still queued
    token = queue.claim(qconn, LEASE)["lease_token"]
    assert queue.complete(qconn, done, token) is True
    assert queue.complete(qconn, done, token) is False  # already succeeded
    assert queue.fail(qconn, done, token, "late") is False  # can't fail a finished job
    assert state_of(conn, done) == "succeeded"

    token = queue.claim(qconn, LEASE)["lease_token"]
    assert queue.fail(qconn, failed, token, "RuntimeError('boom')") is True
    failed_row = row(conn, failed)
    assert (failed_row["state"], failed_row["last_error"]) == ("dead", "RuntimeError('boom')")


def test_stale_workers_late_ack_is_rejected_after_reclaim(conn, qconn):
    """Phase 2 'done when': A's lease expires, B re-claims, A's late acks bounce."""
    job_id = add_job(conn)
    a = queue.claim(qconn, LEASE)
    expire(conn, job_id)  # A goes silent (GC pause, partition...)
    queue.reap(qconn)
    b = queue.claim(qconn, LEASE)

    # A wakes up, unaware, and tries every write it might make.
    assert queue.heartbeat(qconn, job_id, a["lease_token"], LEASE) is False
    assert queue.complete(qconn, job_id, a["lease_token"]) is False
    assert queue.fail(qconn, job_id, a["lease_token"], "zombie") is False
    assert row(conn, job_id)["state"] == "running"
    assert row(conn, job_id)["lease_token"] == b["lease_token"]  # still B's

    assert queue.complete(qconn, job_id, b["lease_token"]) is True


def test_late_ack_on_a_reaped_but_unclaimed_job_is_rejected(conn, qconn):
    """The reaped row still carries A's token; the fence's state check stops A."""
    job_id = add_job(conn)
    a = queue.claim(qconn, LEASE)
    expire(conn, job_id)
    queue.reap(qconn)
    assert row(conn, job_id)["lease_token"] == a["lease_token"]  # token left in place

    assert queue.complete(qconn, job_id, a["lease_token"]) is False
    assert state_of(conn, job_id) == "queued"  # the reaper's decision stands


def test_ack_after_expiry_is_accepted_if_nobody_reaped_it(conn, qconn):
    """The fence checks the token, not the clock: no one else holds the job."""
    job_id = add_job(conn)
    a = queue.claim(qconn, LEASE)
    expire(conn, job_id)

    assert queue.complete(qconn, job_id, a["lease_token"]) is True


# --- reap -------------------------------------------------------------------


def test_reap_requeues_expired_buries_exhausted_and_leaves_the_rest(conn, qconn):
    live, expired, exhausted = add_job(conn), add_job(conn), add_job(conn, max_attempts=1)
    queued, finished = add_job(conn, run_at="now() + interval '1 hour'"), add_job(conn)
    for _ in range(4):
        queue.claim(qconn, LEASE)  # claims live, expired, exhausted and finished
    queue.complete(qconn, finished, row(conn, finished)["lease_token"])
    expire(conn, expired)
    expire(conn, exhausted)

    reaped = queue.reap(qconn)

    assert sorted((r["id"], r["state"]) for r in reaped) == [
        (expired, "queued"), (exhausted, "dead")
    ]
    assert row(conn, expired)["attempts"] == 1  # counted at claim, not again here
    assert row(conn, exhausted)["last_error"] == "lease expired"
    assert state_of(conn, live) == "running"
    assert state_of(conn, queued) == "queued"
    assert state_of(conn, finished) == "succeeded"


def test_reap_waits_for_an_in_flight_heartbeat_and_rechecks(conn, qconn, db_url):
    """A heartbeat racing the reaper wins: the reaper re-checks the extended lease."""
    job_id = add_job(conn)
    job = queue.claim(qconn, LEASE)
    expire(conn, job_id)

    with psycopg.connect(db_url) as hb:  # not autocommit: the heartbeat's lock is held
        assert queue.heartbeat(hb, job_id, job["lease_token"], LEASE)

        result = {}
        reaper = threading.Thread(target=lambda: result.update(reaped=queue.reap(qconn)))
        reaper.start()
        time.sleep(0.3)
        assert reaper.is_alive()  # blocked on the heartbeat's row lock

        hb.commit()
        reaper.join(timeout=5)

    assert result["reaped"] == []
    assert state_of(conn, job_id) == "running"


def test_reap_uses_the_lease_index(qconn):
    qconn.execute("SET enable_seqscan = off")
    plan = "\n".join(r["QUERY PLAN"] for r in qconn.execute("EXPLAIN " + queue.REAP_SQL))
    assert "jobs_lease_idx" in plan
