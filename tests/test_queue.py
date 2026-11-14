"""Claim, heartbeat, complete, fail and reap, tested one statement at a time."""

import threading
import time

import psycopg
import pytest
from psycopg.rows import dict_row

from jobq import queue

LEASE = 30
W = "test-worker"


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
    assert queue.claim(qconn, LEASE, W) is None


def test_claim_marks_running_counts_attempt_and_grants_lease(conn, qconn):
    job_id = add_job(conn)

    job = queue.claim(qconn, LEASE, W)

    assert job["id"] == job_id
    assert job["attempts"] == 1
    assert job["lease_token"] is not None
    assert state_of(conn, job_id) == "running"
    lease_left = conn.execute(
        "SELECT extract(epoch FROM lease_expires_at - now()) FROM jobs WHERE id = %s", (job_id,)
    ).fetchone()[0]
    assert LEASE - 1 < lease_left <= LEASE
    assert queue.claim(qconn, LEASE, W) is None  # a running job can't be claimed again


def test_claim_skips_jobs_not_yet_due(conn, qconn):
    add_job(conn, run_at="now() + interval '1 hour'")
    assert queue.claim(qconn, LEASE, W) is None


def test_claim_order_is_run_at_then_id(conn, qconn):
    later = add_job(conn, run_at="now() - interval '1 minute'")
    # These three get the same run_at, so id breaks the tie.
    tied = [
        add_job(conn, run_at="'2026-01-01T00:00:00Z'::timestamptz") for _ in range(3)
    ]

    claimed = [queue.claim(qconn, LEASE, W)["id"] for _ in range(4)]

    assert claimed == [*tied, later]


def test_claim_skips_a_row_locked_by_another_transaction(conn, qconn, db_url):
    first = add_job(conn, run_at="now() - interval '2 minutes'")
    second = add_job(conn, run_at="now() - interval '1 minute'")

    with psycopg.connect(db_url) as other:  # not autocommit: holds the lock until rollback
        other.execute("SELECT id FROM jobs WHERE id = %s FOR UPDATE", (first,))

        job = queue.claim(qconn, LEASE, W)  # must not block, and must not take `first`

        assert job["id"] == second
        other.rollback()

    assert state_of(conn, first) == "queued"


def test_every_claim_gets_a_new_larger_token(conn, qconn):
    job_id = add_job(conn)
    first = queue.claim(qconn, LEASE, W)
    expire(conn, job_id)
    queue.reap(qconn)

    second = queue.claim(qconn, LEASE, W)

    assert second["id"] == job_id
    assert second["attempts"] == 2
    assert second["lease_token"] > first["lease_token"]


def test_claim_uses_the_partial_index_without_sorting(qconn):
    # The table is tiny, so the planner would happily seq-scan. Forbid that to
    # check the index *can* serve both the WHERE clause and the ORDER BY.
    qconn.execute("SET enable_seqscan = off")
    plan = "\n".join(
        r["QUERY PLAN"]
        for r in qconn.execute("EXPLAIN " + queue.CLAIM_SQL, {"lease_seconds": LEASE, "worker": W})
    )

    assert "jobs_claim_idx" in plan
    assert "Sort" not in plan


# --- heartbeat --------------------------------------------------------------


def test_heartbeat_extends_the_lease(conn, qconn):
    job_id = add_job(conn)
    job = queue.claim(qconn, 1, W)  # a 1 s lease...

    assert queue.heartbeat(qconn, job_id, job["lease_token"], LEASE) is True

    lease_left = conn.execute(
        "SELECT extract(epoch FROM lease_expires_at - now()) FROM jobs WHERE id = %s", (job_id,)
    ).fetchone()[0]
    assert lease_left > LEASE - 1  # ...now extended to 30 s


def test_heartbeat_with_a_stale_token_is_rejected(conn, qconn):
    job_id = add_job(conn)
    job = queue.claim(qconn, LEASE, W)

    assert queue.heartbeat(qconn, job_id, job["lease_token"] - 1, LEASE) is False


# --- complete / fail: the fence -----------------------------------------------


def test_complete_and_fail_only_apply_to_running_jobs(conn, qconn):
    done, failed = add_job(conn), add_job(conn)

    assert queue.complete(qconn, done, 0) is False  # still queued
    token = queue.claim(qconn, LEASE, W)["lease_token"]
    assert queue.complete(qconn, done, token) is True
    assert queue.complete(qconn, done, token) is False  # already succeeded
    assert queue.fail(qconn, done, token, "late", False, 0) is None  # can't fail a finished job
    assert state_of(conn, done) == "succeeded"

    token = queue.claim(qconn, LEASE, W)["lease_token"]
    assert queue.fail(qconn, failed, token, "RuntimeError('boom')", False, 0) == "dead"
    failed_row = row(conn, failed)
    assert (failed_row["state"], failed_row["last_error"]) == ("dead", "RuntimeError('boom')")


def test_stale_workers_late_ack_is_rejected_after_reclaim(conn, qconn):
    """Phase 2 'done when': A's lease expires, B re-claims, A's late acks bounce."""
    job_id = add_job(conn)
    a = queue.claim(qconn, LEASE, W)
    expire(conn, job_id)  # A goes silent (GC pause, partition...)
    queue.reap(qconn)
    b = queue.claim(qconn, LEASE, W)

    # A wakes up, unaware, and tries every write it might make.
    assert queue.heartbeat(qconn, job_id, a["lease_token"], LEASE) is False
    assert queue.complete(qconn, job_id, a["lease_token"]) is False
    assert queue.fail(qconn, job_id, a["lease_token"], "zombie", True, 0) is None
    assert row(conn, job_id)["state"] == "running"
    assert row(conn, job_id)["lease_token"] == b["lease_token"]  # still B's

    assert queue.complete(qconn, job_id, b["lease_token"]) is True


def test_late_ack_on_a_reaped_but_unclaimed_job_is_rejected(conn, qconn):
    """The reaped row still carries A's token; the fence's state check stops A."""
    job_id = add_job(conn)
    a = queue.claim(qconn, LEASE, W)
    expire(conn, job_id)
    queue.reap(qconn)
    assert row(conn, job_id)["lease_token"] == a["lease_token"]  # token left in place

    assert queue.complete(qconn, job_id, a["lease_token"]) is False
    assert state_of(conn, job_id) == "queued"  # the reaper's decision stands


def test_ack_after_expiry_is_accepted_if_nobody_reaped_it(conn, qconn):
    """The fence checks the token, not the clock: no one else holds the job."""
    job_id = add_job(conn)
    a = queue.claim(qconn, LEASE, W)
    expire(conn, job_id)

    assert queue.complete(qconn, job_id, a["lease_token"]) is True


# --- reap -------------------------------------------------------------------


def test_reap_requeues_expired_buries_exhausted_and_leaves_the_rest(conn, qconn):
    live, expired, exhausted = add_job(conn), add_job(conn), add_job(conn, max_attempts=1)
    queued, finished = add_job(conn, run_at="now() + interval '1 hour'"), add_job(conn)
    for _ in range(4):
        queue.claim(qconn, LEASE, W)  # claims live, expired, exhausted and finished
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
    job = queue.claim(qconn, LEASE, W)
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


# --- fail: retry or DLQ -----------------------------------------------------


def seconds_until_run(conn, job_id):
    return conn.execute(
        "SELECT extract(epoch FROM run_at - now()) FROM jobs WHERE id = %s", (job_id,)
    ).fetchone()[0]


def test_retryable_failure_requeues_with_its_backoff_delay(conn, qconn):
    job_id = add_job(conn)
    job = queue.claim(qconn, LEASE, W)

    assert queue.fail(qconn, job_id, job["lease_token"], "TimeoutError()", True, 60) == "queued"

    assert 59 < seconds_until_run(conn, job_id) <= 60
    r = row(conn, job_id)
    assert (r["state"], r["attempts"], r["last_error"]) == ("queued", 1, "TimeoutError()")
    assert queue.claim(qconn, LEASE, W) is None  # backing off: not due yet


def test_retryable_failure_on_the_last_attempt_goes_to_the_dlq(conn, qconn):
    job_id = add_job(conn, max_attempts=2)
    for expected in ("queued", "dead"):
        token = queue.claim(qconn, LEASE, W)["lease_token"]  # delay 0: due at once
        assert queue.fail(qconn, job_id, token, "TimeoutError()", True, 0) == expected
    assert row(conn, job_id)["attempts"] == 2


def test_permanent_failure_skips_the_remaining_attempts(conn, qconn):
    job_id = add_job(conn, max_attempts=5)
    token = queue.claim(qconn, LEASE, W)["lease_token"]

    assert queue.fail(qconn, job_id, token, "KeyError('amount')", False, 60) == "dead"
    assert row(conn, job_id)["attempts"] == 1


# --- job_attempts: history written with every transition --------------------


def history(conn, job_id):
    return conn.execute(
        "SELECT attempt, worker, outcome, error, finished_at IS NOT NULL FROM job_attempts"
        " WHERE job_id = %s ORDER BY lease_token",
        (job_id,),
    ).fetchall()


def test_every_transition_records_the_attempt(conn, qconn):
    ok, bad, crashed = add_job(conn), add_job(conn), add_job(conn)
    tokens = {j["id"]: j["lease_token"] for j in (queue.claim(qconn, LEASE, W) for _ in range(3))}
    assert history(conn, ok) == [(1, W, "running", None, False)]

    queue.complete(qconn, ok, tokens[ok])
    queue.fail(qconn, bad, tokens[bad], "KeyError('x')", False, 0)
    expire(conn, crashed)
    queue.reap(qconn)

    assert history(conn, ok) == [(1, W, "succeeded", None, True)]
    assert history(conn, bad) == [(1, W, "failed", "KeyError('x')", True)]
    assert history(conn, crashed) == [(1, W, "lease_expired", "lease expired", True)]


def test_a_rejected_ack_changes_neither_the_job_nor_its_history(conn, qconn):
    job_id = add_job(conn)
    a = queue.claim(qconn, LEASE, "a")
    expire(conn, job_id)
    queue.reap(qconn)
    queue.claim(qconn, LEASE, "b")

    assert queue.complete(qconn, job_id, a["lease_token"]) is False

    assert [(w, o) for _, w, o, _, _ in history(conn, job_id)] == [
        ("a", "lease_expired"), ("b", "running")
    ]


# --- replay -----------------------------------------------------------------


def kill(qconn, job_id):
    """Claim the job and fail it permanently, landing it in the DLQ."""
    token = queue.claim(qconn, LEASE, W)["lease_token"]
    assert queue.fail(qconn, job_id, token, "KeyError('x')", False, 0) == "dead"


def budget(conn, job_id):
    return conn.execute(
        "SELECT state, attempts, max_attempts, last_error FROM jobs WHERE id = %s", (job_id,)
    ).fetchone()


def test_replay_grants_as_many_attempts_again_by_default(conn, qconn):
    job_id = add_job(conn, max_attempts=5)
    kill(qconn, job_id)  # dead after attempt 1 of 5

    assert queue.replay(qconn, job_id, "t1") is True

    # 1 used + 5 more. last_error still describes the most recent attempt.
    assert budget(conn, job_id) == ("queued", 1, 6, "KeyError('x')")
    assert queue.claim(qconn, LEASE, W)["attempts"] == 2
    # Attempt numbers keep counting: the history reads as one story.
    assert [(n, o) for n, _, o, _, _ in history(conn, job_id)] == [(1, "failed"), (2, "running")]


def test_replay_can_grant_a_specific_number_of_attempts(conn, qconn):
    job_id = add_job(conn, max_attempts=5)
    kill(qconn, job_id)

    assert queue.replay(qconn, job_id, "t1", extra_attempts=2) is True
    assert budget(conn, job_id)[:3] == ("queued", 1, 3)


def test_replay_only_applies_to_the_tenants_own_dead_jobs(conn, qconn):
    queued, dead = add_job(conn), add_job(conn)
    conn.execute("UPDATE jobs SET state = 'dead' WHERE id = %s", (dead,))

    assert queue.replay(qconn, queued, "t1") is False  # not dead
    assert queue.replay(qconn, dead, "someone-else") is False  # not theirs
    assert state_of(conn, dead) == "dead"


def test_a_zombie_from_before_death_cannot_ack_the_replayed_job(conn, qconn):
    job_id = add_job(conn, max_attempts=1)
    a = queue.claim(qconn, LEASE, "a")
    expire(conn, job_id)
    queue.reap(qconn)  # no attempts left: dead, still carrying A's token
    queue.replay(qconn, job_id, "t1")

    assert queue.complete(qconn, job_id, a["lease_token"]) is False  # replayed, not yet claimed
    b = queue.claim(qconn, LEASE, "b")
    assert queue.complete(qconn, job_id, a["lease_token"]) is False  # claimed by B
    assert queue.complete(qconn, job_id, b["lease_token"]) is True
