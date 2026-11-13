"""Claim, complete and fail, tested one statement at a time."""

import psycopg
import pytest
from psycopg.rows import dict_row

from jobq import queue


@pytest.fixture
def qconn(conn, db_url):
    """A dict-row autocommit connection, the way the worker uses it."""
    with psycopg.connect(db_url, autocommit=True, row_factory=dict_row) as qconn:
        yield qconn


def add_job(conn, run_at="now()", type_="noop"):
    return conn.execute(
        f"INSERT INTO jobs (tenant_id, type, run_at) VALUES ('t1', %s, {run_at}) RETURNING id",
        (type_,),
    ).fetchone()[0]


def state_of(conn, job_id):
    return conn.execute("SELECT state FROM jobs WHERE id = %s", (job_id,)).fetchone()[0]


def test_claim_empty_queue_returns_none(qconn):
    assert queue.claim(qconn) is None


def test_claim_marks_running_and_counts_attempt(conn, qconn):
    job_id = add_job(conn)

    job = queue.claim(qconn)

    assert job["id"] == job_id
    assert job["attempts"] == 1
    assert state_of(conn, job_id) == "running"
    assert queue.claim(qconn) is None  # a running job can't be claimed again


def test_claim_skips_jobs_not_yet_due(conn, qconn):
    add_job(conn, run_at="now() + interval '1 hour'")
    assert queue.claim(qconn) is None


def test_claim_order_is_run_at_then_id(conn, qconn):
    later = add_job(conn, run_at="now() - interval '1 minute'")
    # These three get the same run_at, so id breaks the tie.
    tied = [
        add_job(conn, run_at="'2026-01-01T00:00:00Z'::timestamptz") for _ in range(3)
    ]

    claimed = [queue.claim(qconn)["id"] for _ in range(4)]

    assert claimed == [*tied, later]


def test_claim_skips_a_row_locked_by_another_transaction(conn, qconn, db_url):
    first = add_job(conn, run_at="now() - interval '2 minutes'")
    second = add_job(conn, run_at="now() - interval '1 minute'")

    with psycopg.connect(db_url) as other:  # not autocommit: holds the lock until rollback
        other.execute("SELECT id FROM jobs WHERE id = %s FOR UPDATE", (first,))

        job = queue.claim(qconn)  # must not block, and must not take `first`

        assert job["id"] == second
        other.rollback()

    assert state_of(conn, first) == "queued"


def test_complete_and_fail_only_apply_to_running_jobs(conn, qconn):
    done, failed = add_job(conn), add_job(conn)

    assert queue.complete(qconn, done) is False  # still queued
    assert queue.claim(qconn)["id"] == done
    assert queue.complete(qconn, done) is True
    assert queue.complete(qconn, done) is False  # already succeeded
    assert queue.fail(qconn, done) is False  # can't fail a finished job
    assert state_of(conn, done) == "succeeded"

    assert queue.claim(qconn)["id"] == failed
    assert queue.fail(qconn, failed) is True
    assert state_of(conn, failed) == "dead"


def test_claim_uses_the_partial_index_without_sorting(qconn):
    # The table is tiny, so the planner would happily seq-scan. Forbid that to
    # check the index *can* serve both the WHERE clause and the ORDER BY.
    qconn.execute("SET enable_seqscan = off")
    plan = "\n".join(r["QUERY PLAN"] for r in qconn.execute("EXPLAIN " + queue.CLAIM_SQL))

    assert "jobs_claim_idx" in plan
    assert "Sort" not in plan
