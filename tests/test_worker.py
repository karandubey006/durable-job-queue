import logging
import os
import signal
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

from psycopg.types.json import Jsonb

from jobq.reaper import Reaper
from jobq.settings import Settings
from jobq.worker import Worker
from tests.handlers import HANDLERS
from tests.helpers import unfinished, wait_until

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_worker_completes_fails_and_rejects_unknown_types(conn, db_url):
    ok, bad, unknown = (
        conn.execute(
            "INSERT INTO jobs (tenant_id, type) VALUES ('t1', %s) RETURNING id", (t,)
        ).fetchone()[0]
        for t in ("record", "boom", "no_such_type")
    )
    worker = Worker(db_url, HANDLERS)
    thread = threading.Thread(target=worker.run)
    thread.start()
    try:
        assert wait_until(lambda: unfinished(conn) == 0, timeout=10)
    finally:
        worker.stop()
        thread.join(timeout=5)

    states = dict(conn.execute("SELECT id, state FROM jobs").fetchall())
    assert states == {ok: "succeeded", bad: "dead", unknown: "dead"}
    assert conn.execute("SELECT job_id FROM test_executions").fetchall() == [(ok,)]


def test_worker_stops_promptly_when_idle(db_url, conn):
    worker = Worker(db_url, HANDLERS)
    thread = threading.Thread(target=worker.run)
    thread.start()
    time.sleep(2)  # long enough for the idle backoff to reach its 1 s cap
    worker.stop()
    thread.join(timeout=0.5)  # wait() wakes on stop(), not after the full sleep
    assert not thread.is_alive()


def test_eight_workers_drain_10000_jobs_exactly_once(conn, db_url, tmp_path):
    """Phase 1 'done when': 8 worker processes, 10,000 jobs, each run exactly once."""
    n_jobs, n_workers = 10_000, 8
    conn.execute(
        "INSERT INTO jobs (tenant_id, type) SELECT 't1', 'record' FROM generate_series(1, %s)",
        (n_jobs,),
    )

    logs = [open(tmp_path / f"worker{i}.log", "w") for i in range(n_workers)]
    workers = [
        subprocess.Popen(
            [sys.executable, "-m", "jobq.worker", "--handlers", "tests.handlers"],
            cwd=REPO_ROOT,
            env={**os.environ, "DATABASE_URL": db_url},
            stdout=log,
            stderr=log,
        )
        for log in logs
    ]
    try:
        drained = wait_until(lambda: unfinished(conn) == 0, timeout=120, interval=0.25)
    finally:
        for w in workers:
            w.send_signal(signal.SIGTERM)
        exit_codes = [w.wait(timeout=10) for w in workers]
        for log in logs:
            log.close()

    assert drained, "workers did not drain the queue in time"
    assert exit_codes == [0] * n_workers

    total, distinct_jobs, pids = conn.execute(
        "SELECT count(*), count(DISTINCT job_id), count(DISTINCT pid) FROM test_executions"
    ).fetchone()
    assert total == n_jobs  # no job ran twice...
    assert distinct_jobs == n_jobs  # ...and none was skipped
    assert pids > 1  # the work really was spread across processes

    assert conn.execute(
        "SELECT count(*) FILTER (WHERE state = 'succeeded'), max(attempts) FROM jobs"
    ).fetchone() == (n_jobs, 1)


# Short timings so lease expiry happens within a test.
FAST = Settings(lease_seconds=1.0, heartbeat_seconds=0.3, reaper_interval=0.1)


@contextmanager
def running(*runners):
    """Run Worker/Reaper objects on threads for the duration of the block."""
    threads = [threading.Thread(target=r.run) for r in runners]
    for t in threads:
        t.start()
    try:
        yield
    finally:
        for r in runners:
            r.stop()
        for t in threads:
            t.join(timeout=10)


def add_job(conn, type_, payload):
    return conn.execute(
        "INSERT INTO jobs (tenant_id, type, payload) VALUES ('t1', %s, %s) RETURNING id",
        (type_, Jsonb(payload)),
    ).fetchone()[0]


def test_heartbeats_keep_a_job_longer_than_its_lease_alive(conn, db_url):
    # 3 s of work on a 1 s lease, with the reaper sweeping every 0.1 s.
    job_id = add_job(conn, "sleep", {"seconds": 3})

    with running(Worker(db_url, HANDLERS, FAST), Reaper(db_url, FAST)):
        assert wait_until(lambda: unfinished(conn) == 0, timeout=10)

    assert conn.execute("SELECT state, attempts FROM jobs WHERE id = %s", (job_id,)).fetchone() == (
        "succeeded", 1
    )
    assert conn.execute("SELECT count(*) FROM test_executions").fetchone()[0] == 1


def test_handler_stops_when_its_lease_is_taken(conn, db_url, caplog):
    caplog.set_level(logging.INFO)
    job_id = add_job(conn, "sleep_cooperative", {"seconds": 30})
    worker = Worker(db_url, HANDLERS, FAST)

    with running(worker):
        assert wait_until(lambda: conn.execute("SELECT count(*) FROM test_executions").fetchone()[0], 5)
        # Someone else takes the job: as if the lease expired and it was re-claimed.
        conn.execute(
            "UPDATE jobs SET lease_token = nextval('lease_token_seq') WHERE id = %s", (job_id,)
        )
        # The next heartbeat is rejected, the handler's lease.check() raises,
        # and the worker abandons the job well before the 30 s of work.
        assert wait_until(lambda: "lease lost, abandoned" in caplog.text, timeout=3)

    assert conn.execute("SELECT state FROM jobs WHERE id = %s", (job_id,)).fetchone()[0] == "running"
    assert "ack rejected" not in caplog.text  # it didn't even try to ack


# --- Phase 3: retries, backoff and the DLQ ----------------------------------

RETRY = Settings(backoff_base=0.2, backoff_cap=10.0)


def attempts_of(conn, job_id):
    return conn.execute(
        "SELECT attempt, outcome, started_at, finished_at FROM job_attempts"
        " WHERE job_id = %s ORDER BY lease_token",
        (job_id,),
    ).fetchall()


def run_until_done(conn, db_url, settings, timeout=20):
    with running(Worker(db_url, HANDLERS, settings)):
        assert wait_until(lambda: unfinished(conn) == 0, timeout=timeout)


def test_flaky_job_succeeds_on_attempt_3(conn, db_url):
    job_id = add_job(conn, "flaky", {"succeed_on": 3})

    run_until_done(conn, db_url, RETRY)

    assert conn.execute("SELECT state, attempts FROM jobs WHERE id = %s", (job_id,)).fetchone() == (
        "succeeded", 3
    )
    assert [(n, o) for n, o, _, _ in attempts_of(conn, job_id)] == [
        (1, "failed"), (2, "failed"), (3, "succeeded")
    ]


def test_poison_job_lands_in_the_dlq_after_max_attempts(conn, db_url):
    job_id = conn.execute(
        "INSERT INTO jobs (tenant_id, type, max_attempts) VALUES ('t1', 'poison', 4) RETURNING id"
    ).fetchone()[0]

    run_until_done(conn, db_url, RETRY)

    assert conn.execute(
        "SELECT state, attempts, last_error FROM jobs WHERE id = %s", (job_id,)
    ).fetchone() == ("dead", 4, "TimeoutError('payments API timed out')")
    assert [o for _, o, _, _ in attempts_of(conn, job_id)] == ["failed"] * 4


def test_permanent_error_goes_to_the_dlq_on_the_first_attempt(conn, db_url):
    job_id = add_job(conn, "boom", {})  # RuntimeError: not a network/DB error

    run_until_done(conn, db_url, RETRY)

    assert conn.execute("SELECT state, attempts FROM jobs WHERE id = %s", (job_id,)).fetchone() == (
        "dead", 1
    )


def test_backoff_delays_stay_inside_their_bounds(conn, db_url, caplog):
    caplog.set_level(logging.INFO)
    job_id = conn.execute(
        "INSERT INTO jobs (tenant_id, type, max_attempts) VALUES ('t1', 'poison', 5) RETURNING id"
    ).fetchone()[0]

    run_until_done(conn, db_url, RETRY)

    # The delay each failure chose: inside [0, base * 2^(n-1)].
    chosen = [
        float(r.getMessage().rsplit("retry in ", 1)[1].rstrip("s"))
        for r in caplog.records
        if "retry in" in r.getMessage()
    ]
    windows = [RETRY.backoff_base * 2 ** (n - 1) for n in range(1, 5)]  # 0.2, 0.4, 0.8, 1.6
    assert len(chosen) == 4
    assert all(0 <= d <= w for d, w in zip(chosen, windows))

    # And the next attempt really waited about that long: never earlier than
    # the delay, and no later than the delay plus the worker's idle poll.
    history = attempts_of(conn, job_id)
    for delay, (prev, nxt) in zip(chosen, zip(history, history[1:])):
        waited = (nxt[2] - prev[3]).total_seconds()
        assert delay - 0.01 <= waited <= delay + 1.2, (delay, waited)
