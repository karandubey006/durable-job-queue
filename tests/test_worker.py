import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from jobq.worker import Worker
from tests.handlers import HANDLERS

REPO_ROOT = Path(__file__).resolve().parent.parent


def wait_until(predicate, timeout, interval=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def unfinished(conn):
    return conn.execute(
        "SELECT count(*) FROM jobs WHERE state IN ('queued', 'running')"
    ).fetchone()[0]


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
