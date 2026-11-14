"""Crash recovery with real worker processes: SIGKILL (dead) and SIGSTOP (zombie)."""

import signal

from psycopg.types.json import Jsonb

from tests.helpers import TIMING, wait_until

def add_job(conn, type_, payload):
    return conn.execute(
        "INSERT INTO jobs (tenant_id, type, payload) VALUES ('t1', %s, %s) RETURNING id",
        (type_, Jsonb(payload)),
    ).fetchone()[0]


def execution(conn, job_id, attempt):
    """(pid, started_at) of the given attempt, or None if it hasn't started."""
    return conn.execute(
        "SELECT pid, executed_at FROM test_executions WHERE job_id = %s AND attempt = %s",
        (job_id, attempt),
    ).fetchone()


def job_row(conn, job_id):
    return conn.execute(
        "SELECT state, attempts, lease_token FROM jobs WHERE id = %s", (job_id,)
    ).fetchone()


def test_kill_9_mid_job_is_recovered_within_lease_plus_reaper_interval(
    conn, spawn_worker, reaper
):
    """Phase 2 'done when': SIGKILL a worker mid-job; another finishes the job."""
    job_id = add_job(conn, "sleep", {"seconds": [60, 0.1]})  # attempt 1 never finishes
    a = spawn_worker("a")
    assert wait_until(lambda: execution(conn, job_id, 1), timeout=10)
    b = spawn_worker("b")  # idle and polling, ready to take over

    a.kill()  # SIGKILL: no cleanup, no ack, just silence
    a.wait()
    killed_at = conn.execute("SELECT clock_timestamp()").fetchone()[0]

    assert wait_until(lambda: job_row(conn, job_id)[0] == "succeeded", timeout=15)
    assert job_row(conn, job_id)[:2] == ("succeeded", 2)
    pid, restarted_at = execution(conn, job_id, 2)
    assert pid == b.pid
    # Lease expiry + reaper sweep + up to 1 s for B's idle back-off to poll.
    bound = TIMING.lease_seconds + TIMING.reaper_interval + 1.0
    recovery = (restarted_at - killed_at).total_seconds()
    print(f"recovered in {recovery:.2f}s (bound {bound}s)")
    assert recovery <= bound
    # The history tells the story: the killed attempt was closed by the reaper.
    assert conn.execute(
        "SELECT attempt, outcome FROM job_attempts WHERE job_id = %s ORDER BY lease_token",
        (job_id,),
    ).fetchall() == [(1, "lease_expired"), (2, "succeeded")]


def test_zombie_workers_late_ack_is_rejected_while_new_owner_runs(
    conn, spawn_worker, reaper
):
    """Phase 2 'done when': a stale worker's late ack is rejected.

    SIGSTOP freezes worker A completely, heartbeat thread included: a long GC
    pause or a VM stall. Its lease expires, B re-claims the job, and A is woken
    while B is still running. A's handler never checks its lease, so A sends
    complete with its old token, and only the fence stands in the way.
    """
    job_id = add_job(conn, "sleep", {"seconds": [2, 8]})
    a = spawn_worker("a")
    assert wait_until(lambda: execution(conn, job_id, 1), timeout=10)
    b = spawn_worker("b")

    a.send_signal(signal.SIGSTOP)
    assert wait_until(lambda: execution(conn, job_id, 2), timeout=10)  # B owns it now
    b_token = job_row(conn, job_id)[2]

    a.send_signal(signal.SIGCONT)  # A's sleep is long over; it acks at once
    a_log = lambda: a.log_path.read_text()  # noqa: E731
    assert wait_until(lambda: f"job {job_id} attempt 1: succeeded" in a_log(), timeout=5)

    assert "ack rejected" in a_log()
    # B still owns the running job: A's ack changed nothing.
    assert job_row(conn, job_id) == ("running", 2, b_token)

    assert wait_until(lambda: job_row(conn, job_id)[0] == "succeeded", timeout=15)
    assert job_row(conn, job_id) == ("succeeded", 2, b_token)
