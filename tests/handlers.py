"""Handlers used by the tests. Workers load them with --handlers tests.handlers."""

import os
import time


def record(job, conn, lease):
    """Record that this attempt of the job ran, and in which process."""
    conn.execute(
        "INSERT INTO test_executions (job_id, attempt, pid) VALUES (%s, %s, %s)",
        (job["id"], job["attempts"], os.getpid()),
    )


def boom(job, conn, lease):
    raise RuntimeError("boom")


def _seconds(job) -> float:
    """payload["seconds"]: a number, or a list giving the duration per attempt."""
    seconds = job["payload"]["seconds"]
    if isinstance(seconds, list):
        return seconds[min(job["attempts"], len(seconds)) - 1]
    return seconds


def sleep(job, conn, lease):
    """Record the start, then sleep without ever checking the lease: a handler
    that doesn't cooperate, so only the fence can stop its ack."""
    record(job, conn, lease)
    time.sleep(_seconds(job))


def sleep_cooperative(job, conn, lease):
    """Record the start, then sleep in small steps, checking the lease each time."""
    record(job, conn, lease)
    deadline = time.monotonic() + _seconds(job)
    while time.monotonic() < deadline:
        lease.check()
        time.sleep(0.05)


HANDLERS = {
    "record": record,
    "boom": boom,
    "sleep": sleep,
    "sleep_cooperative": sleep_cooperative,
}
