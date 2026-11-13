"""Handlers used by the tests. Workers load them with --handlers tests.handlers."""

import os


def record(job, conn):
    """Record that this job ran, and in which process."""
    conn.execute(
        "INSERT INTO test_executions (job_id, pid) VALUES (%s, %s)", (job["id"], os.getpid())
    )


def boom(job, conn):
    raise RuntimeError("boom")


HANDLERS = {"record": record, "boom": boom}
