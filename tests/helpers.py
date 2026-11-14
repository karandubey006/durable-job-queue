import time

from jobq.settings import Settings

# Short timings for tests that run real worker processes and the reaper.
TIMING = Settings(lease_seconds=2.0, heartbeat_seconds=0.5, reaper_interval=0.5)


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
