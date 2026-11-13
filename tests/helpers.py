import time


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
