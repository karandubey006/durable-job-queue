"""Built-in job handlers. A handler is called as handler(job, conn, lease).

`job` is the claimed row as a dict. `conn` is the worker's autocommit
connection. `lease` is a jobq.worker.Lease: long handlers should call
lease.check() at safe points, which raises once the lease is lost.
Raising an exception fails the job; returning completes it.
"""

import time


def noop(job, conn, lease):
    pass


def sleep(job, conn, lease):
    """Demo: work for payload["seconds"] (default 20), checking the lease each second."""
    for _ in range(int(job["payload"].get("seconds", 20))):
        lease.check()
        time.sleep(1)


HANDLERS = {"noop": noop, "sleep": sleep}
