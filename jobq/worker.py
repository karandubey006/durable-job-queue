"""A worker process: claim a job, run its handler while heartbeating, ack, repeat.

Run with: python -m jobq.worker [--handlers some.module]
The handlers module must define HANDLERS: dict[str, callable]. A handler is
called as handler(job, conn, lease); see jobq/handlers.py.
"""

import argparse
import importlib
import logging
import os
import signal
import socket
import threading

import psycopg
from psycopg.rows import dict_row

from jobq import queue
from jobq.backoff import full_jitter
from jobq.db import database_url
from jobq.errors import PermanentError, is_retryable
from jobq.settings import Settings

log = logging.getLogger("jobq.worker")

# When the queue is empty: sleep IDLE_MIN, doubling up to IDLE_MAX.
# Reset to IDLE_MIN as soon as a claim succeeds.
IDLE_MIN = 0.05
IDLE_MAX = 1.0


class LeaseLost(Exception):
    """Raised by Lease.check() once this worker no longer owns the job."""


class Lease:
    """A handler's view of its claim on a job (D16: cooperative cancellation).

    Long handlers should call lease.check() at safe points. It raises
    LeaseLost once a heartbeat has been rejected, meaning another worker may
    already own the job.
    """

    def __init__(self, job_id: int, token: int):
        self.job_id = job_id
        self.token = token
        self._lost = threading.Event()

    @property
    def lost(self) -> bool:
        return self._lost.is_set()

    def check(self) -> None:
        if self.lost:
            raise LeaseLost(f"job {self.job_id}: lease {self.token} lost")


class Heartbeat:
    """While active, extends a lease every `interval` seconds on its own thread.

    Uses its own connection: the handler is using the main one, and a slow
    query there must not delay heartbeats. Stops at the first rejected
    heartbeat, marking the lease lost.
    """

    def __init__(self, conn, lease: Lease, settings: Settings):
        self.conn = conn
        self.lease = lease
        self.settings = settings
        self._done = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_exc):
        self._done.set()
        self._thread.join()

    def _run(self) -> None:
        # wait() returns True when __exit__ sets _done, ending the loop.
        while not self._done.wait(self.settings.heartbeat_seconds):
            try:
                ok = queue.heartbeat(
                    self.conn, self.lease.job_id, self.lease.token, self.settings.lease_seconds
                )
            except psycopg.Error:
                # Can't tell whether we still own the job. Keep trying: if the
                # lease is gone by the time the DB answers, we'll find out then.
                log.exception("job %s: heartbeat errored, retrying", self.lease.job_id)
                continue
            if not ok:
                log.warning("job %s: heartbeat rejected, lease lost", self.lease.job_id)
                self.lease._lost.set()
                return


class Worker:
    def __init__(
        self, url: str, handlers: dict, settings: Settings | None = None, name: str | None = None
    ):
        self.url = url
        self.handlers = handlers
        self.settings = settings or Settings.from_env()
        # Recorded on every attempt in job_attempts.worker.
        self.name = name or f"{socket.gethostname()}:{os.getpid()}"
        self._stop = threading.Event()

    def stop(self, *_args) -> None:
        """Finish the current job, then exit. Safe to call from a signal handler."""
        self._stop.set()

    def run(self) -> None:
        connect = lambda: psycopg.connect(self.url, autocommit=True, row_factory=dict_row)  # noqa: E731
        with connect() as conn, connect() as heartbeat_conn:
            idle = IDLE_MIN
            while not self._stop.is_set():
                job = queue.claim(conn, self.settings.lease_seconds, self.name)
                if job is None:
                    self._stop.wait(idle)  # returns early if stop() is called
                    idle = min(idle * 2, IDLE_MAX)
                    continue
                idle = IDLE_MIN
                self._execute(conn, heartbeat_conn, job)

    def _execute(self, conn, heartbeat_conn, job: dict) -> None:
        handler = self.handlers.get(job["type"])
        if handler is None:
            log.error("job %s: no handler for type %r", job["id"], job["type"])
            self._fail(conn, job, PermanentError(f"no handler for type {job['type']!r}"))
            return

        lease = Lease(job["id"], job["lease_token"])
        try:
            with Heartbeat(heartbeat_conn, lease, self.settings):
                handler(job, conn, lease)
        except LeaseLost:
            # Another worker may own the job now. Don't ack: the fence would
            # reject it anyway.
            log.warning("job %s attempt %s: lease lost, abandoned", job["id"], job["attempts"])
        except Exception as exc:
            log.exception("job %s attempt %s raised", job["id"], job["attempts"])
            self._fail(conn, job, exc)
        else:
            accepted = queue.complete(conn, job["id"], job["lease_token"])
            self._ack(job, accepted, "succeeded")

    def _fail(self, conn, job: dict, exc: Exception) -> None:
        """Classify the error (D17), pick a backoff delay (D18) and record the
        failure. The SQL decides between retry and DLQ, since only it knows
        the attempt count at the moment of the write."""
        retryable = is_retryable(exc)
        delay = full_jitter(job["attempts"], self.settings.backoff_base, self.settings.backoff_cap)
        state = queue.fail(conn, job["id"], job["lease_token"], repr(exc), retryable, delay)
        kind = "retryable" if retryable else "permanent"
        if state == "queued":
            outcome = f"failed ({kind}) -> queued, retry in {delay:.2f}s"
        elif state == "dead":
            outcome = f"failed ({kind}) -> dead"
        else:
            outcome = "failed"
        self._ack(job, state is not None, outcome)

    @staticmethod
    def _ack(job: dict, accepted: bool, outcome: str) -> None:
        # Always try the ack, even if a heartbeat was rejected: the fence in
        # the database decides, not the worker's view of its lease.
        if accepted:
            log.info("job %s attempt %s: %s", job["id"], job["attempts"], outcome)
        else:
            log.warning(
                "job %s attempt %s: %s ack rejected, lease %s is no longer current",
                job["id"], job["attempts"], outcome, job["lease_token"],
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a jobq worker.")
    parser.add_argument("--handlers", default="jobq.handlers", help="module defining HANDLERS")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format=f"%(asctime)s worker[{os.getpid()}] %(levelname)s %(message)s"
    )
    handlers = importlib.import_module(args.handlers).HANDLERS
    worker = Worker(database_url(), handlers)
    # SIGTERM / Ctrl-C: stop after the current job. SIGKILL can't be caught:
    # the job's lease expires and the reaper requeues it.
    signal.signal(signal.SIGTERM, worker.stop)
    signal.signal(signal.SIGINT, worker.stop)
    log.info("started with handlers: %s; %s", ", ".join(sorted(handlers)), worker.settings)
    worker.run()
    log.info("stopped")


if __name__ == "__main__":
    main()
