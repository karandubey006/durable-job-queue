"""A worker process: claim a job, run its handler, complete or fail it, repeat.

Run with: python -m jobq.worker [--handlers some.module]
The handlers module must define HANDLERS: dict[str, callable].
"""

import argparse
import importlib
import logging
import os
import signal
import threading

import psycopg
from psycopg.rows import dict_row

from jobq import queue
from jobq.db import database_url

log = logging.getLogger("jobq.worker")

# When the queue is empty: sleep IDLE_MIN, doubling up to IDLE_MAX.
# Reset to IDLE_MIN as soon as a claim succeeds.
IDLE_MIN = 0.05
IDLE_MAX = 1.0


class Worker:
    def __init__(self, url: str, handlers: dict):
        self.url = url
        self.handlers = handlers
        self._stop = threading.Event()

    def stop(self, *_args) -> None:
        """Finish the current job, then exit. Safe to call from a signal handler."""
        self._stop.set()

    def run(self) -> None:
        with psycopg.connect(self.url, autocommit=True, row_factory=dict_row) as conn:
            idle = IDLE_MIN
            while not self._stop.is_set():
                job = queue.claim(conn)
                if job is None:
                    self._stop.wait(idle)  # returns early if stop() is called
                    idle = min(idle * 2, IDLE_MAX)
                    continue
                idle = IDLE_MIN
                self._execute(conn, job)

    def _execute(self, conn, job: dict) -> None:
        handler = self.handlers.get(job["type"])
        if handler is None:
            log.error("job %s: no handler for type %r", job["id"], job["type"])
            queue.fail(conn, job["id"])
            return
        try:
            handler(job, conn)
        except Exception:
            log.exception("job %s failed", job["id"])
            if not queue.fail(conn, job["id"]):
                log.warning("job %s: fail ignored, job is no longer running", job["id"])
        else:
            if not queue.complete(conn, job["id"]):
                log.warning("job %s: complete ignored, job is no longer running", job["id"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a jobq worker.")
    parser.add_argument("--handlers", default="jobq.handlers", help="module defining HANDLERS")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format=f"%(asctime)s worker[{os.getpid()}] %(levelname)s %(message)s"
    )
    handlers = importlib.import_module(args.handlers).HANDLERS
    worker = Worker(database_url(), handlers)
    # SIGTERM / Ctrl-C: stop after the current job. (SIGKILL can't be caught;
    # that's the crash case Phase 2 handles.)
    signal.signal(signal.SIGTERM, worker.stop)
    signal.signal(signal.SIGINT, worker.stop)
    log.info("started with handlers: %s", ", ".join(sorted(handlers)))
    worker.run()
    log.info("stopped")


if __name__ == "__main__":
    main()
