"""The reaper process (D13): every interval, requeue jobs whose lease expired.

Run with: python -m jobq.reaper

A dead worker's job is recovered within lease length + reaper interval: the
lease runs out at most `lease_seconds` after the last heartbeat, and the next
sweep starts at most `reaper_interval` after that.
"""

import logging
import os
import signal
import threading

import psycopg
from psycopg.rows import dict_row

from jobq import queue
from jobq.db import database_url
from jobq.settings import Settings

log = logging.getLogger("jobq.reaper")


class Reaper:
    def __init__(self, url: str, settings: Settings | None = None):
        self.url = url
        self.settings = settings or Settings.from_env()
        self._stop = threading.Event()

    def stop(self, *_args) -> None:
        self._stop.set()

    def run(self) -> None:
        with psycopg.connect(self.url, autocommit=True, row_factory=dict_row) as conn:
            while not self._stop.is_set():
                self.sweep(conn)
                self._stop.wait(self.settings.reaper_interval)

    @staticmethod
    def sweep(conn) -> list[dict]:
        reaped = queue.reap(conn)
        for job in reaped:
            if job["state"] == "dead":
                log.warning("job %s: lease expired on attempt %s, no attempts left -> dead",
                            job["id"], job["attempts"])
            else:
                log.info("job %s: lease expired on attempt %s -> requeued",
                         job["id"], job["attempts"])
        return reaped


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format=f"%(asctime)s reaper[{os.getpid()}] %(levelname)s %(message)s"
    )
    reaper = Reaper(database_url())
    signal.signal(signal.SIGTERM, reaper.stop)
    signal.signal(signal.SIGINT, reaper.stop)
    log.info("started; %s", reaper.settings)
    reaper.run()
    log.info("stopped")


if __name__ == "__main__":
    main()
