"""Tests run against a real Postgres (the Compose `db` service), in a separate
`jobq_test` database that is dropped and re-created once per test session."""

import os
import socket
import subprocess
import sys
import threading
from pathlib import Path

import psycopg
import pytest
import uvicorn
from fastapi.testclient import TestClient

from fake_payments.app import ensure_database
from jobq.migrate import migrate
from jobq.reaper import Reaper
from tests.helpers import TIMING, wait_until

REPO_ROOT = Path(__file__).resolve().parent.parent

ADMIN_URL = os.environ.get("JOBQ_TEST_ADMIN_URL", "postgresql://jobq:jobq@localhost:5432/postgres")
TEST_DB = "jobq_test"
TEST_URL = ADMIN_URL.rsplit("/", 1)[0] + f"/{TEST_DB}"
PAYMENTS_TEST_URL = ADMIN_URL.rsplit("/", 1)[0] + "/payments_test"


@pytest.fixture(scope="session")
def db_url():
    with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
        conn.execute(f"CREATE DATABASE {TEST_DB}")
    migrate(TEST_URL)
    with psycopg.connect(TEST_URL, autocommit=True) as conn:
        # Test-only: tests/handlers.py records every handler execution here, so
        # tests can check each job ran exactly once. (Phase 3's job_attempts
        # table replaces this.)
        conn.execute(
            """
            CREATE TABLE test_executions (
                job_id      bigint      NOT NULL,
                attempt     integer     NOT NULL,
                pid         integer     NOT NULL,
                executed_at timestamptz NOT NULL DEFAULT clock_timestamp()
            )
            """
        )
    return TEST_URL


@pytest.fixture
def conn(db_url):
    """Fresh, empty tables for every test."""
    with psycopg.connect(db_url, autocommit=True) as conn:
        conn.execute("TRUNCATE jobs, job_attempts, test_executions RESTART IDENTITY")
        yield conn


@pytest.fixture
def client(conn, db_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", db_url)
    from jobq.api import app

    with TestClient(app) as client:  # `with` runs the lifespan, opening the pool
        yield client


@pytest.fixture
def spawn_worker(db_url, tmp_path):
    """Start worker processes with TIMING; kill any still running at the end.
    spawn(name, **env) adds extra environment variables."""
    procs = []

    def spawn(name, **env):
        log_path = tmp_path / f"{name}.log"
        proc = subprocess.Popen(
            [sys.executable, "-m", "jobq.worker", "--handlers", "tests.handlers"],
            cwd=REPO_ROOT,
            env={
                **os.environ,
                "DATABASE_URL": db_url,
                "JOBQ_LEASE_SECONDS": str(TIMING.lease_seconds),
                "JOBQ_HEARTBEAT_SECONDS": str(TIMING.heartbeat_seconds),
                **env,
            },
            stdout=open(log_path, "w"),
            stderr=subprocess.STDOUT,
        )
        proc.log_path = log_path
        procs.append(proc)
        assert wait_until(lambda: "started" in log_path.read_text(), timeout=10)
        return proc

    yield spawn
    for proc in procs:
        proc.kill()
        proc.wait()


@pytest.fixture
def reaper(db_url):
    r = Reaper(db_url, TIMING)
    thread = threading.Thread(target=r.run)
    thread.start()
    yield r
    r.stop()
    thread.join(timeout=5)


# --- the fake payments service ------------------------------------------------


@pytest.fixture(scope="session")
def payments_url():
    """A fake payments server on a free port, backed by a fresh payments_test DB."""
    with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
        conn.execute("DROP DATABASE IF EXISTS payments_test WITH (FORCE)")
    ensure_database(PAYMENTS_TEST_URL)

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    os.environ["PAYMENTS_DATABASE_URL"] = PAYMENTS_TEST_URL  # read by the app's lifespan
    from fake_payments.app import app

    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    assert wait_until(lambda: server.started, timeout=10)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture
def payments(payments_url):
    """Empty payments tables for every test, and a connection to inspect them.

    Job ids restart at 1 in every test, so without this, job-1's key from an
    earlier test would make this test's job-1 look like a repeat."""
    with psycopg.connect(PAYMENTS_TEST_URL, autocommit=True) as conn:
        conn.execute("TRUNCATE charges, idempotency_keys RESTART IDENTITY")
        yield conn
