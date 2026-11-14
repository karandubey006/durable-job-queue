"""Tests run against a real Postgres (the Compose `db` service), in a separate
`jobq_test` database that is dropped and re-created once per test session."""

import os

import psycopg
import pytest
from fastapi.testclient import TestClient

from jobq.migrate import migrate

ADMIN_URL = os.environ.get("JOBQ_TEST_ADMIN_URL", "postgresql://jobq:jobq@localhost:5432/postgres")
TEST_DB = "jobq_test"
TEST_URL = ADMIN_URL.rsplit("/", 1)[0] + f"/{TEST_DB}"


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
