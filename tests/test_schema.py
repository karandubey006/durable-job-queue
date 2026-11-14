import psycopg
import pytest

from jobq.migrate import migrate


def test_migrate_is_idempotent(db_url, conn):
    assert migrate(db_url) == []  # everything already applied by the fixture
    versions = [r[0] for r in conn.execute("SELECT version FROM schema_migrations ORDER BY version")]
    assert versions == ["001_jobs.sql", "002_claim_index.sql", "003_leases.sql", "004_attempts.sql"]


def test_insert_defaults(conn):
    row = conn.execute(
        "INSERT INTO jobs (tenant_id, type) VALUES ('t1', 'noop')"
        " RETURNING state, attempts, max_attempts, payload, run_at <= now()"
    ).fetchone()
    assert row == ("queued", 0, 5, {}, True)


@pytest.mark.parametrize(
    "column, value",
    [("state", "failed"), ("attempts", -1), ("max_attempts", 0)],
)
def test_check_constraints_reject_bad_values(conn, column, value):
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            f"INSERT INTO jobs (tenant_id, type, {column}) VALUES ('t1', 'noop', %s)",
            (value,),
        )


@pytest.mark.parametrize("column", ["tenant_id", "type"])
def test_required_columns(conn, column):
    other = "type" if column == "tenant_id" else "tenant_id"
    with pytest.raises(psycopg.errors.NotNullViolation):
        conn.execute(f"INSERT INTO jobs ({other}) VALUES ('x')")


def test_running_job_must_hold_a_lease(conn):
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute("INSERT INTO jobs (tenant_id, type, state) VALUES ('t1', 'noop', 'running')")


def test_attempt_numbers_are_unique_per_job(conn):
    job_id = conn.execute("INSERT INTO jobs (tenant_id, type) VALUES ('t1', 'noop') RETURNING id").fetchone()[0]
    insert = "INSERT INTO job_attempts (lease_token, job_id, attempt, worker) VALUES (%s, %s, 1, 'w')"
    conn.execute(insert, (1, job_id))
    with pytest.raises(psycopg.errors.UniqueViolation):
        conn.execute(insert, (2, job_id))
