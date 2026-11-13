import psycopg
import pytest

from jobq.migrate import migrate


def test_migrate_is_idempotent(db_url, conn):
    assert migrate(db_url) == []  # everything already applied by the fixture
    versions = [r[0] for r in conn.execute("SELECT version FROM schema_migrations ORDER BY version")]
    assert versions == ["001_jobs.sql", "002_claim_index.sql"]


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
