"""Apply migrations/NNN_name.sql files in order, each exactly once.

Run with: python -m jobq.migrate
"""

from pathlib import Path

import psycopg

from jobq.db import database_url

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"


def migrate(url: str, migrations_dir: Path = MIGRATIONS_DIR) -> list[str]:
    """Apply every migration not yet recorded in schema_migrations.

    Each file runs in its own transaction together with the INSERT that records
    it, so a file is either fully applied and recorded, or neither.
    Returns the names of the files applied by this call.
    """
    applied_now = []
    # autocommit=True so that conn.transaction() opens a real BEGIN/COMMIT
    # instead of a savepoint inside an implicit transaction.
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version    text        PRIMARY KEY,
                applied_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        done = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}

        for path in sorted(migrations_dir.glob("*.sql")):
            if path.name in done:
                continue
            with conn.transaction():
                conn.execute(path.read_text())
                conn.execute(
                    "INSERT INTO schema_migrations (version) VALUES (%s)", (path.name,)
                )
            applied_now.append(path.name)
    return applied_now


if __name__ == "__main__":
    applied = migrate(database_url())
    print(f"applied: {', '.join(applied)}" if applied else "no new migrations")
