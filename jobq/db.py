import os

DEFAULT_URL = "postgresql://jobq:jobq@localhost:5432/jobq"


def database_url() -> str:
    return os.environ.get("DATABASE_URL", DEFAULT_URL)
