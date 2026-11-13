"""HTTP API: submit a job and fetch it."""

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Any

from fastapi import FastAPI, Header, HTTPException, Request
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from pydantic import AwareDatetime, BaseModel, Field

from jobq.db import database_url

JOB_COLUMNS = "id, tenant_id, type, payload, state, attempts, max_attempts, run_at, created_at"


class JobIn(BaseModel):
    type: str = Field(min_length=1, max_length=200)
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = Field(default=5, ge=1, le=100)
    # Must carry a timezone; None means "run as soon as possible" (the DB's now()).
    run_at: AwareDatetime | None = None


class Job(BaseModel):
    id: int
    tenant_id: str
    type: str
    payload: dict[str, Any]
    state: str
    attempts: int
    max_attempts: int
    run_at: datetime
    created_at: datetime


TenantId = Annotated[str, Header(alias="X-Tenant-Id", min_length=1, max_length=200)]


@asynccontextmanager
async def lifespan(app: FastAPI):
    with ConnectionPool(
        database_url(), min_size=1, max_size=10, kwargs={"row_factory": dict_row}
    ) as pool:
        app.state.pool = pool
        yield


app = FastAPI(title="jobq", lifespan=lifespan)


@app.post("/jobs", status_code=201, response_model=Job)
def submit_job(job: JobIn, tenant_id: TenantId, request: Request):
    with request.app.state.pool.connection() as conn:
        row = conn.execute(
            f"""
            INSERT INTO jobs (tenant_id, type, payload, max_attempts, run_at)
            VALUES (%(tenant_id)s, %(type)s, %(payload)s, %(max_attempts)s,
                    COALESCE(%(run_at)s, now()))
            RETURNING {JOB_COLUMNS}
            """,
            {
                "tenant_id": tenant_id,
                "type": job.type,
                "payload": Jsonb(job.payload),
                "max_attempts": job.max_attempts,
                "run_at": job.run_at,
            },
        ).fetchone()
    # The pool commits when the `with` block exits cleanly, so by the time we
    # return 201 the job is durable. An exception above rolls it back instead.
    return row


@app.get("/jobs/{job_id}", response_model=Job)
def get_job(job_id: int, tenant_id: TenantId, request: Request):
    with request.app.state.pool.connection() as conn:
        row = conn.execute(
            f"SELECT {JOB_COLUMNS} FROM jobs WHERE id = %s AND tenant_id = %s",
            (job_id, tenant_id),
        ).fetchone()
    if row is None:
        # Another tenant's job is indistinguishable from a missing one.
        raise HTTPException(status_code=404, detail="job not found")
    return row
