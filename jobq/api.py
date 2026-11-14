"""HTTP API: submit and fetch jobs, list the DLQ, replay dead jobs."""

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Any

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pydantic import AwareDatetime, BaseModel, Field

from jobq import queue
from jobq.db import database_url

JOB_COLUMNS = queue.JOB_COLUMNS


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
    last_error: str | None
    idempotency_key: str | None


class ReplayIn(BaseModel):
    # Attempts to add on top of those already used. None: the job's current max_attempts.
    extra_attempts: int | None = Field(default=None, ge=1, le=100)


TenantId = Annotated[str, Header(alias="X-Tenant-Id", min_length=1, max_length=200)]
IdempotencyKey = Annotated[
    str | None, Header(alias="Idempotency-Key", min_length=1, max_length=255)
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    with ConnectionPool(
        database_url(), min_size=1, max_size=10, kwargs={"row_factory": dict_row}
    ) as pool:
        app.state.pool = pool
        yield


app = FastAPI(title="jobq", lifespan=lifespan)


@app.post("/jobs", status_code=201, response_model=Job)
def submit_job(
    job: JobIn,
    tenant_id: TenantId,
    request: Request,
    response: Response,
    idempotency_key: IdempotencyKey = None,
):
    """Enqueue a job. With an Idempotency-Key, a retry of the same request
    returns the original job (200, Idempotent-Replayed: true) instead of
    creating a second one; reusing the key for a different request is a 422."""
    try:
        with request.app.state.pool.connection() as conn:
            row, created = queue.submit(
                conn, tenant_id, job.type, job.payload, job.max_attempts, job.run_at,
                idempotency_key,
            )
    except queue.KeyReused as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    # The pool commits when the `with` block exits cleanly, so by the time we
    # return, the job is durable. An exception above rolls it back instead.
    if not created:
        response.status_code = 200
        response.headers["Idempotent-Replayed"] = "true"
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


@app.get("/dlq", response_model=list[Job])
def list_dead_jobs(
    tenant_id: TenantId,
    request: Request,
    after_id: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
):
    """The tenant's dead jobs, oldest first. Page with after_id = last id seen."""
    with request.app.state.pool.connection() as conn:
        return conn.execute(
            f"""
            SELECT {JOB_COLUMNS} FROM jobs
            WHERE  tenant_id = %s AND state = 'dead' AND id > %s
            ORDER  BY id
            LIMIT  %s
            """,
            (tenant_id, after_id, limit),
        ).fetchall()


@app.post("/jobs/{job_id}/replay", response_model=Job)
def replay_job(
    job_id: int, tenant_id: TenantId, request: Request, body: ReplayIn | None = None
):
    """Send a dead job back to the queue with more attempts (D22)."""
    extra = body.extra_attempts if body else None
    with request.app.state.pool.connection() as conn:
        replayed = queue.replay(conn, job_id, tenant_id, extra)
        row = conn.execute(
            f"SELECT {JOB_COLUMNS} FROM jobs WHERE id = %s AND tenant_id = %s",
            (job_id, tenant_id),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="job not found")
    if not replayed:
        raise HTTPException(
            status_code=409, detail=f"job is {row['state']}, only dead jobs can be replayed"
        )
    return row
