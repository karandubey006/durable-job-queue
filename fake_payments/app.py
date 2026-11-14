"""POST /charges, deduplicated by the Idempotency-Key header.

Run with: python -m fake_payments   (PAYMENTS_DATABASE_URL, PAYMENTS_PORT)
"""

import hashlib
import json
import os
from contextlib import asynccontextmanager
from typing import Annotated

import psycopg
from fastapi import FastAPI, Header, HTTPException, Response
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

DEFAULT_URL = "postgresql://jobq:jobq@localhost:5432/payments"

SCHEMA = """
CREATE TABLE IF NOT EXISTS charges (
    id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer        text        NOT NULL,
    amount          integer     NOT NULL CHECK (amount > 0),
    currency        text        NOT NULL,
    idempotency_key text,       -- NULL when the caller sent none
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- One row per key ever seen. Inserted *before* the charge, in the same
-- transaction, so the key's primary key decides which request charges.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key          text        PRIMARY KEY,
    request_hash text        NOT NULL,  -- to reject a reused key with other params
    charge_id    bigint      REFERENCES charges (id),
    created_at   timestamptz NOT NULL DEFAULT now()
);
"""


def database_url() -> str:
    return os.environ.get("PAYMENTS_DATABASE_URL", DEFAULT_URL)


def ensure_database(url: str) -> None:
    """Create the payments database (if missing) and its tables."""
    base, name = url.rsplit("/", 1)
    with psycopg.connect(f"{base}/postgres", autocommit=True) as admin:
        if not admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone():
            try:
                admin.execute(f'CREATE DATABASE "{name}"')
            except psycopg.errors.DuplicateDatabase:
                pass  # created concurrently by another process
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(SCHEMA)


class ChargeIn(BaseModel):
    customer: str = Field(min_length=1)
    amount: int = Field(gt=0)  # minor units (cents)
    currency: str = "usd"


class Charge(BaseModel):
    id: int
    customer: str
    amount: int
    currency: str
    idempotency_key: str | None


class KeyReused(Exception):
    pass


def create_charge(conn, body: ChargeIn, key: str | None) -> tuple[dict, bool]:
    """Charge, or return the charge already made for this key.

    Returns (charge, replayed). Runs inside the caller's transaction; the key
    row and the charge commit together, so a crash can never leave a charge
    without its key (a later retry would charge again) or a key without its
    charge.
    """
    if key is None:
        return _insert_charge(conn, body, None), False

    request_hash = hashlib.sha256(json.dumps(body.model_dump(), sort_keys=True).encode()).hexdigest()
    # If another transaction is mid-way through the same key, this waits for
    # it to commit, then conflicts: the duplicate never charges.
    claimed = conn.execute(
        "INSERT INTO idempotency_keys (key, request_hash) VALUES (%s, %s)"
        " ON CONFLICT (key) DO NOTHING RETURNING key",
        (key, request_hash),
    ).fetchone()
    if claimed:
        charge = _insert_charge(conn, body, key)
        conn.execute("UPDATE idempotency_keys SET charge_id = %s WHERE key = %s", (charge["id"], key))
        return charge, False

    stored = conn.execute(
        "SELECT k.request_hash, c.id, c.customer, c.amount, c.currency, c.idempotency_key"
        " FROM idempotency_keys k JOIN charges c ON c.id = k.charge_id WHERE k.key = %s",
        (key,),
    ).fetchone()
    if stored.pop("request_hash") != request_hash:
        raise KeyReused(f"Idempotency-Key {key!r} was used with different parameters")
    return stored, True


def _insert_charge(conn, body: ChargeIn, key: str | None) -> dict:
    return conn.execute(
        "INSERT INTO charges (customer, amount, currency, idempotency_key) VALUES (%s, %s, %s, %s)"
        " RETURNING id, customer, amount, currency, idempotency_key",
        (body.customer, body.amount, body.currency, key),
    ).fetchone()


@asynccontextmanager
async def lifespan(app: FastAPI):
    url = database_url()
    ensure_database(url)
    with ConnectionPool(url, min_size=1, max_size=10, kwargs={"row_factory": dict_row}) as pool:
        app.state.pool = pool
        yield


app = FastAPI(title="fake payments", lifespan=lifespan)


@app.post("/charges", status_code=201, response_model=Charge)
def post_charge(
    body: ChargeIn,
    response: Response,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
):
    try:
        with app.state.pool.connection() as conn:  # one transaction, committed on exit
            charge, replayed = create_charge(conn, body, idempotency_key)
    except KeyReused as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    if replayed:
        response.status_code = 200
        response.headers["Idempotent-Replayed"] = "true"
    return charge
