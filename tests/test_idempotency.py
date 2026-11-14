"""Idempotency at both layers: submission (one job per request) and execution
(one effect per job), ending with the crash-after-side-effect test."""

import threading
import time

import httpx
import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from fake_payments.app import ChargeIn, create_charge
from jobq import queue
from tests.conftest import PAYMENTS_TEST_URL
from tests.helpers import wait_until

CHARGE = {"customer": "cus_42", "amount": 500}


def charges(payments):
    return payments.execute("SELECT count(*) FROM charges").fetchone()[0]


# --- the payments service dedupes on Idempotency-Key -------------------------


def test_payments_same_key_charges_once(payments, payments_url):
    first = httpx.post(f"{payments_url}/charges", json=CHARGE, headers={"Idempotency-Key": "k1"})
    again = httpx.post(f"{payments_url}/charges", json=CHARGE, headers={"Idempotency-Key": "k1"})

    assert (first.status_code, again.status_code) == (201, 200)
    assert again.headers["Idempotent-Replayed"] == "true"
    assert again.json()["id"] == first.json()["id"]
    assert charges(payments) == 1


def test_payments_without_a_key_charges_every_time(payments, payments_url):
    for _ in range(2):
        assert httpx.post(f"{payments_url}/charges", json=CHARGE).status_code == 201
    assert charges(payments) == 2


def test_payments_rejects_a_reused_key_with_different_parameters(payments, payments_url):
    headers = {"Idempotency-Key": "k1"}
    httpx.post(f"{payments_url}/charges", json=CHARGE, headers=headers)

    resp = httpx.post(f"{payments_url}/charges", json={**CHARGE, "amount": 900}, headers=headers)

    assert resp.status_code == 422
    assert charges(payments) == 1


def test_payments_concurrent_duplicate_waits_and_gets_the_same_charge(payments):
    """Request B arrives while A's transaction (key + charge) is uncommitted."""
    body = ChargeIn(**CHARGE)
    with psycopg.connect(PAYMENTS_TEST_URL, row_factory=dict_row) as a, psycopg.connect(
        PAYMENTS_TEST_URL, row_factory=dict_row
    ) as b:
        charge_a, replayed_a = create_charge(a, body, "k1")  # not committed yet

        result = {}
        thread = threading.Thread(target=lambda: result.update(b=create_charge(b, body, "k1")))
        thread.start()
        time.sleep(0.3)
        assert thread.is_alive()  # blocked on A's uncommitted key row

        a.commit()
        thread.join(timeout=5)
        b.commit()

    charge_b, replayed_b = result["b"]
    assert (replayed_a, replayed_b) == (False, True)
    assert charge_b["id"] == charge_a["id"]
    assert charges(payments) == 1


# --- submission: one job per client request ----------------------------------

T1 = {"X-Tenant-Id": "tenant-a"}


def test_resubmitting_with_the_same_key_returns_the_original_job(client, conn):
    headers = {**T1, "Idempotency-Key": "order-981"}
    first = client.post("/jobs", json={"type": "charge", "payload": CHARGE}, headers=headers)
    again = client.post("/jobs", json={"type": "charge", "payload": CHARGE}, headers=headers)

    assert (first.status_code, again.status_code) == (201, 200)
    assert again.headers["Idempotent-Replayed"] == "true"
    assert "Idempotent-Replayed" not in first.headers
    assert again.json()["id"] == first.json()["id"]
    assert conn.execute("SELECT count(*) FROM jobs").fetchone()[0] == 1


def test_reusing_a_key_for_a_different_request_is_rejected(client, conn):
    headers = {**T1, "Idempotency-Key": "order-981"}
    client.post("/jobs", json={"type": "charge", "payload": CHARGE}, headers=headers)

    for changed in (
        {"type": "charge", "payload": {**CHARGE, "amount": 900}},
        {"type": "refund", "payload": CHARGE},
        {"type": "charge", "payload": CHARGE, "max_attempts": 9},
    ):
        assert client.post("/jobs", json=changed, headers=headers).status_code == 422
    assert conn.execute("SELECT count(*) FROM jobs").fetchone()[0] == 1


def test_keys_are_scoped_per_tenant_and_optional(client, conn):
    body = {"type": "noop"}
    client.post("/jobs", json=body, headers={**T1, "Idempotency-Key": "k"})
    client.post("/jobs", json=body, headers={"X-Tenant-Id": "tenant-b", "Idempotency-Key": "k"})
    client.post("/jobs", json=body, headers=T1)  # no key: always a new job
    client.post("/jobs", json=body, headers=T1)

    assert conn.execute("SELECT count(*) FROM jobs").fetchone()[0] == 4


def test_concurrent_submits_with_one_key_create_one_job(conn, db_url):
    """B submits while A's insert of the same key is uncommitted: B waits on
    the unique index, sees the conflict once A commits, and returns A's job."""
    args = ("t1", "noop", {}, 5, None, "order-981")
    with psycopg.connect(db_url, row_factory=dict_row) as a, psycopg.connect(
        db_url, row_factory=dict_row
    ) as b:
        job_a, created_a = queue.submit(a, *args)  # not committed yet

        result = {}
        thread = threading.Thread(target=lambda: result.update(b=queue.submit(b, *args)))
        thread.start()
        time.sleep(0.3)
        assert thread.is_alive()  # waiting on A's uncommitted key

        a.commit()
        thread.join(timeout=5)

    job_b, created_b = result["b"]
    assert (created_a, created_b) == (True, False)
    assert job_b["id"] == job_a["id"]
    assert conn.execute("SELECT count(*) FROM jobs").fetchone()[0] == 1


# --- execution: the crash-after-side-effect test -----------------------------


@pytest.mark.parametrize("keys, expected_charges", [("on", 1), ("off", 2)])
def test_crash_after_charge_retries_without_charging_twice(
    conn, payments, payments_url, spawn_worker, reaper, keys, expected_charges
):
    """Phase 4 'done when': 2 attempts but 1 charge with keys on, 2 with keys off.

    Attempt 1 charges, then its worker dies before acking (os._exit in
    charge_then_crash). The lease expires, the reaper requeues the job, and
    attempt 2 charges again: with the same Idempotency-Key (job-<id>) the
    payments service recognises the repeat; without it, the customer pays twice.
    """
    job_id = conn.execute(
        "INSERT INTO jobs (tenant_id, type, payload) VALUES ('t1', 'charge_then_crash', %s)"
        " RETURNING id",
        (Jsonb(CHARGE),),
    ).fetchone()[0]
    env = {"PAYMENTS_URL": payments_url, "JOBQ_PAYMENTS_IDEMPOTENCY": keys}

    a = spawn_worker("a", **env)
    assert a.wait(timeout=10) == 1  # charged, then died before acking
    assert charges(payments) == 1
    spawn_worker("b", **env)

    state = lambda: conn.execute("SELECT state FROM jobs WHERE id = %s", (job_id,)).fetchone()[0]  # noqa: E731
    assert wait_until(lambda: state() == "succeeded", timeout=15)

    assert conn.execute(
        "SELECT attempt, outcome FROM job_attempts WHERE job_id = %s ORDER BY attempt", (job_id,)
    ).fetchall() == [(1, "lease_expired"), (2, "succeeded")]
    assert charges(payments) == expected_charges
