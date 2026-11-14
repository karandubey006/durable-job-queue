"""Built-in job handlers. A handler is called as handler(job, conn, lease).

`job` is the claimed row as a dict. `conn` is the worker's autocommit
connection. `lease` is a jobq.worker.Lease: long handlers should call
lease.check() at safe points, which raises once the lease is lost.
Returning completes the job. Raising fails the attempt: network and database
errors (or jobq.errors.RetryableError) are retried with backoff, anything else
(or jobq.errors.PermanentError) sends the job straight to the DLQ.
"""

import os
import time

import httpx

from jobq.errors import PermanentError, RetryableError


def noop(job, conn, lease):
    pass


def sleep(job, conn, lease):
    """Demo: work for payload["seconds"] (default 20), checking the lease each second."""
    for _ in range(int(job["payload"].get("seconds", 20))):
        lease.check()
        time.sleep(1)


def charge(job, conn, lease):
    """Charge payload["customer"] payload["amount"] through the payments API.

    The Idempotency-Key is derived from the job id, so it's the same on every
    attempt of this job: if attempt 1 charged and then crashed before acking,
    attempt 2's request is recognised as a repeat and doesn't charge again.
    (Never derive it from the attempt number or the lease token: those differ
    per attempt, which is exactly what a fencing token is for and exactly what
    an idempotency key must not do.)

    JOBQ_PAYMENTS_IDEMPOTENCY=off omits the key, to demonstrate the duplicate.
    """
    url = os.environ.get("PAYMENTS_URL", "http://localhost:8001")
    headers = {}
    if os.environ.get("JOBQ_PAYMENTS_IDEMPOTENCY", "on") != "off":
        headers["Idempotency-Key"] = f"job-{job['id']}"
    body = {
        "customer": job["payload"]["customer"],
        "amount": job["payload"]["amount"],
        "currency": job["payload"].get("currency", "usd"),
    }
    # Translate httpx's errors into ours (D17 can't recognise them by type).
    # A timeout means "don't know", not "didn't happen": the charge may have
    # gone through. Retrying is safe only because of the key.
    try:
        resp = httpx.post(f"{url}/charges", json=body, headers=headers, timeout=10)
        resp.raise_for_status()
    except httpx.TransportError as exc:
        raise RetryableError(f"payments unreachable: {exc!r}") from exc
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if status == 429 or status >= 500:
            raise RetryableError(f"payments returned {status}") from exc
        raise PermanentError(f"payments returned {status}: {exc.response.text}") from exc


HANDLERS = {"noop": noop, "sleep": sleep, "charge": charge}
