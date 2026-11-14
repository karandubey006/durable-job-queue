# jobq

A durable job queue built on PostgreSQL. Clients submit jobs over HTTP, and a pool of worker processes runs them using leases, fenced acknowledgements, retries with exponential backoff, a dead-letter queue and idempotency keys. Real worker processes are killed partway through jobs in the test suite, which checks that no job is lost and no side effect happens twice.

All of the queue's correctness lives in a few raw SQL statements (no ORM). Each one is short enough to read in full: [`jobq/queue.py`](jobq/queue.py).

```mermaid
flowchart LR
    C[Client] -- "POST /jobs" --> A[API<br/>FastAPI]
    A -- "INSERT" --> P[(PostgreSQL<br/>jobs, job_attempts)]
    W1[Worker] & W2[Worker] & W3[Worker] -- "claim / heartbeat / ack" --> P
    R[Reaper] -- "requeue expired leases" --> P
    W1 -- "Idempotency-Key: job-42" --> X[External API<br/>e.g. payments]
```

## What it guarantees

jobq guarantees **at-least-once** execution, not exactly-once. Exactly-once *effects* need the handler's cooperation (see [Writing handlers](#writing-handlers)).

| Guarantee | What it means |
| --- | --- |
| **Durability** | Once `POST /jobs` returns, the job is committed and survives restarts of the API and workers. (This assumes a single Postgres node with `synchronous_commit = on`.) |
| **No lost jobs** | Every accepted job eventually ends up `succeeded` or `dead`, as long as Postgres is up and at least one worker and the reaper are running. |
| **One valid lease at a time** | At most one worker holds a job's current lease token, and only that worker's acknowledgement is accepted. Late acks from stale workers are rejected. |
| **Bounded retries** | A job runs at most `max_attempts` times, crashes included. After that it moves to the dead-letter queue with its last error. |
| **Idempotent submission** | Resubmitting with the same tenant and `Idempotency-Key` returns the original job instead of creating a second one. |

**Not guaranteed:** strict FIFO order, or that only one copy of a job runs at a time. A worker frozen by a long GC pause or a network partition can still be running while a second attempt starts. Fencing stops the frozen worker from changing the job's state, but not from repeating its side effects. Idempotency keys handle that.

## Quick start

You need Docker and [uv](https://docs.astral.sh/uv/).

```bash
# Postgres, migrations, the API (:8000), the reaper and a fake payments API (:8001)
docker compose up -d --build

# Start a worker (run several in separate terminals to see them share the work)
uv run python -m jobq.worker

# Submit a job
curl -X POST localhost:8000/jobs \
  -H 'X-Tenant-Id: acme' -H 'Idempotency-Key: order-981' \
  -H 'Content-Type: application/json' \
  -d '{"type": "charge", "payload": {"customer": "cus_42", "amount": 500}}'
```

Send the same request again and you get the same job back with `200` and `Idempotent-Replayed: true`. Send it with the same key and a different amount and you get a `422`.

### Watch a crash recovery

```bash
# A worker with a short 6 s lease, and a job that takes 20 s
JOBQ_LEASE_SECONDS=6 uv run python -m jobq.worker
curl -X POST localhost:8000/jobs -H 'X-Tenant-Id: acme' -H 'Content-Type: application/json' \
  -d '{"type": "sleep", "payload": {"seconds": 20}}'

# Kill the worker partway through, with no chance to clean up
pkill -9 -f jobq.worker

# Watch the lease run out; the reaper requeues the job within a few seconds
docker compose exec db psql -U jobq -c \
  "SELECT id, state, attempts, lease_token, lease_expires_at - now() AS lease_left FROM jobs"
```

Start a worker again and it claims the job as attempt 2 with a new, larger lease token. The `job_attempts` table records attempt 1 as `lease_expired`.

## How it works

### Job lifecycle

```mermaid
stateDiagram-v2
    [*] --> queued: submit
    queued --> running: claim
    running --> succeeded: complete
    running --> queued: retryable error or lease expired, attempts left
    running --> dead: permanent error or attempts used up
    dead --> queued: replay
```

Every claim issues a new lease token and counts an attempt. Only an acknowledgement carrying the job's current token can move it out of `running`. A retry waits out its backoff in `queued`, because its `run_at` is in the future.

### Claiming: one statement, `FOR UPDATE SKIP LOCKED`

A worker claims a job with a single `UPDATE`. Its subquery picks the oldest runnable job and locks it with `FOR UPDATE SKIP LOCKED`, so concurrent workers never get the same row and never wait on one another. The same statement marks the job `running`, increments `attempts`, issues a fresh lease token and records the attempt. A partial index on `(run_at, id) WHERE state = 'queued'` serves both the filter and the sort, and its size tracks the backlog rather than the table's history.

Removing the row lock causes double execution: in testing, 10,000 jobs produced 23,351 handler runs.

### Leases, heartbeats and fencing

A claim doesn't hold a database lock while the job runs. Instead, it grants a **lease** (`lease_expires_at`, set from the database's clock) and a **fencing token** (`lease_token`, taken from a sequence, so every claim gets a new, larger value).

- **Heartbeats:** a background thread in the worker extends the lease every `lease / 3` seconds, on its own connection.
- **Reaper:** a separate process requeues running jobs whose lease has expired. It moves them to `dead` instead if they have no attempts left.
- **Fencing:** heartbeat, complete and fail all run `WHERE id = $1 AND lease_token = $2 AND state = 'running'`. A worker whose lease was revoked, whether it has been reaped or re-claimed by someone else, matches zero rows and its write is rejected. When a heartbeat is rejected, the handler is told to stop.

All times come from Postgres's `now()`, so clock drift between worker machines doesn't matter. In the worst case, a dead worker's job waits *lease length + reaper interval + one poll* before it runs again (36 s with the defaults).

### Retries, backoff and the dead-letter queue

Handler exceptions are classified by type. Network and database errors (`ConnectionError`, `TimeoutError`, `psycopg.OperationalError`) are retried, and anything else, bugs included, goes straight to the DLQ. Handlers can override this by raising `RetryableError` or `PermanentError`.

A retry waits `random(0, min(cap, base · 2^(attempt-1)))` seconds (exponential backoff with **full jitter**), so jobs that failed together don't all retry together. The wait is stored as data in `run_at`, which means there are no timers, no separate retry queue, and nothing is lost if every process restarts.

A dead job stays in `jobs` with `state = 'dead'` and its `last_error`. `POST /jobs/{id}/replay` sends it back with more attempts. Every attempt, with its worker, timings, outcome and error, is kept in `job_attempts`, which is written in the same statement as each state change.

### Idempotency, at two layers

| Layer | Prevents | How |
| --- | --- | --- |
| **Submission** | A client retry (for example after a lost response) creating a second job | `UNIQUE (tenant_id, idempotency_key)` and `INSERT … ON CONFLICT DO NOTHING`. The constraint, not a prior `SELECT`, settles concurrent duplicates. |
| **Execution** | A retried job repeating its side effect | The handler sends `Idempotency-Key: job-<id>` to the external API. The key is the same on every attempt, and the receiver deduplicates on it. |

The bundled fake payments service shows the receiver's side. It commits the key and the charge in one transaction, so a crash can never leave one without the other. A concurrent duplicate request waits on the key's row lock and then gets the stored charge back.

## API

All endpoints are scoped to the tenant in the `X-Tenant-Id` header. Another tenant's jobs look like they don't exist (404).

| Endpoint | Description |
| --- | --- |
| `POST /jobs` | Body: `type`, `payload` (JSON object), `max_attempts` (1–100, default 5), `run_at` (optional, must include a timezone). Optional `Idempotency-Key` header. Returns `201` for a new job, `200` with `Idempotent-Replayed: true` for a repeat, or `422` if the key was used for a different request. |
| `GET /jobs/{id}` | The job, including `state`, `attempts` and `last_error`. |
| `GET /dlq?after_id=&limit=` | Dead jobs, oldest first. Page through by passing the last id you saw as `after_id`. |
| `POST /jobs/{id}/replay` | Requeue a dead job. Optional body `{"extra_attempts": N}`, which defaults to the job's current `max_attempts`. Returns `409` if the job isn't dead. |

## Writing handlers

A handler is a function `handler(job, conn, lease)` registered in a module's `HANDLERS` dict. Workers load it with `python -m jobq.worker --handlers your.module`. If the handler returns, the job is complete. If it raises, the attempt has failed.

Because delivery is at-least-once, a handler must be safe to run more than once:

- **Make external calls idempotent.** Pass a key that comes from the job id and is stable across attempts, never from the attempt number. See the `charge` handler in [`jobq/handlers.py`](jobq/handlers.py).
- **Classify errors.** Wrap your HTTP client's errors in `RetryableError` or `PermanentError`. Most clients' exception types (`httpx`'s included) aren't subclasses of Python's built-in `TimeoutError` or `ConnectionError`.
- **Long jobs should call `lease.check()`** at safe points. It raises once the lease is lost, meaning another worker may already own the job.
- **If the downstream API has no idempotency support**, check before acting (for example, look up an existing charge tagged with the job id) and reconcile afterwards. A small race window remains.

## Testing

The tests run against a real Postgres, because concurrency bugs don't show up with mocks.

```bash
docker compose up -d db
uv run pytest
```

Highlights:

- **Exactly-once claiming under contention:** 8 worker processes drain 10,000 jobs, and every job runs exactly once.
- **`kill -9` mid-job:** another worker finishes the job within lease + reaper interval. With a 2 s lease and a 0.5 s reaper interval, recovery takes about 2.5 s.
- **Zombie worker:** a worker is frozen with `SIGSTOP` until its lease expires and another worker takes over, then resumed. Its late acknowledgement is rejected, and the job stays with the new owner.
- **Crash after the side effect:** the worker charges a card and then exits before acknowledging. The retry shows 2 attempts but **1 charge** with idempotency keys, and **2 charges** without them.
- **Races resolved by constraints:** concurrent duplicate submissions, and concurrent duplicate charge requests, each produce exactly one row.
- **Retries:** a flaky job succeeds on attempt 3, a poison job lands in the DLQ after `max_attempts`, and backoff delays stay inside their bounds.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://jobq:jobq@localhost:5432/jobq` | The queue's database |
| `JOBQ_LEASE_SECONDS` | `30` | Lease length. Longer than the longest pause you expect, such as GC or a database failover. |
| `JOBQ_HEARTBEAT_SECONDS` | lease / 3 | Heartbeat interval. Two beats can be missed safely. |
| `JOBQ_REAPER_INTERVAL` | `5` | How often the reaper looks for expired leases |
| `JOBQ_BACKOFF_BASE` / `JOBQ_BACKOFF_CAP` | `1` / `300` | Backoff window in seconds: `min(cap, base · 2^(attempt-1))` |
| `PAYMENTS_URL` | `http://localhost:8001` | Where the `charge` handler sends requests |

## Design trade-offs

- **Postgres instead of a broker.** An application on the same database can enqueue a job in the same transaction as its own data, so there's no dual write and no separate system to run. The limits: every state change leaves a dead row version, so throughput depends on vacuum keeping up, and a long-running transaction anywhere in the database slows it down. Each worker also holds a connection, so large fleets need a pooler.
- **`SKIP LOCKED` costs strict ordering.** Jobs run roughly in `run_at` order, but a job that's locked or being claimed is skipped and a later one may start first.
- **Leases instead of held locks.** Claims are millisecond transactions, so there's no open transaction per running job blocking vacuum. The price is a recovery delay after a crash, and the need for fencing.
- **Lease length.** A short lease recovers faster. A long lease survives longer pauses without the job being taken from a live worker and run twice. Heartbeats let a job run longer than any single lease.
- **Attempts are counted at claim time.** A job that crashes its worker every time still uses up attempts and eventually stops, instead of looping forever. The cost is that an innocent job can lose an attempt to an unrelated crash.
- **The dead-letter queue is a state, not a table.** A replay keeps the job's id, history and idempotency key. Partial indexes keep dead rows out of the claim and reaper paths.

## Project layout

```
jobq/           api.py · queue.py (all queue SQL) · worker.py · reaper.py
                backoff.py · errors.py · handlers.py · settings.py · migrate.py
migrations/     numbered SQL files, applied in order by jobq/migrate.py
fake_payments/  a stand-in external payments API with Idempotency-Key support
tests/          integration tests against real Postgres and real processes
```

## Roadmap

- Per-tenant concurrency limits and queue caps (`429` on a full queue)
- A chaos test that randomly kills and restarts workers under load and reports lost jobs, duplicate effects and recovery-time percentiles
- Transactional handlers: commit a handler's own database writes in the same transaction as its acknowledgement, which makes same-database effects exactly-once
- Benchmarks: throughput by worker count, enqueue-to-start latency, and `LISTEN/NOTIFY` vs polling
