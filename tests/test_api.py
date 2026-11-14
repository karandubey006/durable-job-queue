from datetime import datetime, timedelta, timezone

T1 = {"X-Tenant-Id": "tenant-a"}
T2 = {"X-Tenant-Id": "tenant-b"}


def test_submit_creates_committed_job(client, conn):
    resp = client.post("/jobs", json={"type": "email", "payload": {"to": "x@y.z"}}, headers=T1)

    assert resp.status_code == 201
    job = resp.json()
    assert job["tenant_id"] == "tenant-a"
    assert job["state"] == "queued"
    assert job["attempts"] == 0
    assert job["max_attempts"] == 5

    # Read it back on a separate connection: proves it was committed, not just returned.
    row = conn.execute("SELECT type, payload, state FROM jobs WHERE id = %s", (job["id"],)).fetchone()
    assert row == ("email", {"to": "x@y.z"}, "queued")


def test_get_returns_job(client):
    job_id = client.post("/jobs", json={"type": "noop"}, headers=T1).json()["id"]

    resp = client.get(f"/jobs/{job_id}", headers=T1)

    assert resp.status_code == 200
    assert resp.json()["id"] == job_id


def test_get_missing_job_is_404(client):
    assert client.get("/jobs/999", headers=T1).status_code == 404


def test_other_tenants_job_is_404(client):
    job_id = client.post("/jobs", json={"type": "noop"}, headers=T1).json()["id"]
    assert client.get(f"/jobs/{job_id}", headers=T2).status_code == 404


def test_tenant_header_required(client, conn):
    assert client.post("/jobs", json={"type": "noop"}).status_code == 422
    assert conn.execute("SELECT count(*) FROM jobs").fetchone()[0] == 0


def test_scheduled_run_at(client):
    run_at = datetime.now(timezone.utc) + timedelta(hours=1)
    resp = client.post("/jobs", json={"type": "noop", "run_at": run_at.isoformat()}, headers=T1)

    assert resp.status_code == 201
    assert datetime.fromisoformat(resp.json()["run_at"]) == run_at


def test_run_at_without_timezone_rejected(client):
    resp = client.post("/jobs", json={"type": "noop", "run_at": "2026-12-01T09:00:00"}, headers=T1)
    assert resp.status_code == 422


def test_invalid_body_rejected(client):
    assert client.post("/jobs", json={"type": ""}, headers=T1).status_code == 422
    assert client.post("/jobs", json={"type": "x", "max_attempts": 0}, headers=T1).status_code == 422


def dead_job(conn, tenant, error="KeyError('x')"):
    return conn.execute(
        "INSERT INTO jobs (tenant_id, type, state, attempts, last_error)"
        " VALUES (%s, 'noop', 'dead', 5, %s) RETURNING id",
        (tenant, error),
    ).fetchone()[0]


def test_dlq_lists_only_the_tenants_dead_jobs(client, conn):
    mine = [dead_job(conn, "tenant-a") for _ in range(3)]
    dead_job(conn, "tenant-b")
    client.post("/jobs", json={"type": "noop"}, headers=T1)  # queued, not dead

    page1 = client.get("/dlq?limit=2", headers=T1).json()
    page2 = client.get(f"/dlq?limit=2&after_id={page1[-1]['id']}", headers=T1).json()

    assert [j["id"] for j in page1 + page2] == mine
    assert page1[0]["last_error"] == "KeyError('x')"


def budget(resp):
    return {k: resp.json()[k] for k in ("state", "attempts", "max_attempts")}


def test_replay_requeues_a_dead_job_with_more_attempts(client, conn):
    job_id = dead_job(conn, "tenant-a")  # died at 5/5

    resp = client.post(f"/jobs/{job_id}/replay", headers=T1)

    assert resp.status_code == 200
    assert budget(resp) == {"state": "queued", "attempts": 5, "max_attempts": 10}
    assert client.get("/dlq", headers=T1).json() == []


def test_replay_with_an_explicit_number_of_attempts(client, conn):
    job_id = dead_job(conn, "tenant-a")
    assert client.post(f"/jobs/{job_id}/replay", json={"extra_attempts": 0}, headers=T1).status_code == 422

    resp = client.post(f"/jobs/{job_id}/replay", json={"extra_attempts": 2}, headers=T1)

    assert budget(resp) == {"state": "queued", "attempts": 5, "max_attempts": 7}


def test_replay_refuses_jobs_that_arent_dead(client):
    job_id = client.post("/jobs", json={"type": "noop"}, headers=T1).json()["id"]
    assert client.post(f"/jobs/{job_id}/replay", headers=T1).status_code == 409


def test_replay_of_another_tenants_or_missing_job_is_404(client, conn):
    job_id = dead_job(conn, "tenant-a")
    assert client.post(f"/jobs/{job_id}/replay", headers=T2).status_code == 404
    assert client.post("/jobs/999/replay", headers=T1).status_code == 404
