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
