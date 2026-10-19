import os
from datetime import timedelta

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

os.environ["DATABASE_URL"] = "sqlite+pysqlite:///:memory:"

from app.database import Base, get_db
import app.main as main_module
from app.main import app
from app.mock_notification import app as mock_app
from app.models import DeliveryAttempt, NotificationJob, Registration, utcnow
from app.worker import process_one, publish_pending_jobs, recover_stale_jobs


@pytest.fixture
def setup(monkeypatch):
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    monkeypatch.setattr(main_module, "engine", engine)

    def override_db():
        with factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_db
    with TestClient(app) as client:
        yield client, factory
    app.dependency_overrides.clear()
    engine.dispose()


def create_inputs(client):
    member = client.post("/members", json={"email": "member@example.com", "name": "Member"})
    workshop = client.post("/workshops", json={"title": "Test workshop"})
    assert member.status_code == workshop.status_code == 201
    return workshop.json()["id"], member.json()["id"]


def test_registration_is_atomic_and_does_not_wait_for_delivery(setup):
    client, factory = setup
    workshop_id, member_id = create_inputs(client)
    response = client.post(
        "/registrations",
        json={"workshop_id": workshop_id, "member_id": member_id},
    )
    assert response.status_code == 201
    with factory() as db:
        assert db.scalar(select(func.count(Registration.id))) == 1
        job = db.get(NotificationJob, response.json()["notification_job_id"])
        assert job.status == "pending"
        assert job.notification_type == "confirmation"


def test_duplicate_registration_is_rejected_by_database_constraint(setup):
    client, factory = setup
    workshop_id, member_id = create_inputs(client)
    payload = {"workshop_id": workshop_id, "member_id": member_id}
    assert client.post("/registrations", json=payload).status_code == 201
    assert client.post("/registrations", json=payload).status_code == 409
    with factory() as db:
        assert db.scalar(select(func.count(NotificationJob.id))) == 1


def test_worker_retries_failure_then_succeeds(setup):
    client, factory = setup
    workshop_id, member_id = create_inputs(client)
    job_id = client.post(
        "/registrations", json={"workshop_id": workshop_id, "member_id": member_id}
    ).json()["notification_job_id"]
    calls = []

    def sender(*_):
        calls.append(1)
        if len(calls) == 1:
            raise httpx.HTTPStatusError("503", request=None, response=None)

    assert process_one(factory, sender)
    with factory() as db:
        job = db.get(NotificationJob, job_id)
        assert job.status == "pending" and job.attempts == 1
        job.next_retry_at = utcnow() - timedelta(seconds=1)
        db.commit()
    assert process_one(factory, sender)
    with factory() as db:
        assert db.get(NotificationJob, job_id).status == "succeeded"
        assert db.scalar(select(func.count(DeliveryAttempt.id))) == 2


def test_worker_stops_after_bounded_retries(setup):
    client, factory = setup
    workshop_id, member_id = create_inputs(client)
    job_id = client.post(
        "/registrations", json={"workshop_id": workshop_id, "member_id": member_id}
    ).json()["notification_job_id"]

    def sender(*_):
        raise OSError("mock service unavailable")

    for _ in range(3):
        assert process_one(factory, sender)
        with factory() as db:
            job = db.get(NotificationJob, job_id)
            if job.status == "pending":
                job.next_retry_at = utcnow() - timedelta(seconds=1)
                db.commit()
    with factory() as db:
        job = db.get(NotificationJob, job_id)
        assert job.status == "failed" and job.attempts == 3
        assert db.scalar(select(func.count(DeliveryAttempt.id))) == 3


def test_redis_stream_dispatch_publishes_durable_job_id(setup):
    client, factory = setup
    workshop_id, member_id = create_inputs(client)
    job_id = client.post(
        "/registrations", json={"workshop_id": workshop_id, "member_id": member_id}
    ).json()["notification_job_id"]

    class FakeRedis:
        entries = []

        def xadd(self, stream, fields):
            self.entries.append((stream, fields))
            return b"1-0"

    fake_redis = FakeRedis()
    assert publish_pending_jobs(fake_redis, factory) == 1
    assert fake_redis.entries[0][1]["job_id"] == str(job_id)
    with factory() as db:
        assert db.get(NotificationJob, job_id).redis_published_at is not None


def test_mock_notification_failure_simulation_and_idempotency():
    client = TestClient(mock_app)
    payload = {
        "job_id": 9001,
        "notification_type": "confirmation",
        "recipient": "mock@example.com",
        "workshop_title": "Mock workshop",
    }
    headers = {"Idempotency-Key": "test-job-9001"}
    failed = client.post("/notify", json=payload, headers={**headers, "X-Mock-Failure": "error"})
    assert failed.status_code == 503
    first = client.post("/notify", json=payload, headers=headers)
    repeated = client.post("/notify", json=payload, headers=headers)
    assert first.json()["duplicate"] is False
    assert repeated.json()["duplicate"] is True


def test_cancellation_creates_one_job_per_registered_member_and_is_idempotent(setup):
    client, factory = setup
    workshop_id, first = create_inputs(client)
    second = client.post("/members", json={"email": "second@example.com", "name": "Second"}).json()["id"]
    for member_id in (first, second):
        assert client.post(
            "/registrations", json={"workshop_id": workshop_id, "member_id": member_id}
        ).status_code == 201
    response = client.post(f"/workshops/{workshop_id}/cancel")
    assert response.status_code == 202
    assert response.json()["notification_jobs_created"] == 2
    assert client.post(f"/workshops/{workshop_id}/cancel").json()["notification_jobs_created"] == 2
    with factory() as db:
        assert db.scalar(
            select(func.count(NotificationJob.id)).where(
                NotificationJob.notification_type == "cancellation"
            )
        ) == 2


def test_stale_processing_job_is_recovered_after_restart(setup):
    client, factory = setup
    workshop_id, member_id = create_inputs(client)
    job_id = client.post(
        "/registrations", json={"workshop_id": workshop_id, "member_id": member_id}
    ).json()["notification_job_id"]
    with factory() as db:
        job = db.get(NotificationJob, job_id)
        job.status = "processing"
        job.updated_at = utcnow() - timedelta(minutes=10)
        db.commit()
    assert recover_stale_jobs(factory) == 1
    with factory() as db:
        assert db.get(NotificationJob, job_id).status == "pending"
