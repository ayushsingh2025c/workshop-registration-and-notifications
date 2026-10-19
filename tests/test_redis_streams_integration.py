import os
import uuid
from datetime import timedelta

import httpx
import pytest
import redis
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

os.environ["DATABASE_URL"] = "sqlite+pysqlite:///:memory:"

import app.main as main_module
from app.database import Base, get_db
from app.main import app
from app.models import NotificationJob, utcnow
from app.worker import (
    connect_redis,
    consume_stream_once,
    publish_pending_jobs,
)
import app.worker as worker_module


@pytest.fixture
def streams(monkeypatch):
    url = os.getenv("REDIS_URL")
    if not url:
        pytest.skip("Set REDIS_URL to run real Redis Streams integration tests")

    suffix = uuid.uuid4().hex
    stream = f"notification-jobs:test:{suffix}"
    group = f"notification-workers:test:{suffix}"
    monkeypatch.setattr(worker_module, "STREAM_NAME", stream)
    monkeypatch.setattr(worker_module, "CONSUMER_GROUP", group)
    try:
        client = connect_redis()
    except redis.RedisError as exc:
        pytest.skip(f"Redis Streams integration endpoint unavailable: {exc}")
    assert any(g["name"] == group for g in client.xinfo_groups(stream))
    yield client, stream, group
    client.delete(stream)
    client.close()


@pytest.fixture
def app_db(monkeypatch):
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


def create_pending_job(client, factory):
    member = client.post(
        "/members",
        json={"email": f"{uuid.uuid4().hex}@example.test", "name": "Streams test"},
    ).json()
    workshop = client.post("/workshops", json={"title": "Redis Streams test"}).json()
    registration = client.post(
        "/registrations",
        json={"workshop_id": workshop["id"], "member_id": member["id"]},
    ).json()
    with factory() as db:
        job = db.get(NotificationJob, registration["notification_job_id"])
        assert job.status == "pending"
    return registration["notification_job_id"]


def test_real_redis_group_publish_consume_and_ack(streams, app_db):
    client, factory = app_db
    redis_client, stream, group = streams
    job_id = create_pending_job(client, factory)
    assert any(g["name"] == group for g in redis_client.xinfo_groups(stream))

    assert publish_pending_jobs(redis_client, factory) == 1
    calls = []
    assert consume_stream_once(redis_client, factory, lambda *args: calls.append(args))
    assert len(calls) == 1
    assert redis_client.xpending(stream, group)["pending"] == 0
    with factory() as db:
        job = db.get(NotificationJob, job_id)
        assert job.status == "succeeded"
        assert job.redis_message_id


def test_real_redis_failure_is_acked_and_durable_retry_is_republished(streams, app_db):
    client, factory = app_db
    redis_client, stream, group = streams
    job_id = create_pending_job(client, factory)

    assert publish_pending_jobs(redis_client, factory) == 1

    def fail(*_):
        raise httpx.HTTPStatusError("simulated 503", request=None, response=None)

    assert consume_stream_once(redis_client, factory, fail)
    assert redis_client.xpending(stream, group)["pending"] == 0
    with factory() as db:
        job = db.get(NotificationJob, job_id)
        assert job.status == "pending" and job.attempts == 1
        assert job.last_error
        job.next_retry_at = utcnow() - timedelta(seconds=1)
        db.commit()

    assert publish_pending_jobs(redis_client, factory) == 1
    assert consume_stream_once(redis_client, factory, lambda *_: None)
    with factory() as db:
        assert db.get(NotificationJob, job_id).status == "succeeded"
    assert redis_client.xpending(stream, group)["pending"] == 0


def test_real_redis_reclaims_worker_pending_entry(streams, app_db, monkeypatch):
    client, factory = app_db
    redis_client, stream, group = streams
    job_id = create_pending_job(client, factory)
    assert publish_pending_jobs(redis_client, factory) == 1
    redis_client.xreadgroup(group, "crashed-worker", {stream: ">"}, count=1)
    with factory() as db:
        job = db.get(NotificationJob, job_id)
        job.status = "processing"
        job.updated_at = utcnow() - timedelta(minutes=10)
        db.commit()

    assert worker_module.recover_stale_jobs(factory) == 1
    monkeypatch.setattr(worker_module, "STREAM_CLAIM_IDLE_MS", 0)
    assert consume_stream_once(redis_client, factory, lambda *_: None)
    assert redis_client.xpending(stream, group)["pending"] == 0
    with factory() as db:
        assert db.get(NotificationJob, job_id).status == "succeeded"


def test_real_redis_duplicate_entries_process_job_once(streams, app_db):
    client, factory = app_db
    redis_client, stream, group = streams
    job_id = create_pending_job(client, factory)
    assert publish_pending_jobs(redis_client, factory) == 1
    redis_client.xadd(stream, {"job_id": str(job_id)})
    calls = []
    sender = lambda *args: calls.append(args)

    assert consume_stream_once(redis_client, factory, sender)
    assert consume_stream_once(redis_client, factory, sender)
    assert len(calls) == 1
    assert redis_client.xpending(stream, group)["pending"] == 0


def test_real_redis_reconciles_after_publish_outage(streams, app_db):
    client, factory = app_db
    redis_client, stream, _ = streams
    job_id = create_pending_job(client, factory)

    class OfflineRedis:
        def xadd(self, *_args, **_kwargs):
            raise redis.ConnectionError("simulated Redis outage")

    with pytest.raises(redis.ConnectionError):
        publish_pending_jobs(OfflineRedis(), factory)
    with factory() as db:
        job = db.get(NotificationJob, job_id)
        assert job.status == "pending"
        assert job.redis_published_at is None

    assert publish_pending_jobs(redis_client, factory) == 1
    assert redis_client.xlen(stream) == 1
    with factory() as db:
        assert db.get(NotificationJob, job_id).redis_published_at is not None
