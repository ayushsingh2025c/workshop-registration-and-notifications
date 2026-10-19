import logging
import os
import time
from datetime import timedelta

import httpx
from sqlalchemy import select, update
from sqlalchemy.orm import sessionmaker

from app.database import SessionLocal
from app.models import DeliveryAttempt, Member, NotificationJob, Workshop, utcnow

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("notification-worker")
MAX_BACKOFF_SECONDS = 60
STREAM_NAME = "notification-jobs"
CONSUMER_GROUP = "notification-workers"


def send_notification(job, member, workshop):
    response = httpx.post(
        os.getenv("MOCK_NOTIFICATION_URL", "http://127.0.0.1:8001/notify"),
        json={
            "job_id": job.id,
            "notification_type": job.notification_type,
            "recipient": member.email,
            "workshop_title": workshop.title,
        },
        headers={"Idempotency-Key": job.idempotency_key},
        timeout=float(os.getenv("NOTIFICATION_TIMEOUT_SECONDS", "3")),
    )
    response.raise_for_status()


def process_one(
    session_factory: sessionmaker = SessionLocal,
    sender=send_notification,
    job_id: int | None = None,
) -> bool:
    now = utcnow()
    with session_factory() as db:
        job = db.scalar(
            select(NotificationJob)
            .where(
                NotificationJob.status == "pending",
                NotificationJob.next_retry_at <= now,
                NotificationJob.attempts < NotificationJob.max_attempts,
                *([NotificationJob.id == job_id] if job_id is not None else []),
            )
            .order_by(NotificationJob.created_at, NotificationJob.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if job is None:
            return False
        attempt_number = job.attempts + 1
        claimed = db.execute(
            update(NotificationJob)
            .where(NotificationJob.id == job.id, NotificationJob.status == "pending")
            .values(status="processing", attempts=NotificationJob.attempts + 1, updated_at=now)
        )
        if claimed.rowcount != 1:
            db.rollback()
            return False
        db.commit()
        job_id = job.id
        member = db.get(Member, job.member_id)
        workshop = db.get(Workshop, job.workshop_id)
        try:
            sender(job, member, workshop)
            outcome, error = "success", None
        except (httpx.HTTPError, TimeoutError, OSError) as exc:
            outcome, error = "retryable_error", str(exc)[:1000]
        with session_factory() as result_db:
            current = result_db.get(NotificationJob, job_id)
            current.updated_at = utcnow()
            result_db.add(
                DeliveryAttempt(
                    job_id=job_id,
                    attempt_number=attempt_number,
                    outcome=outcome,
                    error_detail=error,
                )
            )
            if outcome == "success":
                current.status = "succeeded"
                current.completed_at = utcnow()
                current.last_error = None
            else:
                current.last_error = error
                if attempt_number >= current.max_attempts:
                    current.status = "failed"
                    current.completed_at = utcnow()
                else:
                    delay = min(2 ** (attempt_number - 1), MAX_BACKOFF_SECONDS)
                    current.status = "pending"
                    current.next_retry_at = utcnow() + timedelta(seconds=delay)
                    current.redis_published_at = None
                    current.redis_message_id = None
            result_db.commit()
        log.info("job_id=%s type=%s attempt=%s outcome=%s", job_id, job.notification_type, attempt_number, outcome)
        return True


def recover_stale_jobs(session_factory: sessionmaker = SessionLocal, stale_after_seconds: int = 300):
    cutoff = utcnow() - timedelta(seconds=stale_after_seconds)
    with session_factory() as db:
        recovered = db.execute(
            update(NotificationJob)
            .where(
                NotificationJob.status == "processing",
                NotificationJob.updated_at < cutoff,
                NotificationJob.attempts < NotificationJob.max_attempts,
            )
            .values(
                status="pending",
                next_retry_at=utcnow(),
                updated_at=utcnow(),
                redis_published_at=None,
                redis_message_id=None,
            )
        ).rowcount
        exhausted = db.execute(
            update(NotificationJob)
            .where(
                NotificationJob.status == "processing",
                NotificationJob.updated_at < cutoff,
                NotificationJob.attempts >= NotificationJob.max_attempts,
            )
            .values(status="failed", completed_at=utcnow(), updated_at=utcnow())
        ).rowcount
        db.commit()
        return recovered + exhausted


def publish_pending_jobs(redis_client, session_factory: sessionmaker = SessionLocal) -> int:
    published = 0
    with session_factory() as db:
        jobs = db.scalars(
            select(NotificationJob)
            .where(
                NotificationJob.status == "pending",
                NotificationJob.next_retry_at <= utcnow(),
                NotificationJob.redis_published_at.is_(None),
            )
            .order_by(NotificationJob.id)
            .limit(100)
        ).all()
        for job in jobs:
            message_id = redis_client.xadd(STREAM_NAME, {"job_id": str(job.id)})
            job.redis_published_at = utcnow()
            job.redis_message_id = message_id.decode() if isinstance(message_id, bytes) else str(message_id)
            published += 1
        db.commit()
    return published


def consume_stream_once(redis_client, session_factory: sessionmaker = SessionLocal, sender=send_notification) -> bool:
    messages = redis_client.xreadgroup(
        CONSUMER_GROUP,
        os.getenv("WORKER_NAME", "worker-1"),
        {STREAM_NAME: ">"},
        count=1,
        block=100,
    )
    for stream, entries in messages:
        for message_id, fields in entries:
            raw_job_id = fields.get("job_id", fields.get(b"job_id"))
            process_one(session_factory, sender, int(raw_job_id))
            redis_client.xack(stream, CONSUMER_GROUP, message_id)
            return True
    return False


def run_forever():
    recover_stale_jobs()
    redis_client = None
    try:
        import redis

        redis_client = redis.Redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            protocol=2,
        )
        redis_client.ping()
        try:
            redis_client.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
        except redis.exceptions.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        log.info("Connected to Redis Streams")
    except Exception:
        log.warning("Redis Streams unavailable; using durable PostgreSQL polling", exc_info=True)
        redis_client = None
    while True:
        try:
            if redis_client is not None:
                publish_pending_jobs(redis_client)
                if consume_stream_once(redis_client):
                    continue
            if not process_one():
                time.sleep(float(os.getenv("WORKER_POLL_SECONDS", "1")))
        except Exception as exc:
            if redis_client is not None:
                log.warning("Redis Streams failed; falling back to PostgreSQL polling: %s", exc)
                redis_client = None
            else:
                log.exception("Worker iteration failed")
                time.sleep(1)


if __name__ == "__main__":
    run_forever()
