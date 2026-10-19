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
STREAM_CLAIM_IDLE_MS = 60_000
REDIS_RECONNECT_SECONDS = 5


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
    consumer = os.getenv("WORKER_NAME", "worker-1")
    claimed = redis_client.xautoclaim(
        STREAM_NAME,
        CONSUMER_GROUP,
        consumer,
        min_idle_time=STREAM_CLAIM_IDLE_MS,
        start_id="0-0",
        count=1,
    )
    entries = claimed[1]
    if entries:
        return _handle_stream_entry(
            redis_client,
            entries[0][0],
            entries[0][1],
            session_factory,
            sender,
            STREAM_NAME,
        )

    messages = redis_client.xreadgroup(
        CONSUMER_GROUP,
        consumer,
        {STREAM_NAME: ">"},
        count=1,
        block=100,
    )
    for stream, stream_entries in messages:
        for message_id, fields in stream_entries:
            return _handle_stream_entry(
                redis_client, message_id, fields, session_factory, sender, stream
            )
    return False


def _handle_stream_entry(
    redis_client,
    message_id,
    fields,
    session_factory: sessionmaker,
    sender,
    stream: str = STREAM_NAME,
) -> bool:
    raw_job_id = fields.get("job_id", fields.get(b"job_id"))
    if raw_job_id is None:
        raise ValueError(f"Stream entry {message_id} has no job_id")

    job_id = int(raw_job_id)
    process_one(session_factory, sender, job_id)
    with session_factory() as db:
        job = db.get(NotificationJob, job_id)
        # A retryable result is already durable as pending; the relay will
        # publish it again when next_retry_at is reached.
        handled = job is None or job.status in {"pending", "succeeded", "failed"}
    if handled:
        redis_client.xack(stream, CONSUMER_GROUP, message_id)
    return handled


def connect_redis():
    import redis

    client = redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://127.0.0.1:16380/0"),
        protocol=2,
        decode_responses=True,
    )
    client.ping()
    try:
        client.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise
    return client


def run_forever():
    recover_stale_jobs()
    redis_client = None
    last_recovery = time.monotonic()
    last_redis_attempt = 0.0
    while True:
        now = time.monotonic()
        if now - last_recovery >= 30:
            recover_stale_jobs()
            last_recovery = now
        if redis_client is None and now - last_redis_attempt >= REDIS_RECONNECT_SECONDS:
            last_redis_attempt = now
            try:
                redis_client = connect_redis()
                log.info("Connected to Redis Streams at configured REDIS_URL")
            except Exception as exc:
                log.warning("Redis Streams unavailable; using durable PostgreSQL polling: %s", exc)

        did_work = False
        try:
            if redis_client is not None:
                publish_pending_jobs(redis_client)
                did_work = consume_stream_once(redis_client)
        except Exception as exc:
            if redis_client is not None:
                log.warning("Redis Streams failed; falling back to PostgreSQL polling: %s", exc)
                redis_client = None
                last_redis_attempt = time.monotonic()
        if redis_client is None:
            try:
                did_work = process_one()
            except Exception:
                log.exception("PostgreSQL polling iteration failed")
        if not did_work:
            time.sleep(float(os.getenv("WORKER_POLL_SECONDS", "1")))


if __name__ == "__main__":
    run_forever()
