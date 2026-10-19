# Live Demonstration Guide

This walkthrough uses the endpoints and models implemented by the project. Notifications are sent only to the local mock service; no real email or SMS is sent.

## 1. Prerequisites and configuration

- Windows PowerShell and the project virtual environment (`.venv`).
- A PostgreSQL server and database that the configured database user can access. The application does not create the PostgreSQL database; create/select an empty demo database before starting the API.
- `.env` copied from `.env.example` and edited locally with valid PostgreSQL credentials. Do not show or submit `.env`.
- Docker Desktop/Compose is used to run local Redis 7.4.2. The Compose service binds only to `127.0.0.1:16380` and persists data in a named volume.

From the project folder, in **each terminal**, load the environment without printing it:

```powershell
$envFile = Get-Content .\.env
foreach ($line in $envFile) {
    if ($line -match '^\s*([^#=]+)=(.*)$') {
        Set-Item -Path ("Env:" + $Matches[1].Trim()) -Value $Matches[2].Trim().Trim('"').Trim("'")
    }
}
```

If `.env` does not exist yet, copy the template once with `Copy-Item .\.env.example .\.env`, then edit `.env` locally. Never overwrite an existing `.env` just to repeat setup.
Set its `REDIS_URL` to `redis://127.0.0.1:16380/0`. If `.env` already exists, change only that setting, preserving the database credentials and other local settings.

## 2. Start the three processes

Start Redis from the project folder:

```powershell
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" compose -f compose.redis.yml up -d
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" compose -f compose.redis.yml ps
```

Wait for the container health status to show `healthy`. Verify the server version and Streams command availability:

```powershell
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" exec task2-redis-1 redis-cli INFO server
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" exec task2-redis-1 redis-cli XINFO STREAM notification-jobs
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" exec task2-redis-1 redis-cli XINFO GROUPS notification-jobs
```

The worker creates the stream and consumer group with `MKSTREAM` at startup; `XINFO STREAM` can report length 0 before jobs are published. `XINFO GROUPS` should list `notification-workers`.

Open three PowerShell terminals in the project folder. Load `.env` in each terminal using the block above.

**Terminal 1 — API:**

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload --port 8000
```

At startup, SQLAlchemy `create_all` creates missing application tables in the configured database; it does not create the database itself or drop existing data.

**Terminal 2 — local mock notification service:**

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.mock_notification:app --port 8001
```

**Terminal 3 — separate notification worker:**

```powershell
.\.venv\Scripts\python.exe -m app.worker
```

The worker connects to `REDIS_URL`; for the Compose service set `redis://127.0.0.1:16380/0`. It creates/uses the `notification-jobs` stream and `notification-workers` group. The worker calls the mock service at `MOCK_NOTIFICATION_URL` (default `http://127.0.0.1:8001/notify`). When startup logs `Connected to Redis Streams at configured REDIS_URL`, Streams mode is active. If Redis goes down, it logs a warning and continues processing with PostgreSQL polling, then retries the Redis connection every five seconds.

Interactive API documentation is at <http://127.0.0.1:8000/docs>. The PowerShell requests below can also be pasted into another terminal. Use fresh email addresses and the IDs returned by the API.

## 3. Create a workshop and member

**Create a workshop**

```powershell
$workshop = Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:8000/workshops `
  -ContentType 'application/json' `
  -Body (@{title='Demo: reliable notifications';description='Local demonstration only'} | ConvertTo-Json)
$workshop
```

Endpoint: `POST /workshops`

Request body:

```json
{"title":"Demo: reliable notifications","description":"Local demonstration only"}
```

Expected: `201 Created`, with `id`, `title`, `description`, and `status: "active"`. Save `$workshop.id`.

**Create a member**

```powershell
$tag = [guid]::NewGuid().ToString('N').Substring(0,8)
$member = Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:8000/members `
  -ContentType 'application/json' `
  -Body (@{email="demo-$tag@example.test";name='Demo Member'} | ConvertTo-Json)
$member
```

Endpoint: `POST /members`

Request body:

```json
{"email":"demo-<unique-tag>@example.test","name":"Demo Member"}
```

Expected: `201 Created`, with `id`, normalized `email`, and `name`. Save `$member.id`. The `.test` address is a dummy address; the mock does not contact it.

## 4. Register, reject a duplicate, and verify confirmation

**Register once**

```powershell
$payload = @{workshop_id=$workshop.id;member_id=$member.id} | ConvertTo-Json
$registration = Invoke-RestMethod -Method Post `
  -Uri http://127.0.0.1:8000/registrations `
  -ContentType 'application/json' -Body $payload
$registration
```

Endpoint: `POST /registrations`

Request body:

```json
{"workshop_id":<workshop-id>,"member_id":<member-id>}
```

Expected: `201 Created` with `id`, `workshop_id`, `member_id`, `status: "registered"`, and `notification_job_id`. Registration and its pending confirmation job are committed in one transaction. The HTTP handler does not wait for notification delivery.

**Repeat the exact registration** (the expected error is `409`, so catch it in PowerShell):

```powershell
try {
    Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/registrations `
      -ContentType 'application/json' -Body $payload
} catch {
    "Duplicate rejected: HTTP $([int]$_.Exception.Response.StatusCode)"
}
```

Expected: `409 Conflict` with detail `Member is already registered for this workshop`. The unique database constraint prevents a second registration; the rejected call does not add a second job.

**Inspect the confirmation job**

```powershell
$job = Invoke-RestMethod -Uri "http://127.0.0.1:8000/notification-jobs/$($registration.notification_job_id)"
$job
```

Endpoint: `GET /notification-jobs/{job_id}`. Expected final evidence is `notification_type: "confirmation"`, `status: "succeeded"`, `attempts: 1`. It can briefly be `pending` while the worker picks it up. Also show the worker's success log line.

## 5. Cancel and verify cancellation notifications

Create/register any additional members before cancellation if you want to show fan-out to multiple recipients. Then cancel:

```powershell
$cancellation = Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8000/workshops/$($workshop.id)/cancel"
$cancellation
```

Endpoint: `POST /workshops/{workshop_id}/cancel`

Expected: `202 Accepted` with `status: "cancelled"` and `notification_jobs_created` equal to the number of registered members. Repeating cancellation is idempotent and does not create another set of jobs.

There is no endpoint listing a workshop's jobs. To obtain cancellation job IDs and verify their status without printing the database URL, first load `.env` in this terminal and set the workshop ID:

```powershell
$env:DEMO_WORKSHOP_ID = [string]$workshop.id
@'
import os
from sqlalchemy import select
from app.database import SessionLocal
from app.models import NotificationJob

with SessionLocal() as db:
    jobs = db.scalars(
        select(NotificationJob).where(
            NotificationJob.workshop_id == int(os.environ["DEMO_WORKSHOP_ID"]),
            NotificationJob.notification_type == "cancellation",
        )
    ).all()
    print(",".join(str(job.id) for job in jobs))
'@ | .\.venv\Scripts\python.exe -
```

For each returned ID, inspect `GET http://127.0.0.1:8000/notification-jobs/{id}`. Expected: one job per registered member with `notification_type: "cancellation"` and final `status: "succeeded"`. The workshop and registrations are marked cancelled in the same transaction that inserts these jobs.

## 6. Simulate notification failure, then recover by retry

This is a local-only failure simulation. It changes the mock service's environment, not the application code or real mail/SMS settings.

1. Stop the worker with `Ctrl+C`. In Terminal 2, stop the mock service with `Ctrl+C`.
2. In Terminal 2, load `.env` as above, then set failure mode and restart:

   ```powershell
   $env:MOCK_FAILURE_MODE = 'error'
   .\.venv\Scripts\python.exe -m uvicorn app.mock_notification:app --port 8001
   ```

3. In Terminal 3, load `.env`, increase the polling interval for a visible retry window, and restart the worker:

   ```powershell
   $env:WORKER_POLL_SECONDS = '10'
   .\.venv\Scripts\python.exe -m app.worker
   ```

4. Create a fresh member and workshop using section 3, then register as in section 4. The API still returns `201`; the worker's POST to the mock gets simulated `503 Service Unavailable`.
5. Inspect the returned confirmation job with `GET /notification-jobs/{notification_job_id}` before the next retry. Expected: `status: "pending"`, `attempts: 1`, and non-empty `last_error`. The first backoff is one second.
6. In Terminal 2, stop the failing mock with `Ctrl+C`, load `.env`, set success mode, and restart it:

   ```powershell
   $env:MOCK_FAILURE_MODE = 'success'
   .\.venv\Scripts\python.exe -m uvicorn app.mock_notification:app --port 8001
   ```

7. Leave the worker running. After its next polling interval, query the same job again. Expected: `status: "succeeded"`, `attempts: 2`, and `last_error: null`. Show both the 503/error and subsequent 200/success worker/mock logs.

The worker makes at most three attempts. Backoff is `min(2 ** (attempt_number - 1), 60)` seconds: one second before attempt 2, two seconds before attempt 3, then jobs become `failed`. Keep `WORKER_POLL_SECONDS` at its default during Streams mode; the 10-second setting is useful only if demonstrating PostgreSQL fallback and may slow Redis reconnect polling.

## 7. Demonstrate recovery after a worker restart

1. Stop only the worker with `Ctrl+C`; leave the API and success-mode mock running.
2. Create a fresh member/workshop and register. The response includes a confirmation job ID.
3. Query that ID while the worker is stopped. Expected: `status: "pending"`, `attempts: 0`; the job exists in PostgreSQL.
4. Restart the worker with `.\.venv\Scripts\python.exe -m app.worker`.
5. Query the same job until it reports `status: "succeeded"`, `attempts: 1`.

This verifies pending-job recovery after worker restart. Separately, the startup recovery code resets `processing` jobs older than five minutes to `pending` if retries remain; the automated `test_stale_processing_job_is_recovered_after_restart` verifies that stale-claim transition. A live hard-kill during an in-flight HTTP request is not required for this demonstration.

## Redis integration checks

With the Compose container healthy and `.env` loaded, set the URL explicitly for the test process if needed:

```powershell
$env:REDIS_URL = 'redis://127.0.0.1:16380/0'
.\.venv\Scripts\python.exe -m pytest -q tests\test_redis_streams_integration.py
```

The real-server tests create uniquely named temporary streams and consumer groups, verify group creation, publish/consume/acknowledge jobs, retry failures, reclaim a crashed consumer's pending entry with `XAUTOCLAIM`, handle duplicate stream entries, and reconcile a simulated Redis publish outage. Each test deletes only its own temporary stream. The existing fake Redis unit test is separate and does not prove server integration.

## What the tests cover

Run `.\.venv\Scripts\python.exe -m pytest -q` with `REDIS_URL` set and the Compose container running for the full suite. Without a reachable URL, live Redis tests skip; SQLite API tests and fake-client unit tests still run.

## Reliability explanation for the evaluator

- **Lost notification intent:** Registration plus confirmation job, and cancellation state plus per-member cancellation jobs, are committed to PostgreSQL together. PostgreSQL is the durable source of truth and the job table serves as the outbox. Redis publication is a separate relay operation: a failed XADD leaves `redis_published_at` unset, so later reconciliation can retry without losing the committed job. Database durability/backups remain operational responsibilities.
- **Retries:** HTTP/network/timeout failures are recorded in `delivery_attempts`; retryable jobs return to `pending` with exponential delays (1s, 2s, capped at 60s). The third failed attempt marks the job `failed`; the worker does not retry it automatically afterward.
- **Registration duplicates:** A unique constraint on `(workshop_id, member_id)` enforces one registration per member/workshop even under concurrent requests. The API converts the conflict to HTTP 409. A second uniqueness constraint prevents duplicate notification types per registration.
- **Notification duplicate delivery:** The worker sends a stable `Idempotency-Key`, and the mock service suppresses repeats held in its in-memory set. That set is process-local and resets when the mock restarts. A crash after remote acceptance but before the database success update can therefore cause another delivery attempt. The system is not exactly-once; it uses retryable/at-least-once processing and only provides mock-process-lifetime deduplication. It does not promise at-most-once external effects.
- **Worker restart:** Pending jobs remain in PostgreSQL and are republished if not marked published. Stream entries left pending by a crashed consumer are reclaimable with `XAUTOCLAIM`; stale PostgreSQL `processing` claims are reset after five minutes at startup and periodically. The real Redis test verifies pending-entry reclaim; the API demo can verify pending-job processing after restart.
- **Cancellation:** Cancellation updates the workshop and registrations and creates one durable notification job for each registered member in the same transaction. Each job is processed independently.
- **Redis Streams:** The project Compose service runs Redis 7.4.2 with AOF and a named volume, exposed only on localhost port 16380. PostgreSQL remains canonical; the worker relays pending rows with no `redis_published_at`. A failed XADD leaves that marker unset and is retried by reconciliation. The worker only acknowledges a stream entry after PostgreSQL contains a durable resulting state. If Redis is unavailable, PostgreSQL polling remains active and Redis is retried every five seconds. Exactly-once external effects remain impossible to guarantee if a notification succeeds but the worker crashes before recording success; the stable key helps only when the recipient service durably honors idempotency.
