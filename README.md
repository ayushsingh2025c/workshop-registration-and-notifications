# Workshop Registration Backend

FastAPI + SQLAlchemy service with PostgreSQL as the durable source of truth. A registration and its confirmation job are committed atomically. A separate worker delivers pending jobs to a mock HTTP service; notification delivery never runs in the registration request.

## Setup

Python 3.11+ is recommended. Activate the existing virtual environment on Windows:

```powershell
.\.venv\Scripts\Activate.ps1
```

Set `DATABASE_URL` in `.env` to a PostgreSQL database you can access (replace the username/password placeholders in `.env.example`). Set `REDIS_URL=redis://127.0.0.1:16380/0` to use the local Redis Compose service. If `.env` already exists, update only the `REDIS_URL` line; do not overwrite other local settings. The service creates missing PostgreSQL tables at startup with SQLAlchemy; it does not create the database or drop existing data. `.env` is ignored by Git. Environment variables are not loaded from the file automatically, so load it in each PowerShell terminal before starting a process:

```powershell
$envFile = Get-Content .\.env
foreach ($line in $envFile) {
    if ($line -match '^\s*([^#=]+)=(.*)$') {
        Set-Item -Path ("Env:" + $Matches[1].Trim()) -Value $Matches[2].Trim().Trim('"').Trim("'")
    }
}
```

Install only if dependencies are missing:

```powershell
pip install -r requirements.txt
```

## Redis Streams

The project provides a real Redis server for local development. It is pinned to Redis 7.4.2, bound only to `127.0.0.1:16380`, and uses a named volume with AOF persistence. Start/check it from the project folder:

```powershell
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" compose -f compose.redis.yml up -d
& "C:\Program Files\Docker\Docker\resources\bin\docker.exe" compose -f compose.redis.yml ps
```

If Docker is on `PATH`, `docker compose -f compose.redis.yml ...` is equivalent. The worker uses `REDIS_URL` to create the `notification-jobs` stream consumer group. PostgreSQL remains authoritative: the worker periodically relays due pending jobs whose `redis_published_at` is null. A failed Redis publish leaves the database row unpublished and the relay retries after reconnect. If Redis is unavailable, the worker processes due jobs by PostgreSQL polling and retries Redis connection every five seconds.

## Run

In separate terminals, from the project folder:

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload --port 8000
.\.venv\Scripts\python.exe -m uvicorn app.mock_notification:app --port 8001
.\.venv\Scripts\python.exe -m app.worker
```

Open `http://127.0.0.1:8000/docs`. Create a member (`POST /members`) and workshop (`POST /workshops`), then register with `POST /registrations`. Check delivery using `GET /notification-jobs/{job_id}`. Cancel with `POST /workshops/{workshop_id}/cancel`.

Set `MOCK_FAILURE_MODE=error` or `timeout` for mock-service failure simulation. The worker retries up to three times with exponential backoff, then records `failed`. Redis consumer groups distribute job IDs; the worker acknowledges only after the corresponding database state (success, terminal failure, or scheduled retry) is committed. It uses `XAUTOCLAIM` to recover idle pending stream entries and PostgreSQL startup/periodic recovery to reset stale processing claims. The outbox relay republishes due retry jobs. Delivery is at-least-once: stable idempotency keys are sent, but the mock's deduplication is process-local, so exactly-once external effects are not guaranteed.

For the complete PowerShell walkthrough of registration, duplicate rejection, notification failure/retry, cancellation fan-out, and worker-restart recovery, see [DEMO_GUIDE.md](DEMO_GUIDE.md). It also documents Redis Streams recovery and the PostgreSQL fallback.

## Tests

```powershell
$env:REDIS_URL = 'redis://127.0.0.1:16380/0'
.\.venv\Scripts\python.exe -m pytest -q
```

The Redis integration tests connect to the real Compose server when `REDIS_URL` is set and reachable; otherwise they skip. Unit/API tests use SQLite and mocked Redis where appropriate.
