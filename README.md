# Workshop Registration Backend

FastAPI + SQLAlchemy service with PostgreSQL as the durable source of truth. A registration and its confirmation job are committed atomically. A separate worker delivers pending jobs to a mock HTTP service; notification delivery never runs in the registration request.

## Setup

Python 3.11+ is recommended. Activate the existing virtual environment on Windows:

```powershell
.\.venv\Scripts\Activate.ps1
```

Set `DATABASE_URL` in `.env` to a PostgreSQL database you can access (replace the username/password placeholders in `.env.example`). The service creates missing tables at startup with SQLAlchemy; it does not drop or reset existing tables. `.env` is ignored by Git. Environment variables are not loaded from the file automatically, so load it in each PowerShell terminal before starting a process:

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

## Run

In separate terminals, from the project folder:

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload --port 8000
.\.venv\Scripts\python.exe -m uvicorn app.mock_notification:app --port 8001
.\.venv\Scripts\python.exe -m app.worker
```

Open `http://127.0.0.1:8000/docs`. Create a member (`POST /members`) and workshop (`POST /workshops`), then register with `POST /registrations`. Check delivery using `GET /notification-jobs/{job_id}`. Cancel with `POST /workshops/{workshop_id}/cancel`.

Set `MOCK_FAILURE_MODE=error` or `timeout` for mock-service failure simulation. The worker retries up to three times with exponential backoff, then records `failed`. Pending jobs remain in PostgreSQL and are polled after restart; stale processing claims are recovered after five minutes. When Redis Streams is available, the worker dispatches durable pending job IDs to a consumer group and acknowledges messages only after attempting the database-backed job. PostgreSQL reconciliation and direct polling remain the recovery path if Redis is unavailable. Delivery is at-least-once; the mock service deduplicates idempotency keys for its process lifetime.

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```
