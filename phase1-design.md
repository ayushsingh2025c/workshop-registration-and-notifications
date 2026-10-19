# Phase 1 Design: Workshop Registration and Notification Service

This document captures the Phase 1 design required by `project.md` and aligns with the selected stack in `techstack.md`.

## 1. Goal

Build a backend service that:

- lets organizers create workshops
- lets members register for workshops
- guarantees each successful registration creates a durable confirmation notification job
- returns registration responses without waiting for notification delivery
- handles notifications asynchronously, with retries and restart recovery
- supports workshop cancellation fan-out to all registered members

## 2. Clarified design decisions

### 2.1 Member creation and identity

Members are a first-class domain entity and must exist before a registration can be created. The API should provide a separate member lifecycle, for example:

- `POST /members` to create a member record
- `GET /members/{member_id}` to read a member

The registration endpoint should accept only an existing `member_id` and must reject unknown member references with `404 Not Found`.

Why this is safer:

- prevents orphaned registrations
- keeps member identity and email uniqueness explicit
- allows a member to be reused across many workshops without silently creating duplicates
- makes auditing and idempotency simpler

If a member email is unique and the user wants an "upsert" flow, the API can still do a controlled upsert at the member boundary, but the registration endpoint itself should not auto-create members.

### 2.2 Outbox and notification job responsibilities

PostgreSQL is the durable source of truth. Redis Streams is an operational transport layer, not the system of record.

The service should use a single durable `notification_jobs` table as the authoritative job queue. A separate DB outbox table is optional but not required. The practical design is:

- registration and confirmation job are inserted in the same database transaction
- a worker or dispatcher publishes job IDs to Redis Streams for processing
- the database record remains the canonical job record used for recovery and audit

A job row should include fields such as:

- `id`
- `workshop_id`
- `member_id`
- `registration_id`
- `notification_type` (`confirmation`, `cancellation`)
- `status` (`pending`, `processing`, `succeeded`, `failed`, `cancelled`)
- `attempts`
- `max_attempts`
- `next_retry_at`
- `payload`
- `idempotency_key`
- `last_error`
- `created_at`
- `updated_at`
- `completed_at`
- `published_to_redis_at` (optional)
- `redis_stream_message_id` (optional)

This keeps job state durable even if Redis is unavailable or the worker restarts.

### 2.3 Worker crash recovery and recovery strategy

The worker must never rely on in-memory job state. On startup, it should:

1. load pending or retryable jobs from PostgreSQL:
   - `status IN ('pending', 'processing')`
   - `next_retry_at <= now()`
   - and `attempts < max_attempts`
2. re-queue them to Redis Streams if they are not already visible there
3. process jobs in a bounded loop
4. when a worker crashes after claiming a job but before the HTTP call completes, the job stays in `processing` status and is recovered on the next startup by a reconciliation step

A robust reconciliation rule is:

- jobs with `status = 'processing'` and no heartbeat/completion evidence are treated as recoverable and reset to `pending` after a timeout window
- Redis consumer groups should be used for stream ownership, but PostgreSQL remains the recovery source of truth

This ensures pending work survives a backend restart without silently disappearing.

### 2.4 Retry exhaustion and permanent failure handling

Retries should be bounded and explicit:

- retryable failures: 429, 5xx, network errors, timeouts
- permanent failures: invalid payload, validation rejections, unsupported notification type

Rules:

- increment `attempts` after each attempt
- set `next_retry_at` using exponential backoff, e.g. 1s, 2s, 4s, 8s, capped at a maximum delay
- if attempts exceed `max_attempts`, mark the job `failed` and stop retrying
- record the last error and final status in PostgreSQL

This avoids infinite retry loops and preserves a clear audit trail.

### 2.5 Cancellation consistency and idempotence

Workshop cancellation must generate one job per registered member, and the cancel action itself must be durable.

Suggested process:

1. validate workshop exists and is not already cancelled
2. in one database transaction:
   - set workshop status to `cancelled`
   - set all matching registrations to `cancelled`
   - insert one `notification_jobs` row per registered member with `notification_type = 'cancellation'`
3. commit
4. return success immediately

Design constraints:

- if the cancellation request is repeated, the service should be idempotent and return the same result rather than creating duplicate jobs
- a cancellation job must be associated with a specific member and workshop, and each job is independent
- if a cancellation request comes in during a worker retry of a confirmation job, both jobs can coexist because they have different `notification_type` values and distinct `idempotency_key`s

## 3. Proposed architecture

```text
Client
  |
  v
FastAPI API
  |
  +--> PostgreSQL (source of truth)
  |      - workshops
  |      - members
  |      - registrations
  |      - notification_jobs
  |      - delivery_attempts
  |
  +--> Redis Streams (delivery transport)
         |
         v
      Background worker
         |
         v
      Mock notification service
         |
         v
      Update delivery status in PostgreSQL
```

Key rule: PostgreSQL is the source of truth for registration intent and notification jobs. Redis is used to move work between processes; it is not the only durable record.

## 4. Data model

### Workshop
- id: UUID or big integer primary key
- title: string
- description: string, optional
- organizer_id: string or UUID, optional for later use
- status: active | cancelled
- created_at: timestamp
- updated_at: timestamp

### Member
- id: UUID or big integer primary key
- email: unique string
- name: string
- created_at: timestamp

### Registration
- id: UUID or big integer primary key
- workshop_id: foreign key to workshops.id
- member_id: foreign key to members.id
- status: registered | cancelled
- created_at: timestamp

Unique constraint:
- unique(workshop_id, member_id)

### NotificationJob
- id: UUID or big integer primary key
- notification_type: confirmation | cancellation
- workshop_id: foreign key to workshops.id
- member_id: foreign key to members.id
- registration_id: nullable foreign key to registrations.id
- status: pending | processing | succeeded | failed | cancelled
- attempts: integer
- max_attempts: integer
- next_retry_at: timestamp
- payload: JSON payload for the mock service
- idempotency_key: stable string
- last_error: text, optional
- created_at: timestamp
- updated_at: timestamp
- completed_at: timestamp, optional
- published_to_redis_at: timestamp, optional
- redis_stream_message_id: text, optional

### DeliveryAttempt
- id: UUID or big integer primary key
- notification_job_id: foreign key to notification_jobs.id
- attempt_number: integer
- started_at: timestamp
- finished_at: timestamp, optional
- http_status: integer, optional
- outcome: success | retryable_error | permanent_error | timeout
- error_detail: text, optional

## 5. API design

### POST /members
Request body:
```json
{
  "email": "member@example.com",
  "name": "Jane Member"
}
```

Response:
- 201 Created
- returns member details

Error responses:
- 409 Conflict if email already exists
- 422 Unprocessable Entity for invalid payload

### POST /workshops
Request body:
```json
{
  "title": "FastAPI Deep Dive",
  "description": "A backend and reliability workshop",
  "organizer_id": "org-123"
}
```

Success response:
- 201 Created
- JSON with workshop details

### GET /workshops/{workshop_id}
Success response:
- 200 OK
- JSON with workshop metadata and registration count

### POST /registrations
Request body:
```json
{
  "workshop_id": "<uuid>",
  "member_id": "<uuid>"
}
```

Success response:
- 201 Created
- JSON with registration result

Error responses:
- 404 Not Found if workshop or member is missing
- 409 Conflict if duplicate registration is attempted
- 422 Unprocessable Entity if payload is invalid

Behavior:
- create registration and confirmation job in one database transaction
- return immediately after commit
- do not wait for notification delivery

### POST /workshops/{workshop_id}/cancel
Success response:
- 202 Accepted
- includes cancellation summary and job count

Behavior:
- require the workshop to exist
- if already cancelled, return idempotent success with current state
- create one cancellation job per member registration in one transaction
- save cancellation state and jobs atomically

## 6. Duplicate registration prevention

Duplicate handling is enforced in both layers:

1. unique constraint on `(workshop_id, member_id)`
2. API catches DB uniqueness errors and returns `409 Conflict`
3. worker processing remains idempotent using stable `idempotency_key`
4. mock notification service accepts the same idempotency key and ignores duplicate delivery attempts

## 7. Async workflow

When a registration succeeds:

1. API begins a transaction
2. insert registration row
3. insert confirmation notification job row with `status = 'pending'`
4. commit transaction
5. API returns 201 immediately
6. dispatcher publishes a message to Redis Streams for newly pending jobs
7. worker consumes job message and calls mock notification service
8. worker updates PostgreSQL job status, attempts, and retry timing

The job row remains the durable source of truth even if Redis delivery is delayed or the worker restarts.

## 8. Retry, timeout, and idempotency rules

- Retryable conditions:
  - HTTP 429
  - HTTP 500-599
  - network connection failure
  - request timeout
- Permanent failures:
  - invalid payload rejected by the notification service
  - unsupported notification type
- Use bounded exponential backoff, e.g. 1s, 2s, 4s, 8s, capped at a max retry window
- Track `attempts`, `next_retry_at`, and `last_error` in PostgreSQL
- Use stable `idempotency_key` values to make duplicate retries safe
- After `attempts >= max_attempts`, mark job `failed` and stop retrying

## 9. Failure scenarios and recovery rules

### 1. Backend restart during pending delivery
- registration is already committed in PostgreSQL
- notification job remains durable in PostgreSQL
- worker startup reconciles `pending`/`processing` jobs and re-enqueues missing work

### 2. Worker crash after claiming a job
- job may remain `processing` without completion
- startup reconciliation resets stale `processing` jobs to `pending` after a timeout window
- the worker may retry safely with the same idempotency key

### 3. Timeout ambiguity
- HTTP timeout may mean the notification service processed the request before the timeout
- worker must retry using the same `idempotency_key` and the mock service must be idempotent

### 4. One failing notification must not block unrelated jobs
- each job is independently processed with a separate claim and retry window
- failures are isolated to the specific job record

### 5. Failed registration
- if the registration insert fails or the member/workshop reference is invalid, no job is created
- this prevents false confirmation notifications

## 10. Acceptance criteria for Phase 1

The design is complete when all of the following are true:

- members are explicitly modeled and created before registration
- workshop, member, registration, and notification job models are defined
- API endpoints and request/response shapes are documented
- duplicate registration prevention is enforced by a database uniqueness constraint
- a successful registration creates a durable confirmation job in the same transaction
- asynchronous processing does not block registration responses
- stale or crashed worker jobs are recoverable after restart
- retry exhaustion and cancellation semantics are explicit and consistent

## 11. Deliverables produced for Phase 1

- API and data-model outline
- architecture and recovery description
- failure and restart scenarios
- explicit rules for member creation, worker recovery, retry exhaustion, and cancellation consistency

This matches the Phase 1 completion criteria in `project.md` and the selected technology strategy in `techstack.md`.
