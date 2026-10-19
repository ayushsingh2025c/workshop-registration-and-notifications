# Recommended Backend Tech Stack

## 1. Decision Summary

For the GDG Tech Wildcard Entry task, use **Python + FastAPI +
PostgreSQL**, with a separate Python background worker and **Redis
Streams** for asynchronous notification processing.

This stack is a good fit because the assignment emphasizes reliable
registration, fast API responses, retryable notifications, cancellation
fan-out, and recovery after backend restarts. It is also practical to
build, test, and demonstrate within a student project.

## 2. Selected Stack

  -----------------------------------------------------------------------
  Layer                   Technology              Purpose
  ----------------------- ----------------------- -----------------------
  Language                Python                  Main implementation
                                                  language for the API,
                                                  worker, and mock
                                                  service

  HTTP API                FastAPI                 Workshop, registration,
                                                  and cancellation
                                                  endpoints

  ASGI server             Uvicorn                 Runs the FastAPI
                                                  application

  Request/response        Pydantic                Validates input and
  validation                                      defines response
                                                  schemas

  Primary database        PostgreSQL              Durable storage for
                                                  workshops, members,
                                                  registrations,
                                                  notification jobs, and
                                                  delivery attempts

  ORM                     SQLAlchemy              Database access and
                                                  transaction management

  Schema migrations       Alembic                 Versioned database
                                                  schema changes

  Async job transport     Redis Streams           Carries notification
                                                  jobs to workers and
                                                  supports consumer
                                                  groups/pending-entry
                                                  recovery

  Background processing   Python worker           Sends notifications,
                                                  records results,
                                                  retries failures, and
                                                  resumes pending jobs

  Mock notification       FastAPI endpoint or a   Simulates success, HTTP
  service                 small separate FastAPI  errors, and timeouts
                          app                     

  Testing                 pytest + FastAPI        Unit and integration
                          TestClient/httpx        tests for API and
                                                  failure scenarios

  Load testing            Locust                  Checks API
                                                  responsiveness under
                                                  concurrent registration
                                                  requests

  Local environment       Docker Compose          Runs PostgreSQL, Redis,
                                                  API, worker, and mock
                                                  service consistently

  Version control         Git + GitHub            Tracks changes and
                                                  supports
                                                  submission/review
  -----------------------------------------------------------------------

Keep the API and worker in the same repository and share the data models
and service logic, but run them as **separate processes**. This keeps
the project simple without making notification delivery part of the HTTP
request path.

## 3. Why This Stack

### Python

-   The syntax is readable, so more time can go into reliability logic
    instead of language overhead.
-   Python works well for HTTP APIs, database access, background
    workers, and automated tests.
-   FastAPI, SQLAlchemy, pytest, and Redis clients have mature
    ecosystems.

**Why not Java or Node.js?** Both can solve this task well. Java with
Spring Boot is robust but brings more framework and configuration
overhead for a compact assignment. Node.js with Express or NestJS is
also suitable, but Python is the selected option for a straightforward
API/worker implementation. This is a project-fit decision, not a claim
that Python is universally better.

### FastAPI

-   Provides typed request and response models through Pydantic.
-   Generates interactive OpenAPI documentation automatically, useful
    during demonstration.
-   Makes it straightforward to implement and test REST endpoints.

**Why not Django REST Framework?** Django REST Framework is strong when
the project needs Django's broader batteries-included features, admin
site, and conventions. This task has a small API surface and a separate
worker, so FastAPI is a leaner fit.

**Why not Flask?** Flask can handle the task, but request validation,
API schemas, and generated documentation require more manual assembly or
additional extensions.

### PostgreSQL

-   Provides durable relational storage and transactions.
-   Enforces uniqueness for `(workshop_id, member_id)`, preventing
    duplicate registrations even when concurrent requests arrive.
-   Supports foreign keys and constraints to protect data integrity.
-   Can store the notification outbox/job records and delivery attempts
    so work is not held only in memory.

**Why not SQLite?** SQLite is convenient for quick prototypes, but
PostgreSQL better represents a multi-process backend with concurrent API
requests and workers. PostgreSQL is the stronger default for
demonstrating production-style transaction behavior.

**Why not MongoDB?** The core entities have clear relationships and
important transactional constraints. A relational database makes those
relationships, uniqueness rules, and atomic registration/job creation
easier to express.

### SQLAlchemy + Alembic

SQLAlchemy centralizes database access and transaction handling. Alembic
keeps schema changes reproducible as the project evolves.

**Why not raw SQL everywhere?** Raw SQL is useful for specific queries,
but using it for every operation can scatter query and transaction logic
throughout the application. SQLAlchemy offers a maintainable default
while still allowing explicit SQL where needed.

### Redis Streams + Python Worker

Redis Streams provides a queue-like stream, consumer groups, and
pending-entry tracking. The worker can process jobs separately from the
API and acknowledge them after processing.

**Why not run notifications inside the API request?** A slow or
unavailable notification service would increase registration latency and
could make registration appear to fail even after the registration was
saved.

**Why not use only an in-memory queue?** In-memory jobs disappear when
the process exits, so they cannot meet the restart-recovery requirement.

**Why not RabbitMQ or Kafka?** - RabbitMQ is a strong message broker,
but it adds broker concepts and setup that are not necessary for this
project's scale. - Kafka is designed for high-throughput event streaming
and durable distributed event pipelines; it is excessive for this
assignment. - Redis Streams provides enough queue/consumer functionality
for a focused demonstration, provided persistence and recovery are
configured correctly.

**Important reliability rule:** PostgreSQL is the source of truth for
registrations and notification intent. Create the registration and its
durable outbox/job record in the same PostgreSQL transaction. A
dispatcher can publish outbox entries to Redis Streams and mark them
published. This avoids the dual-write failure where the database commit
succeeds but publishing to Redis fails. The worker should also record
delivery state in PostgreSQL.

Redis persistence should be enabled in the local setup, but Redis must
not be the only record that a notification is owed. On startup or during
periodic reconciliation, the dispatcher should republish durable outbox
entries that were not successfully published. Use stable job
IDs/idempotency keys because retries and ambiguous timeouts can still
lead to duplicate attempts.

### pytest, Locust, and Docker Compose

-   **pytest** verifies business rules and failure handling.
-   **Locust** measures whether registration responses remain responsive
    while workers process notifications.
-   **Docker Compose** makes it straightforward to start all required
    services with consistent configuration.

These tools directly support the assignment's demonstration and
submission requirements.

## 4. Proposed Architecture

``` text
Client / Organizer
        |
        v
FastAPI + Uvicorn
        |
        v
PostgreSQL transaction
  - registration
  - durable notification outbox/job
        |
        v
Outbox dispatcher -----> Redis Streams
                              |
                              v
                       Python worker
                              |
                              v
                  Mock HTTP notification service
                              |
                              v
                 Update delivery state in PostgreSQL
```

Workshop cancellation should create a durable cancellation notification
job for every registered member. Each recipient's job should be
processed independently so a failure for one recipient does not block
others.

## 5. Reliability Requirements the Implementation Must Enforce

1.  **Atomic registration:** Save a registration and its confirmation
    outbox record in one PostgreSQL transaction.
2.  **No false confirmation:** Do not create a confirmation job if
    registration fails or is rejected as a duplicate.
3.  **Fast API response:** Return after the database transaction
    commits; do not wait for notification delivery.
4.  **Durable intent:** PostgreSQL stores every notification that must
    be sent, even if Redis or a worker is temporarily unavailable.
5.  **Retry policy:** Retry retryable HTTP errors and timeouts with
    bounded exponential backoff and attempt tracking.
6.  **Independent jobs:** Process each recipient's notification
    separately.
7.  **Restart recovery:** Reclaim pending Redis entries and reconcile
    unpublished PostgreSQL outbox records after restart.
8.  **Duplicate protection:** Use stable notification IDs/idempotency
    keys and make the mock service idempotent. Record delivery state
    carefully.
9.  **Honest delivery guarantee:** A timeout may occur after the mock
    service has accepted a notification. Therefore, exactly-once
    delivery cannot be assumed; design for at-least-once attempts and
    idempotent effects.
10. **Observability:** Log job ID, notification type, attempt number,
    outcome, and failure reason without logging unnecessary personal
    data.

## 6. Scope and Implementation Order

Build in this order:

1.  Project skeleton, configuration, Docker Compose, PostgreSQL, and
    migrations.
2.  Workshop and registration APIs with database constraints.
3.  PostgreSQL outbox and atomic registration/job creation.
4.  Mock HTTP notification service.
5.  Redis Streams dispatcher and background worker.
6.  Retry, timeout, idempotency, and restart recovery.
7.  Workshop cancellation and per-member notification jobs.
8.  Automated tests, load test, run instructions, and demonstration.

## 7. Definition of Done

The stack is correctly implemented when:

-   Duplicate registrations are rejected by a database constraint.
-   Successful registrations return without waiting for notification
    delivery.
-   Every successful registration has a durable confirmation job.
-   Failed registrations do not produce confirmations.
-   Notification errors and timeouts are retried.
-   Restarting the API or worker does not silently lose pending
    notification intent.
-   One failed recipient does not block other notifications.
-   Cancellation creates jobs for all registered members.
-   Tests cover success, failure, retries, duplicates, cancellation, and
    restart recovery.
-   A reviewer can start the services and run the tests using the
    documented commands.
