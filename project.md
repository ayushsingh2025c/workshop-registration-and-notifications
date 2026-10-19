# Project Plan --- Workshop Registration and Notifications

## Project Goal

Build a backend service where organizers can create workshops and
members can register. Each successful registration must eventually
produce a confirmation notification, and cancelling a workshop must
eventually notify every registered member.

The service must respond to registration requests without waiting for
notification delivery. Notifications must be processed asynchronously,
retried after failures, and recoverable after backend restarts. All
notifications are simulated; no real email or SMS integration is
required.

## Phase 1 --- Requirements and Architecture

**Objective:** Turn the task requirements into a clear implementation
design.

### Work

-   Define the workshop, member, registration, and notification-job data
    models.
-   Define the API endpoints and expected success/error responses.
-   Decide how duplicate registrations will be prevented.
-   Design asynchronous notification processing and persistent job
    storage.
-   Define retry rules, timeout handling, idempotency, and job statuses.
-   Document the main failure scenarios, including a backend restart
    during pending delivery.

### Deliverables

-   API and data-model outline.
-   Architecture diagram or written architecture description.
-   Acceptance criteria for each requirement.

### Completion criteria

The design explains how every successful registration creates a durable
confirmation job and how pending jobs survive a restart.

## Phase 2 --- Project Setup and Persistent Data Layer

**Objective:** Establish the backend foundation and reliable storage.

### Work

-   Set up the project structure, configuration, environment variables,
    and run scripts.
-   Configure the database and migrations.
-   Implement persistence for workshops, members, registrations, and
    notification jobs.
-   Add database constraints for valid references and one registration
    per member per workshop.
-   Implement transaction handling for operations that must succeed or
    fail together.

### Deliverables

-   Runnable backend skeleton.
-   Database schema and migrations.
-   Basic persistence/repository layer.

### Completion criteria

The application starts reliably, the schema can be created from
migrations, and the database enforces registration uniqueness.

## Phase 3 --- Workshop and Registration APIs

**Objective:** Implement the core business functionality.

### Work

-   Implement workshop creation and workshop retrieval.
-   Implement member registration and validation.
-   Reject duplicate registrations and invalid workshop/member
    references.
-   Save a successful registration and its confirmation notification job
    in the **same database transaction**.
-   Return the registration result without waiting for the notification
    service.
-   Ensure failed registrations do not create confirmation jobs.

### Deliverables

-   Working workshop and registration endpoints.
-   Consistent response and error formats.
-   Tests for normal, invalid, and duplicate registration cases.

### Completion criteria

A successful registration is durable and immediately returns a result;
failed or duplicate registrations do not produce confirmation jobs.

## Phase 4 --- Notification Service and Background Processing

**Objective:** Deliver notifications asynchronously and handle temporary
failures.

### Work

-   Build the mock HTTP notification service.
-   Make the mock service configurable so it can succeed, return an
    error, or time out.
-   Implement a background worker that claims pending jobs and calls the
    mock service.
-   Track attempts and delivery status.
-   Retry retryable failures using bounded exponential backoff.
-   Prevent one failed job from blocking unrelated jobs.
-   Use stable notification identifiers/idempotency keys and handle
    ambiguous timeout outcomes as safely as possible.

### Deliverables

-   Mock notification HTTP service.
-   Background worker and retry mechanism.
-   Persistent notification status and attempt tracking.

### Completion criteria

The API remains independent of delivery latency, failed jobs are
retried, and the worker can continue processing other jobs when one
delivery fails.

## Phase 5 --- Cancellation, Recovery, and Reliability

**Objective:** Complete cancellation behavior and prove the system can
recover from failures.

### Work

-   Implement workshop cancellation.
-   Create a durable cancellation notification job for every registered
    member.
-   Ensure cancellation processing does not depend on all notifications
    succeeding immediately.
-   Resume pending or retryable jobs after a backend restart.
-   Handle worker interruption and safe job claiming so jobs are not
    silently lost.
-   Review duplicate-delivery risks, retry limits, and the behavior of
    permanently failing jobs.

### Deliverables

-   Workshop cancellation endpoint and notification flow.
-   Restart-recovery behavior.
-   Documented delivery guarantees and known limitations.

### Completion criteria

Every registered member has a cancellation notification job, and saved
pending work can resume after restarting the backend.

## Phase 6 --- Testing, Demonstration, and Submission

**Objective:** Validate the requirements and prepare a reproducible
submission.

### Work

-   Add unit and integration tests for the main business flows.
-   Test duplicate registration, failed registration, notification
    errors, timeouts, retries, and cancellation.
-   Restart the backend while notification jobs are pending and verify
    recovery.
-   Verify that one failing notification does not prevent other jobs
    from being processed.
-   Run the project from a clean setup using the documented
    instructions.
-   Prepare a concise demonstration and explain the reliability design.

### Deliverables

-   Automated test suite and test results.
-   Source code and database migrations.
-   README with prerequisites, configuration, setup, run, and test
    instructions.
-   Demonstration steps covering success, failure, and recovery.
-   Explanation of lost-notification prevention, retries, duplicate
    delivery, and backend restarts.

### Completion criteria

A reviewer can set up and run the project, reproduce the key failure
scenarios, and verify the required behavior from tests or the
demonstration.

## Cross-Phase Engineering Rules

-   **Durability:** Persist registrations and notification jobs; do not
    rely on in-memory queues as the only source of truth.
-   **Atomicity:** Commit a registration and its confirmation job in one
    database transaction.
-   **Asynchronous response:** Never wait for notification delivery
    before returning the registration result.
-   **Retry safety:** Use bounded retries, backoff, attempt tracking,
    and stable notification identifiers.
-   **Duplicate handling:** Enforce registration uniqueness in the
    database and make notification processing idempotent where possible.
-   **Failure isolation:** A single notification failure must not stop
    unrelated jobs.
-   **Observability:** Record useful logs and job statuses so failures
    and retries can be diagnosed.
-   **Scope control:** Use a mock notification service; real email/SMS
    integration is out of scope.

## Definition of Done

The project is complete when all of the following are true:

-   Organizers can create workshops.
-   Members can register only once per workshop.
-   Successful registration responses do not wait for notification
    delivery.
-   Every successful registration creates a durable confirmation job.
-   Failed registrations do not create confirmation notifications.
-   Notification errors and timeouts trigger retries.
-   Pending work survives backend restarts.
-   One failed notification does not block delivery attempts for others.
-   Workshop cancellation schedules notifications for all registered
    members.
-   Tests, source code, and clear run instructions are included.
-   The design's handling of retries, duplicate delivery, lost
    notifications, and restarts is explained.
