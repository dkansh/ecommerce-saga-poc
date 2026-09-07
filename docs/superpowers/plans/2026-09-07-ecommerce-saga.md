# Ecommerce Saga Implementation Plan

> Execute with test-driven development and task review. Use subagent-driven-development for independent participant and documentation tasks.

**Goal:** A runnable, minimal four-service comparison of saga orchestration and choreography with durable recovery.

**Architecture:** Order owns the checkout lifecycle. Each participant owns its data; Kafka and a transactional outbox connect local transactions.

**Tech Stack:** Java 26, Spring Boot 4.1.1, PostgreSQL 18.6, Kafka 4.3.1, Gradle 9.7.1.

**Spec:** [../../design.md](../../design.md)

## Global constraints

- Work in the new `ecommerce-saga-poc` Git repository; pin stable releases.
- No cross-service SQL, external payment charges, or unrelated ecommerce fields.
- One business transaction includes inbox and outbox; compensation is idempotent.
- Docker integration tests must fail if infrastructure is unavailable, not silently skip.

## Task 1: Wire contract, build and transactional delivery

- [x] Create root/module Gradle builds, Gradle Wrapper, common Message/MessageHandler/Outbox contracts.
- [x] Test envelope validation, duplicate handling, rollback and shared HTTP route binding.
- [x] Implement inbox processing, ordered outbox relay, persistent quarantine, retry and demo fault controls.
- [x] Verify with `./gradlew :messaging:test` and PostgreSQL integration tests.

## Task 2: Business participants

- [x] Write participant tests: insufficient stock, duplicate reserve/refund, cancellation before forward command.
- [x] Implement Inventory, Payment and Shipping modules with separate Flyway migrations.
- [x] Consume `MessageHandler.accepts(Message)` / `handle(Message)`; emit via `Outbox.add(Message)`.
- [x] Use `Faults.check(Message, Fault, boolean)` for deterministic transient/permanent demo failures.
- [x] Review SQL locking and exercise participants through actual infrastructure.

## Task 3: Order lifecycle

- [x] Test terminal-state protection, ordered compensation and idempotency conflicts.
- [x] Implement POST/GET orders, persisted coordinator, choreography observer and watchdog.
- [x] Add inspection, cancellation and compensation retry endpoints.
- [x] Verify both modes against literal expected terminal business states.

## Task 4: Infrastructure and executable scenarios

- [x] Add Compose, pinned Docker build, separate database roles, health checks, CI.
- [x] Run `./gradlew test integrationTest bootJar`, `docker compose up -d --build --wait` and scenario runner.
- [x] Check success, business failures, duplicate requests, transient failures, timeouts, failed refund recovery and overselling.
- [x] Run restart, broker outage and late-message exercises; record exact outcomes.

## Task 5: Documentation and final review

- [x] Document architecture, guarantees, trade-offs, API examples, Windows/Linux run steps and all failure exercises.
- [x] Independently review concurrency/recovery paths and fix findings.
- [x] Save verification evidence and commit project for the user's later GitHub push.
