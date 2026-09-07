# Ecommerce Saga Implementation Plan

> Execute with test-driven development and task review. Use subagent-driven-development for independent participant and documentation tasks.

**Goal:** A runnable, minimal four-service comparison of saga orchestration and choreography with durable recovery.

**Architecture:** Order owns the checkout lifecycle. Each participant owns its data; Kafka and a transactional outbox connect local transactions.

**Tech Stack:** Java 26, Spring Boot 4.1.1, PostgreSQL 18.6, Kafka 4.3.1, Maven 3.9.16.

**Spec:** [../../design.md](../../design.md)

## Global constraints

- Work in the new `ecommerce-saga-poc` Git repository; pin stable releases.
- No cross-service SQL, external payment charges, or unrelated ecommerce fields.
- One business transaction includes inbox and outbox; compensation is idempotent.
- Docker integration tests must fail if infrastructure is unavailable, not silently skip.

## Task 1: Wire contract, build and transactional delivery

- [ ] Create root/module POMs, Maven wrapper, common Message/MessageHandler/Outbox contracts.
- [ ] Write tests for envelope validation, duplicate handling and rollback before implementation.
- [ ] Implement inbox processing, ordered outbox relay, persistent quarantine, retry and demo fault controls.
- [ ] Verify with `./mvnw -pl messaging -am test` and PostgreSQL integration tests.

## Task 2: Business participants

- [ ] Write participant tests: insufficient stock, duplicate reserve/refund, cancellation before forward command.
- [ ] Implement Inventory, Payment and Shipping modules with separate Flyway migrations.
- [ ] Consume `MessageHandler.accepts(Message)` / `handle(Message)`; emit via `Outbox.add(Message)`.
- [ ] Use `Faults.check(Message, Fault, boolean)` for deterministic transient/permanent demo failures.
- [ ] Review SQL locking and exercise participants through actual infrastructure.

## Task 3: Order lifecycle

- [ ] Test terminal-state protection, ordered compensation and idempotency conflicts first.
- [ ] Implement POST/GET orders, persisted coordinator, choreography observer and watchdog.
- [ ] Add inspection, cancellation and compensation retry endpoints.
- [ ] Verify both modes against literal expected terminal business states.

## Task 4: Infrastructure and executable scenarios

- [ ] Add Compose, pinned Docker build, separate database roles, health checks, CI.
- [ ] Run `./mvnw verify`, `docker compose up -d --build --wait` and scenario runner.
- [ ] Check success, business failures, duplicate requests, transient failures, timeouts, failed refund recovery and overselling.
- [ ] Run restart, broker outage and late-message exercises; record exact outcomes.

## Task 5: Documentation and final review

- [ ] Document architecture, guarantees, trade-offs, API examples, Windows/Linux run steps and all failure exercises.
- [ ] Independently review concurrency/recovery paths and fix findings.
- [ ] Save verification evidence and commit project for the user's later GitHub push.
