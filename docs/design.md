# Ecommerce saga design

Build four independently running Spring Boot services: Order (8080), Inventory
(8081), Payment (8082), Shipping (8083). A Maven reactor contains these and a
small messaging module. Each service owns a database and login. One PostgreSQL
server and one Kafka KRaft broker keep local resource usage modest.

## Flow and alternatives

Reserve stock, charge payment, book shipment, confirm order. Compare orchestration
(Order issues explicit commands from persisted state) with choreography (each
participant reacts to the previous participant's fact). Both modes run in the
same binaries; each order selects its mode. Orchestration alone is simpler to
operate; choreography alone removes forward-flow coordination but makes global
recovery harder. Implementing both exposes that trade-off directly.

For orchestration, cancellation runs shipping -> payment -> inventory, including
no-op cancellations for steps that never ran. For choreography, CANCEL_SAGA fans
out to participants; Order observes all three acknowledgements before reporting
CANCELLED. This is a deliberately coordinated abort protocol, not a claim that
timeouts magically resolve in a fully decentralized system.

## Correctness

Local state, inbox deduplication and an outbox record commit in one DB transaction.
An ordered polling relay sends Kafka records with sagaId as key. Delivery is at
least once; effects are idempotent, not distributed exactly once. Cancellation
writes a permanent tombstone even when no forward work exists. Late forward work
cannot reverse cancellation. Conditional stock updates prevent overselling.
Payment/shipping are local simulated providers; real external providers require
their own idempotency keys and reconciliation.

Orders persist deadlines; a watchdog cancels overdue work after restart. Stuck
compensation becomes MANUAL_INTERVENTION, never falsely CANCELLED. Transient
consumer failures retry with bounded backoff; exhausted records are quarantined
durably and copied to a DLT. Operator replay and compensation retry are explicit.
Duplicate HTTP idempotency keys return the original order; changed payload is 409.

## Scope and validation

Essential fields: one SKU, quantity, total amount in integer minor units, mode,
fault scenario, IDs and state. No catalog, basket, customer, address, real card data,
gateway, registry, Cassandra or Kubernetes. Include health/metrics, Flyway,
structured correlation logs, Docker Compose, Maven wrapper, CI, unit and actual
PostgreSQL/Kafka tests, and a detailed Markdown runbook with sequence diagrams.

Demonstrate both modes: success, no stock, payment decline, shipping rejection,
duplicate requests/events, transient failure, unavailable participant, timeout,
late success after cancellation, failed refund and recovery, restart, Kafka outage,
poison record quarantine/replay and concurrent stock contention. Never substitute
an H2 test for PostgreSQL locking verification.
