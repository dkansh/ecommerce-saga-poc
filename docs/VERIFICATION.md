# Verification record

Verified locally on 2026-09-07 using Windows, JDK 26.0.2, Gradle 9.7.1,
Docker Desktop Linux containers, PostgreSQL 18.6 and Kafka 4.3.1.

## Build and database tests

`gradlew test integrationTest bootJar --no-daemon --console=plain` completed successfully.
All four executable `app.jar` files were built. Docker then built all four runtime
images from source using the Gradle Wrapper; every service passed its health check.

| Test suite | Tests | Failures / errors / skipped |
|---|---:|---|
| Message validation | 2 | 0 / 0 / 0 |
| Inbox, outbox, fault retry and operator HTTP route | 6 | 0 / 0 / 0 |
| Order database lifecycle and concurrent idempotency | 5 | 0 / 0 / 0 |
| Saga state machine | 6 | 0 / 0 / 0 |
| Inventory transactions | 4 | 0 / 0 / 0 |
| Payment transactions | 4 | 0 / 0 / 0 |
| Payment demo configuration | 1 | 0 / 0 / 0 |
| Shipping transactions | 4 | 0 / 0 / 0 |
| Shipping demo configuration | 1 | 0 / 0 / 0 |
| **Total** | **33** | **0 / 0 / 0** |

There are 10 unit tests and 23 integration tests. Integration tests use PostgreSQL,
Flyway migrations, random test schemas and actual Spring transaction proxies.
No H2 substitution or silently skipped database test was used.

Machine-readable counts: [tests.json](../evidence/tests.json).

## Live cross-service scenarios

`node scripts/scenarios.mjs` passed **24 checks in 166.77 seconds**. It tested:

- service health
- invalid quantity in both modes
- successful checkout, repeated HTTP key and conflicting key reuse in both modes
- sold-out inventory, declined payment and rejected shipping in both modes
- transient payment retry in both modes
- explicit cancellation in both modes
- duplicate message ID and cancellation-before-forward delivery in both modes
- unavailable payment, durable quarantine, timeout, unblock and safe replay in both modes
- failed refund, MANUAL_INTERVENTION and compensation retry in both modes
- concurrent orders competing for insufficient total stock

Assertions inspect order state, participant rows and exact stock changes.
Full output: [scenarios.txt](../evidence/scenarios.txt).

## Infrastructure failure exercises

`node scripts/resilience.mjs --poison-only` first inserted and verified a poison record.
Then `node scripts/resilience.mjs` passed:

| Exercise | Observed result |
|---|---|
| Orchestration, Kafka stopped during order acceptance | Outbox retained; checkout completed after Kafka restart |
| Orchestration, Order restarted while Payment offline | Persisted deadline cancelled checkout; all participants compensated after recovery |
| Choreography, Kafka stopped during order acceptance | Outbox retained; checkout completed after Kafka restart |
| Choreography, Order restarted while Payment offline | Persisted deadline cancelled checkout; all participants compensated after recovery |
| Invalid envelope version | All services quarantined it; exact record found in Order's actual Kafka DLT |

The last DLT assertion ran with the earlier poison record still retained, verifying
that the test works when the DLT already has history.

Outputs: [poison-first.txt](../evidence/poison-first.txt),
[resilience.txt](../evidence/resilience.txt).
All six containers were healthy after the exercises. Test orders and diagnostic records
remain available for inspection. Successful scenarios consumed some of the initial stock.

## Review and fixes

An independent code review checked transaction boundaries, cancellation markers,
order locking, retries, compensation acknowledgements and the guide's claims.
Its two findings were fixed and re-reviewed:

1. Simulated decline/rejection now respects `DEMO_ENABLED=false`, including pending
   fault-bearing commands. Regression tests failed before the fix and passed afterward.
2. The DLT assertion searches retained records for this run's ID rather than assuming
   the first record belongs to the current run.

The live suite additionally exposed missing Java parameter metadata for HTTP routes
in the shared Gradle library. All Java compilation now uses `-parameters`; a MockMvc
integration test verifies the operator route binds its UUID and persists the unblock.
The complete live suite passed after rebuilding the corrected images.

## Scope of this evidence

The GitHub Actions workflow is supplied but has not run on GitHub; no remote was pushed.
This is functional and recovery verification, not a load benchmark, a penetration test,
a multi-host failover test, or validation against real payment/shipping providers.
The tests cover duplicate delivery and rollback boundaries; they do not force a process
crash at every possible instruction between Kafka acknowledgement and outbox commit.

Host ports 5432 and 8081 were already occupied. This project uses PostgreSQL 55432
and Inventory 18081, without changing those existing services.
