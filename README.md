# Ecommerce Saga POC

Four Java microservices demonstrate **orchestration and choreography** over Kafka,
with PostgreSQL transactions, an outbox, an inbox, compensation, timeouts and recovery.
The domain is deliberately small: one SKU per order and a total in integer minor units.

| Technology | Pinned version |
|---|---|
| Java | 26.0.2 |
| Spring Boot | 4.1.1 |
| Gradle Wrapper | 9.7.1 |
| PostgreSQL | 18.6 |
| Apache Kafka, KRaft | 4.3.1 |

Versions checked against [Java downloads](https://www.oracle.com/java/technologies/downloads/),
[Spring Boot](https://spring.io/projects/spring-boot/),
[Gradle releases](https://gradle.org/releases/),
[PostgreSQL downloads](https://www.postgresql.org/download/) and
[Kafka downloads](https://kafka.apache.org/community/downloads/) on 2026-09-07.
Java 26 is the current feature release; Java 25 is the current LTS. This project uses
26 as requested. No preview features are enabled. Spring Boot's BOM manages library versions.

## Start

Install Docker Desktop with Linux containers and Compose v2. Allocate approximately
6 GB RAM to Docker. A local JDK or Gradle installation is unnecessary for Docker startup.

```sh
docker compose up -d --build --wait
```

Run from this repository's root. Initial startup downloads images and builds all services.
The Gradle Wrapper verifies its downloaded distribution against the pinned SHA-256 checksum.

| Service | URL | Responsibility |
|---|---|---|
| Order | http://localhost:8080 | Request idempotency, order lifecycle, orchestration, abort watchdog |
| Inventory | http://localhost:18081 | Stock reservation and release |
| Payment | http://localhost:8082 | Simulated charge and refund |
| Shipping | http://localhost:8083 | Simulated booking and cancellation |
| PostgreSQL | localhost:55432 | Separate databases and logins per service |
| Kafka | localhost:9092 | Three-partition saga topic and service DLTs |

All published ports bind to loopback. Compose uses known **local demo credentials**.
Payment and shipping simulate providers using local database transactions; no real money or shipments are involved.

Create an order in PowerShell:

```powershell
$body = @{mode='ORCHESTRATION'; sku='SKU-1'; quantity=1; amountCents=1200; fault='NONE'} | ConvertTo-Json
$order = Invoke-RestMethod http://localhost:8080/orders -Method Post `
  -Headers @{'Idempotency-Key'=[guid]::NewGuid().ToString()} -ContentType application/json -Body $body
Invoke-RestMethod "http://localhost:8080/orders/$($order.id)"
Invoke-RestMethod "http://localhost:8080/orders/$($order.id)/history"
```

Or use curl on Linux/macOS:

```sh
curl -i http://localhost:8080/orders \
  -H 'Content-Type: application/json' -H "Idempotency-Key: demo-$(date +%s)" \
  -d '{"mode":"ORCHESTRATION","sku":"SKU-1","quantity":1,"amountCents":1200,"fault":"NONE"}'
```

The `202 Accepted` response contains an `id` and `Location`; poll that URL until
`COMPLETED`, `CANCELLED` or `MANUAL_INTERVENTION`. Change `mode` to `CHOREOGRAPHY`
to run the event-driven forward flow.

## Verify

With JDK 26 installed:

```powershell
.\gradlew.bat test
docker compose up -d --wait postgres kafka
.\gradlew.bat integrationTest
.\gradlew.bat bootJar
```

On Linux/macOS, use `./gradlew` for the same tasks. `test` runs unit tests;
`integrationTest` runs actual PostgreSQL tests in isolated schemas in `saga_test`.
Integration tests fail when PostgreSQL is unavailable. They do not silently skip.

With all four services running and Node.js 22+ installed:

```sh
node scripts/scenarios.mjs --quick
node scripts/scenarios.mjs
node scripts/resilience.mjs
```

The full run includes timeout and failed-refund recovery, so allow several minutes.
It checks participant records and stock, not only order status. The resilience runner
stops and restarts this project's Kafka/Payment/Order containers to verify outage recovery,
then checks poison-record delivery to an actual Kafka DLT. Successful orders consume
stock; follow the scenario runner's reset guidance for repeated full runs.

Start with [the visual walkthrough](docs/VISUAL-WALKTHROUGH.md) for diagrams of both
saga modes, compensation, retries and recovery. Its [standalone browser edition](docs/VISUAL-WALKTHROUGH.html)
includes the rendered diagrams and works offline.
See [the detailed guide](docs/GUIDE.md) for the architecture, transaction boundaries,
every scenario, API reference, failure recovery, debugging and production trade-offs.
See [verification evidence](docs/VERIFICATION.md) for what was actually executed.

## Project layout

```text
messaging/          Envelope, inbox, outbox, retry, quarantine and local demo controls
order-service/      Order API, saga transition logic, persisted deadlines and history
inventory-service/ Stock and reservation transactions
payment-service/   Simulated charge/refund transactions
shipping-service/  Simulated booking/cancel transactions
infra/             Separate database bootstrap
scripts/           Executable cross-service scenarios
docs/              Detailed guide, design and verification record
```

```sh
docker compose logs -f order-service payment-service
docker compose stop
docker compose start
```

`stop` retains data. `docker compose down` removes this project's containers but keeps
its volumes. **`docker compose down -v` deletes this POC's database and Kafka data**;
use it only when intentionally resetting the demo. Other Compose projects are unaffected.

Source repository: [dkansh/ecommerce-saga-poc](https://github.com/dkansh/ecommerce-saga-poc).
The default branch is `main`. Build and scenario results are available in
[GitHub Actions](https://github.com/dkansh/ecommerce-saga-poc/actions).
