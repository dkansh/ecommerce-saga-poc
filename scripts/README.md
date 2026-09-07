# Executable saga scenarios

The scenario runner exercises the four running services through HTTP and checks both the order result and each participant's persisted state. It uses Node's built-in `fetch`, `assert`, and `crypto` APIs, so it has no package installation step. Use Node 20 or newer.

Start the complete local stack and wait for all four health checks to report `UP`, then run from the repository root:

```shell
node scripts/scenarios.mjs
```

The default run is comprehensive. It runs every business scenario in both `ORCHESTRATION` and `CHOREOGRAPHY` mode:

- successful checkout, HTTP idempotency, and changed-payload conflict
- invalid quantity and sold-out inventory
- declined payment and rejected shipping
- bounded transient payment retry
- explicit client cancellation through `POST /orders/{id}/cancel`
- duplicate Kafka message ID handling
- cancellation tombstones followed by late forward messages
- permanent payment unavailability, quarantine, watchdog cancellation, unblock, and safe replay
- unavailable refund, `MANUAL_INTERVENTION`, operator unblock, and compensation retry
- three concurrent reservations sized so only one can win

The payment-unavailable and refund-unavailable cases deliberately wait for persisted service deadlines. Each poll is bounded to 100 seconds by default. There are no unconditional long sleeps. For a faster smoke run that skips those two deadline-driven cases, use:

```shell
node scripts/scenarios.mjs --quick
```

Every completed order must leave inventory `RESERVED`, payment `CHARGED`, and shipping `BOOKED`. Every compensated order must leave inventory `RELEASED`, payment `REFUNDED`, and shipping `CANCELLED`. The runner measures inventory before each case and verifies the exact resulting stock, including restoration after compensation and nonnegative stock under contention.

The operational checks call the demo-only `/ops/publish`, `/ops/failures`, `/ops/unblock`, `/ops/replay`, and `/ops/outbox` endpoints. Their token defaults to `local-demo-token`. Override URLs, token, or timeouts with environment variables when the stack is mapped differently:

| Variable | Default |
| --- | --- |
| `ORDER_URL` | `http://localhost:8080` |
| `INVENTORY_URL` | `http://localhost:18081` |
| `PAYMENT_URL` | `http://localhost:8082` |
| `SHIPPING_URL` | `http://localhost:8083` |
| `DEMO_TOKEN` | `local-demo-token` |
| `SCENARIO_TIMEOUT_MS` | `100000` |
| `REQUEST_TIMEOUT_MS` | `5000` |

PowerShell example:

```powershell
$env:DEMO_TOKEN = "a-different-local-token"
$env:SCENARIO_TIMEOUT_MS = "120000"
node scripts/scenarios.mjs
```

Run against a freshly started demo stack for reproducible stock totals. The runner restores stock for synthetic duplicate and late-message cases, but successful orders and the final contention winner intentionally consume stock. A failure exits with a nonzero status and includes the scenario name, the expected condition, and the last observed value.
