#!/usr/bin/env node

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";

const BASES = {
  order: process.env.ORDER_URL ?? "http://localhost:8080",
  inventory: process.env.INVENTORY_URL ?? "http://localhost:18081",
  payment: process.env.PAYMENT_URL ?? "http://localhost:8082",
  shipping: process.env.SHIPPING_URL ?? "http://localhost:8083",
};
const DEMO_TOKEN = process.env.DEMO_TOKEN ?? "local-demo-token";
const POLL_TIMEOUT_MS = Number(process.env.SCENARIO_TIMEOUT_MS ?? 100_000);
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS ?? 5_000);
const MODES = ["ORCHESTRATION", "CHOREOGRAPHY"];
const TERMINAL = new Set(["COMPLETED", "CANCELLED", "MANUAL_INTERVENTION"]);
const args = process.argv.slice(2);
const quick = args.includes("--quick");

if (args.some((argument) => !["--quick", "--help", "-h"].includes(argument))) {
  console.error("Usage: node scripts/scenarios.mjs [--quick]");
  process.exit(2);
}
if (args.includes("--help") || args.includes("-h")) {
  console.log(`Usage: node scripts/scenarios.mjs [--quick]

Runs the executable saga scenarios against Order on 8080, Inventory on 18081,
Payment on 8082, and Shipping on 8083.
The full run includes watchdog timeout and manual-intervention recovery cases.
--quick skips those deliberately slow cases.`);
  process.exit(0);
}
assert.ok(Number.isFinite(POLL_TIMEOUT_MS) && POLL_TIMEOUT_MS > 0, "SCENARIO_TIMEOUT_MS must be positive");
assert.ok(Number.isFinite(REQUEST_TIMEOUT_MS) && REQUEST_TIMEOUT_MS > 0, "REQUEST_TIMEOUT_MS must be positive");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function elapsed(started) {
  return `${((performance.now() - started) / 1000).toFixed(2)}s`;
}

function compact(value) {
  const rendered = typeof value === "string" ? value : (JSON.stringify(value) ?? String(value));
  return rendered.length <= 700 ? rendered : `${rendered.slice(0, 697)}...`;
}

async function request(path, {
  base = BASES.order,
  method = "GET",
  body,
  headers = {},
  expected = [200],
} = {}) {
  const expectedStatuses = Array.isArray(expected) ? expected : [expected];
  let response;
  try {
    response = await fetch(`${base}${path}`, {
      method,
      headers: {
        Accept: "application/json",
        ...(body === undefined ? {} : { "Content-Type": "application/json" }),
        ...headers,
      },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    });
  } catch (error) {
    throw new Error(`${method} ${base}${path} failed: ${error.message}`, { cause: error });
  }

  const text = await response.text();
  let value = null;
  if (text) {
    try {
      value = JSON.parse(text);
    } catch {
      value = text;
    }
  }
  if (!expectedStatuses.includes(response.status)) {
    throw new Error(
      `${method} ${base}${path}: expected HTTP ${expectedStatuses.join("/")}, got ${response.status}: ${compact(value)}`,
    );
  }
  return { status: response.status, body: value, headers: response.headers };
}

async function poll(label, probe, accept, timeoutMs = POLL_TIMEOUT_MS) {
  const started = performance.now();
  let interval = 100;
  let lastValue;
  let lastError;
  while (performance.now() - started < timeoutMs) {
    try {
      lastValue = await probe();
      lastError = undefined;
      if (await accept(lastValue)) return lastValue;
    } catch (error) {
      lastError = error;
    }
    await sleep(interval);
    interval = Math.min(1_000, Math.ceil(interval * 1.4));
  }
  const detail = lastError ? lastError.message : compact(lastValue);
  throw new Error(`Timed out after ${Math.round(timeoutMs / 1000)}s waiting for ${label}; last result: ${detail}`);
}

async function scenario(name, action) {
  const started = performance.now();
  process.stdout.write(`\n> ${name}\n`);
  try {
    await action();
    process.stdout.write(`[PASS] ${name} (${elapsed(started)})\n`);
  } catch (error) {
    error.message = `${name}: ${error.message}`;
    throw error;
  }
}

function orderRequest(mode, overrides = {}) {
  return {
    mode,
    sku: "SKU-1",
    quantity: 1,
    amountCents: 1_200,
    fault: "NONE",
    ...overrides,
  };
}

async function createOrder(body, idempotencyKey = `scenario-${randomUUID()}`) {
  const response = await request("/orders", {
    method: "POST",
    body,
    headers: { "Idempotency-Key": idempotencyKey },
    expected: [202],
  });
  assert.equal(typeof response.body, "object", "create order response must be JSON");
  assert.equal(typeof response.body.id, "string", "create order response must include id");
  assert.equal(typeof response.body.status, "string", "create order response must include status");
  return { order: response.body, key: idempotencyKey };
}

async function getOrder(id) {
  return (await request(`/orders/${id}`)).body;
}

async function waitForOrder(id, expectedStatus) {
  let previous;
  return poll(
    `order ${id} to reach ${expectedStatus}`,
    async () => {
      const order = await getOrder(id);
      if (order.status !== previous) {
        process.stdout.write(`  ${id}: ${order.status}\n`);
        previous = order.status;
      }
      return order;
    },
    (order) => order.status === expectedStatus || (TERMINAL.has(order.status) && order.status !== expectedStatus),
  ).then((order) => {
    assert.equal(order.status, expectedStatus, `order ${id} reached unexpected terminal state ${order.status}`);
    assert.ok(Object.hasOwn(order, "reason"), "order inspection response must include reason");
    if (expectedStatus !== "COMPLETED") {
      assert.equal(typeof order.reason, "string", "failed/cancelled order must have a reason");
      assert.ok(order.reason.trim(), "failed/cancelled order reason must not be blank");
    }
    return order;
  });
}

async function getParticipant(service, sagaId) {
  const paths = {
    inventory: `/inventory/${sagaId}`,
    payment: `/payments/${sagaId}`,
    shipping: `/shipments/${sagaId}`,
  };
  const response = await request(paths[service], { base: BASES[service], expected: [200, 404] });
  return response.status === 404 ? null : response.body;
}

async function waitForParticipant(service, sagaId, expectedState) {
  const row = await poll(
    `${service} ${sagaId} to reach ${expectedState}`,
    () => getParticipant(service, sagaId),
    (candidate) => candidate?.state === expectedState,
  );
  assert.equal(row.saga_id, sagaId, `${service} row must belong to the saga`);
  return row;
}

async function stock(sku = "SKU-1") {
  const row = (await request(`/inventory/stock/${sku}`, { base: BASES.inventory })).body;
  assert.equal(row.sku, sku);
  assert.equal(Number.isInteger(row.available), true, `stock.available must be an integer: ${compact(row)}`);
  return row.available;
}

async function waitForStock(sku, expected) {
  return poll(
    `${sku} stock to equal ${expected}`,
    () => stock(sku),
    (available) => available === expected,
  );
}

async function assertCompleted(sagaId, beforeStock, quantity = 1) {
  await Promise.all([
    waitForParticipant("inventory", sagaId, "RESERVED"),
    waitForParticipant("payment", sagaId, "CHARGED"),
    waitForParticipant("shipping", sagaId, "BOOKED"),
  ]);
  const available = await waitForStock("SKU-1", beforeStock - quantity);
  assert.ok(available >= 0, "stock must never be negative");
}

async function assertCompensated(sagaId, sku, beforeStock) {
  await Promise.all([
    waitForParticipant("inventory", sagaId, "RELEASED"),
    waitForParticipant("payment", sagaId, "REFUNDED"),
    waitForParticipant("shipping", sagaId, "CANCELLED"),
  ]);
  assert.equal(await waitForStock(sku, beforeStock), beforeStock, "compensation must restore stock");
}

async function ops(path, { base = BASES.order, method = "GET", body, expected = [200, 204] } = {}) {
  return request(path, {
    base,
    method,
    body,
    expected,
    headers: { "X-Demo-Token": DEMO_TOKEN },
  });
}

function message(sagaId, mode, type, overrides = {}) {
  return {
    version: 1,
    id: randomUUID(),
    sagaId,
    mode,
    type,
    sku: "SKU-1",
    quantity: 1,
    amountCents: 1_200,
    fault: "NONE",
    ...overrides,
  };
}

async function publish(envelope) {
  await ops("/ops/publish", { method: "POST", body: envelope });
}

async function outbox(base) {
  const value = (await ops("/ops/outbox", { base })).body;
  return { pending: Number(value.pending), total: Number(value.total) };
}

async function waitForQuiescence(minimumSourceTotal, sourceBase = BASES.order) {
  let stable = 0;
  let previousSignature;
  let stableSince;
  const serviceBases = Object.values(BASES);
  const sourceIndex = serviceBases.indexOf(sourceBase);
  assert.notEqual(sourceIndex, -1, `unknown outbox source ${sourceBase}`);
  await poll(
    "all message outboxes to drain and remain stable",
    async () => Promise.all(serviceBases.map(outbox)),
    (rows) => {
      const signature = rows.map(({ pending, total }) => `${pending}:${total}`).join("|");
      const allDrained = rows.every(({ pending }) => pending === 0);
      const sourcePublished = rows[sourceIndex].total >= minimumSourceTotal;
      if (allDrained && sourcePublished && signature === previousSignature) {
        stable += 1;
        stableSince ??= performance.now();
      } else {
        stable = 0;
        stableSince = undefined;
      }
      previousSignature = signature;
      return stable >= 3 && performance.now() - stableSince >= 1_500;
    },
  );
}

function failurePayload(row) {
  if (typeof row.payload === "object" && row.payload !== null) return row.payload;
  try {
    return JSON.parse(row.payload);
  } catch {
    return null;
  }
}

async function failures() {
  const rows = (await ops("/ops/failures", { base: BASES.payment })).body;
  assert.ok(Array.isArray(rows), "GET /ops/failures must return an array");
  return rows;
}

async function waitForFailure(sagaId) {
  return poll(
    `payment failure quarantine record for ${sagaId}`,
    failures,
    (rows) => rows.some((row) => failurePayload(row)?.sagaId === sagaId),
  ).then((rows) => rows.find((row) => failurePayload(row)?.sagaId === sagaId));
}

async function verifyReady() {
  await Promise.all(Object.entries(BASES).map(async ([name, base]) => {
    const health = await request("/actuator/health", { base });
    assert.equal(health.body.status, "UP", `${name} health must be UP`);
  }));
}

async function invalidRequestScenario(mode) {
  await request("/orders", {
    method: "POST",
    body: orderRequest(mode, { quantity: 0 }),
    headers: { "Idempotency-Key": `invalid-${randomUUID()}` },
    expected: [400],
  });
}

async function successScenario(mode) {
  const before = await stock();
  const body = orderRequest(mode);
  const { order, key } = await createOrder(body);
  const duplicate = await createOrder(body, key);
  assert.equal(duplicate.order.id, order.id, "same idempotency key and payload must return the original order");
  await request("/orders", {
    method: "POST",
    body: { ...body, amountCents: body.amountCents + 1 },
    headers: { "Idempotency-Key": key },
    expected: [409],
  });
  await waitForOrder(order.id, "COMPLETED");
  await assertCompleted(order.id, before);
}

async function compensatedScenario(mode, overrides, sku = "SKU-1") {
  const before = await stock(sku);
  const { order } = await createOrder(orderRequest(mode, { sku, ...overrides }));
  await waitForOrder(order.id, "CANCELLED");
  await assertCompensated(order.id, sku, before);
  return order.id;
}

async function transientScenario(mode) {
  const before = await stock();
  const { order } = await createOrder(orderRequest(mode, { fault: "PAYMENT_TRANSIENT" }));
  await waitForOrder(order.id, "COMPLETED");
  await assertCompleted(order.id, before);
}

async function clientCancellationScenario(mode) {
  const before = await stock();
  const { order } = await createOrder(orderRequest(mode, { fault: "PAYMENT_UNAVAILABLE" }));
  await waitForParticipant("inventory", order.id, "RESERVED");
  const response = await request(`/orders/${order.id}/cancel`, { method: "POST", expected: [202] });
  assert.equal(response.body.id, order.id);
  assert.ok(
    ["CANCELLING_SHIPPING", "COMPENSATING", "CANCELLED"].includes(response.body.status),
    `cancel response has unexpected status ${response.body.status}`,
  );
  await waitForOrder(order.id, "CANCELLED");
  await assertCompensated(order.id, "SKU-1", before);
  await ops(`/ops/unblock/${order.id}`, { base: BASES.payment, method: "POST" });
}

async function unavailableScenario(mode) {
  const before = await stock();
  const { order } = await createOrder(orderRequest(mode, { fault: "PAYMENT_UNAVAILABLE" }));
  const failure = await waitForFailure(order.id);
  assert.equal(typeof failure.id, "number", `quarantine row needs a numeric id: ${compact(failure)}`);
  assert.equal(typeof failure.message_key, "string", "quarantine row must include message_key");
  assert.equal(failure.message_key, order.id, "quarantined message must retain the saga partition key");
  assert.ok(failure.error, "quarantine row must include an error");

  await waitForOrder(order.id, "CANCELLED");
  await assertCompensated(order.id, "SKU-1", before);

  await ops(`/ops/unblock/${order.id}`, { base: BASES.payment, method: "POST" });
  const paymentOutboxTotal = (await outbox(BASES.payment)).total;
  await ops(`/ops/replay/${failure.id}`, { base: BASES.payment, method: "POST" });
  await waitForQuiescence(paymentOutboxTotal + 1, BASES.payment);

  assert.equal((await getOrder(order.id)).status, "CANCELLED", "late replay must not revive a cancelled order");
  await assertCompensated(order.id, "SKU-1", before);
}

async function refundRecoveryScenario(mode) {
  const before = await stock();
  const { order } = await createOrder(orderRequest(mode, { fault: "REFUND_UNAVAILABLE" }));
  await waitForOrder(order.id, "MANUAL_INTERVENTION");

  await waitForParticipant("payment", order.id, "CHARGED");
  await waitForParticipant("shipping", order.id, "CANCELLED");
  const expectedInventory = mode === "ORCHESTRATION" ? "RESERVED" : "RELEASED";
  await waitForParticipant("inventory", order.id, expectedInventory);
  assert.equal(
    await stock(),
    mode === "ORCHESTRATION" ? before - 1 : before,
    "manual-intervention stock must reflect completed compensation steps",
  );

  await ops(`/ops/unblock/${order.id}`, { base: BASES.payment, method: "POST" });
  await request(`/orders/${order.id}/retry`, { method: "POST", expected: [202] });
  await waitForOrder(order.id, "CANCELLED");
  await assertCompensated(order.id, "SKU-1", before);
}

async function duplicateMessageScenario(mode) {
  const before = await stock();
  const sagaId = randomUUID();
  const forwardType = mode === "ORCHESTRATION" ? "RESERVE_INVENTORY" : "ORDER_CREATED";
  const duplicate = message(sagaId, mode, forwardType);
  const startingOutboxTotal = (await outbox(BASES.order)).total;

  await publish(duplicate);
  await publish(duplicate);
  await waitForParticipant("inventory", sagaId, "RESERVED");
  assert.equal(await waitForStock("SKU-1", before - 1), before - 1, "same message id must reserve once");

  if (mode === "CHOREOGRAPHY") {
    await Promise.all([
      waitForParticipant("payment", sagaId, "CHARGED"),
      waitForParticipant("shipping", sagaId, "BOOKED"),
    ]);
    await publish(message(sagaId, mode, "CANCEL_SAGA"));
    await assertCompensated(sagaId, "SKU-1", before);
  } else {
    await publish(message(sagaId, mode, "RELEASE_INVENTORY"));
    await waitForParticipant("inventory", sagaId, "RELEASED");
    await waitForStock("SKU-1", before);
  }
  await waitForQuiescence(startingOutboxTotal + 3);
}

async function cancellationBeforeLateForwardScenario(mode) {
  const before = await stock();
  const sagaId = randomUUID();
  const sourceTotal = (await outbox(BASES.order)).total;
  if (mode === "CHOREOGRAPHY") {
    await publish(message(sagaId, mode, "CANCEL_SAGA"));
  } else {
    for (const type of ["CANCEL_SHIPMENT", "REFUND_PAYMENT", "RELEASE_INVENTORY"]) {
      await publish(message(sagaId, mode, type));
    }
  }
  await assertCompensated(sagaId, "SKU-1", before);

  const lateTypes = mode === "CHOREOGRAPHY"
    ? ["ORDER_CREATED", "INVENTORY_RESERVED", "PAYMENT_CHARGED"]
    : ["RESERVE_INVENTORY", "CHARGE_PAYMENT", "BOOK_SHIPMENT"];
  for (const type of lateTypes) await publish(message(sagaId, mode, type));
  await waitForQuiescence(sourceTotal + lateTypes.length + (mode === "CHOREOGRAPHY" ? 1 : 3));

  await assertCompensated(sagaId, "SKU-1", before);
}

async function oversellScenario() {
  const before = await stock();
  assert.ok(before > 1, `oversell scenario requires at least two units, found ${before}`);
  const quantity = Math.floor(before / 2) + 1;
  const requests = await Promise.all(Array.from({ length: 3 }, () =>
    createOrder(orderRequest("ORCHESTRATION", { quantity }))));
  const orders = await Promise.all(requests.map(({ order }) => poll(
    `contending order ${order.id} to terminate`,
    () => getOrder(order.id),
    (candidate) => TERMINAL.has(candidate.status),
  )));

  const completed = orders.filter(({ status }) => status === "COMPLETED");
  const cancelled = orders.filter(({ status }) => status === "CANCELLED");
  assert.equal(completed.length, 1, `exactly one contending order should complete: ${compact(orders)}`);
  assert.equal(cancelled.length, 2, `the other contending orders should cancel: ${compact(orders)}`);

  await assertCompleted(completed[0].id, before, quantity);
  for (const order of cancelled) {
    assert.ok(order.reason, `cancelled contending order ${order.id} must include a reason`);
    await Promise.all([
      waitForParticipant("inventory", order.id, "RELEASED"),
      waitForParticipant("payment", order.id, "REFUNDED"),
      waitForParticipant("shipping", order.id, "CANCELLED"),
    ]);
  }
  const after = await waitForStock("SKU-1", before - quantity);
  assert.ok(after >= 0, `stock oversold: ${after}`);
  assert.equal(after, before - quantity, "remaining stock must equal initial stock minus the one winner");
}

async function main() {
  const suiteStarted = performance.now();
  console.log(`Saga scenarios (${quick ? "quick" : "full"})`);
  console.log(`Polling timeout: ${Math.round(POLL_TIMEOUT_MS / 1000)}s`);
  await scenario("all four services are ready", verifyReady);

  for (const mode of MODES) {
    await scenario(`${mode}: invalid quantity is rejected`, () => invalidRequestScenario(mode));
    await scenario(`${mode}: success and HTTP idempotency`, () => successScenario(mode));
    await scenario(`${mode}: sold-out inventory compensates`, () =>
      compensatedScenario(mode, {}, "SOLD-OUT"));
    await scenario(`${mode}: declined payment compensates`, () =>
      compensatedScenario(mode, { fault: "PAYMENT_DECLINED" }));
    await scenario(`${mode}: rejected shipping compensates`, () =>
      compensatedScenario(mode, { fault: "SHIPPING_REJECTED" }));
    await scenario(`${mode}: transient payment failure retries to success`, () => transientScenario(mode));
    await scenario(`${mode}: client cancellation compensates`, () => clientCancellationScenario(mode));
    await scenario(`${mode}: same message id is applied once`, () => duplicateMessageScenario(mode));
    await scenario(`${mode}: cancellation tombstones reject late forward work`, () =>
      cancellationBeforeLateForwardScenario(mode));

    if (!quick) {
      await scenario(`${mode}: unavailable payment is quarantined, times out, and replays safely`, () =>
        unavailableScenario(mode));
      await scenario(`${mode}: failed refund reaches manual intervention and recovers`, () =>
        refundRecoveryScenario(mode));
    }
  }

  await scenario("concurrent reservations do not oversell", oversellScenario);
  console.log(`\nAll scenarios passed in ${elapsed(suiteStarted)}.`);
}

main().catch((error) => {
  console.error(`\n[FAIL] ${error.stack ?? error.message}`);
  process.exitCode = 1;
});
