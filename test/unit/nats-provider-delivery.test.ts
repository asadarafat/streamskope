import { afterEach, describe, expect, it, vi } from "vitest";

import { AccountedProviderEventQueue } from "../../src/platform/node/accounted-provider-event-queue";
import { createNatsDeliveryPolicy } from "../../src/platform/node/nats-delivery-policy";
import { createNatsElectronDeliveryBinding } from "../../src/platform/electron/main/nats-provider-delivery";
import { emptyNatsCounters } from "../../src/features/nats/application/record-buffer";
import {
  NATS_PROTOCOL_VERSION,
  parseNatsEvent,
  type NatsEvent,
  type NatsRecord,
} from "../../src/features/nats/contracts";
import { copiedNatsReceipt } from "../support/nats-application-fixture";

const context = {
  version: NATS_PROTOCOL_VERSION,
  operation: "subscription.start" as const,
  correlationId: "fixture-context",
};
function record(id: string, generation = "generation-1"): NatsRecord {
  const receipt = copiedNatsReceipt(id);
  if (receipt.kind !== "record") throw new Error("fixture");
  return { ...receipt.record, id, generation };
}
function batch(sequence: number, ids: readonly string[], generation = "generation-1"): NatsEvent {
  return {
    ...context,
    sequence,
    event: "records.batch",
    payload: {
      generation,
      records: ids.map((id) => record(id, generation)),
      counters: { ...emptyNatsCounters(), receivedRecords: 100, publishedRecords: 100 },
    },
  };
}
afterEach(() => vi.useRealTimers());
describe("NATS provider-specific loss evidence and shared delivery ownership", () => {
  it("retains inflight records and reports only this client's pending omissions", () => {
    const queue = new AccountedProviderEventQueue({
      limits: { maxEvents: 10, maxSerializedBytes: 100_000, maxRecords: 3 },
      overflow: "evict-oldest-pending-records",
      policy: createNatsDeliveryPolicy(),
    });
    expect(queue.enqueue(batch(1, ["one"]))).toBeUndefined();
    const first = queue.begin();
    if (first.kind !== "lease") throw new Error("fixture");
    queue.enqueue(batch(2, ["two", "three", "four"]));
    expect(queue.costs.records).toBe(3);
    first.lease.complete();
    const second = queue.begin();
    if (second.kind !== "lease" || second.lease.event.event !== "records.batch")
      throw new Error("fixture");
    expect(second.lease.event.payload.records.map((item) => item.id)).toEqual(["three", "four"]);
    expect(second.lease.event.payload.counters.transportOmittedRecords).toBe(1);
    expect(parseNatsEvent(second.lease.event)).toEqual(second.lease.event);
    second.lease.complete();
    expect(queue.costs.records).toBe(0);
    queue.close();
  });
  it("resets omission evidence for a confirmed new subscription generation", () => {
    const queue = new AccountedProviderEventQueue({
      limits: { maxEvents: 10, maxSerializedBytes: 100_000, maxRecords: 1 },
      overflow: "evict-oldest-pending-records",
      policy: createNatsDeliveryPolicy(),
    });
    queue.enqueue(batch(1, ["one", "two"]));
    queue.enqueue({
      ...context,
      sequence: 2,
      event: "subscription.changed",
      payload: {
        state: "loading",
        generation: "generation-2",
        subject: "qualification.>",
        counters: emptyNatsCounters(),
      },
    });
    const loading = queue.begin();
    if (loading.kind !== "lease" || loading.lease.event.event !== "subscription.changed")
      throw new Error("fixture");
    expect(loading.lease.event.payload.counters.transportOmittedRecords).toBe(0);
    loading.lease.complete();
    queue.enqueue(batch(3, ["new"], "generation-2"));
    const next = queue.begin();
    if (next.kind !== "lease" || next.lease.event.event !== "records.batch")
      throw new Error("fixture");
    expect(next.lease.event.payload.counters.transportOmittedRecords).toBe(0);
    queue.close();
  });
  it("holds native event ownership until matching ACK and ignores stale ACKs", () => {
    const sent: unknown[] = [];
    const failures: string[] = [];
    const delivery = createNatsElectronDeliveryBinding().create(
      (event) => sent.push(event),
      (reason) => failures.push(reason),
    );
    delivery.enqueue(batch(1, ["one"]));
    delivery.enqueue(batch(2, ["two"]));
    expect(sent).toHaveLength(1);
    delivery.acknowledge(99);
    expect(sent).toHaveLength(1);
    delivery.acknowledge(1);
    expect(sent).toHaveLength(2);
    delivery.acknowledge(2);
    delivery.close();
    expect(failures).toEqual([]);
  });
  it("fails native overflow without silently omitting live records", () => {
    const failures: string[] = [];
    const delivery = createNatsElectronDeliveryBinding().create(
      () => undefined,
      (reason) => failures.push(reason),
    );
    for (let index = 0; index < 65; index++) delivery.enqueue(batch(index, [String(index)]));
    expect(failures).toEqual(["event-limit"]);
    delivery.close();
  });
  it("uses the shared finite ACK deadline and gives Core NATS recovery instructions", async () => {
    vi.useFakeTimers();
    const failures: string[] = [];
    const binding = createNatsElectronDeliveryBinding();
    const delivery = binding.create(
      () => undefined,
      (reason) => failures.push(reason),
    );
    delivery.enqueue(batch(1, ["one"]));
    await vi.advanceTimersByTimeAsync(30_000);
    expect(failures).toEqual(["ack-timeout"]);
    expect(binding.recoveryInstruction?.(true, "ack-timeout")).toContain("cannot replay");
    expect(binding.recoveryInstruction?.(false, "ack-timeout")).toContain("could not be confirmed");
    delivery.close();
  });
});
