import { afterEach, describe, expect, it, vi } from "vitest";

import { NatsRecordBuffer } from "../../src/features/nats/application/record-buffer";
import {
  NATS_LIMITS,
  NATS_PROTOCOL_VERSION,
  natsRecordRetainedBytes,
  type NatsRecordsBatch,
} from "../../src/features/nats/contracts";
import { parseNatsEvent } from "../../src/features/nats/contracts/validation";
import { copiedNatsReceipt } from "../support/nats-application-fixture";

afterEach(() => vi.useRealTimers());
function fixture(): { buffer: NatsRecordBuffer; batches: NatsRecordsBatch[]; failures: unknown[] } {
  const batches: NatsRecordsBatch[] = [];
  const failures: unknown[] = [];
  const buffer = new NatsRecordBuffer(
    "fixture-generation",
    (batch) => batches.push(batch),
    (error) => failures.push(error),
  );
  return { buffer, batches, failures };
}
describe("Core NATS copied-record bounds and truthful counters", () => {
  it("keeps newest bounded records during setup and accounts every eviction", async () => {
    vi.useFakeTimers();
    const { buffer, batches, failures } = fixture();
    for (let index = 0; index < 1200; index++) buffer.accept(copiedNatsReceipt(String(index)));
    expect(buffer.counters()).toMatchObject({
      receivedRecords: 1200,
      applicationOmittedRecords: 200,
      queuedRecords: 1000,
      publishedRecords: 0,
    });
    buffer.startPublishing();
    await vi.advanceTimersByTimeAsync(25);
    expect(batches.flatMap((batch) => batch.records).map((record) => record.payload.data)).toEqual(
      Array.from({ length: 1000 }, (_, index) => String(index + 200)),
    );
    expect(batches).toHaveLength(5);
    expect(buffer.counters()).toMatchObject({
      publishedRecords: 1000,
      queuedRecords: 0,
      queuedBytes: 0,
    });
    expect(failures).toEqual([]);
    buffer.close();
  });
  it("accounts exact serialized record bytes, including JSON escaping", async () => {
    vi.useFakeTimers();
    const { buffer, batches } = fixture();
    for (let index = 0; index < 70; index++) buffer.accept(copiedNatsReceipt("\n".repeat(100_000)));
    expect(buffer.counters().queuedBytes).toBeLessThanOrEqual(NATS_LIMITS.queuedBytes);
    expect(buffer.counters().applicationOmittedRecords).toBeGreaterThan(0);
    buffer.startPublishing();
    await vi.advanceTimersByTimeAsync(25);
    expect(batches.length).toBeGreaterThan(1);
    for (const [sequence, batch] of batches.entries()) {
      const event = {
        version: NATS_PROTOCOL_VERSION,
        sequence,
        event: "records.batch",
        operation: "subscription.start",
        correlationId: "x".repeat(128),
        payload: batch,
      };
      expect(new TextEncoder().encode(JSON.stringify(event)).byteLength).toBeLessThanOrEqual(
        NATS_LIMITS.batchBytes,
      );
      expect(parseNatsEvent(event)).toEqual(event);
    }
    const records = batches.flatMap((batch) => batch.records);
    expect(new Set(records.map((record) => record.id)).size).toBe(records.length);
    buffer.close();
  });
  it("omits legal raw payloads whose escaped representation cannot fit one wire batch", async () => {
    vi.useFakeTimers();
    const { buffer, batches } = fixture();
    buffer.accept(copiedNatsReceipt("\u0001".repeat(NATS_LIMITS.payloadBytes)));
    expect(buffer.counters()).toMatchObject({
      receivedRecords: 1,
      applicationOmittedRecords: 1,
      queuedBytes: 0,
      queuedRecords: 0,
    });
    buffer.startPublishing();
    await vi.advanceTimersByTimeAsync(25);
    expect(batches[0]!.records).toEqual([]);
    buffer.close();
  });
  it("measures the actual retained record rather than only its payload", () => {
    const { buffer } = fixture();
    const receipt = copiedNatsReceipt("value");
    buffer.accept(receipt);
    if (receipt.kind !== "record") throw new Error("fixture");
    expect(buffer.counters().queuedBytes).toBe(
      natsRecordRetainedBytes({
        ...receipt.record,
        generation: "fixture-generation",
        id: "fixture-generation.1",
      }),
    );
    buffer.close();
  });
  it("cancels pending publication and counts discards once when closed", async () => {
    vi.useFakeTimers();
    const { buffer, batches } = fixture();
    buffer.startPublishing();
    buffer.accept(copiedNatsReceipt());
    buffer.close();
    buffer.close();
    buffer.accept(copiedNatsReceipt());
    await vi.advanceTimersByTimeAsync(100);
    expect(batches).toEqual([]);
    expect(buffer.counters()).toMatchObject({
      receivedRecords: 1,
      applicationOmittedRecords: 1,
      publishedRecords: 0,
      queuedRecords: 0,
    });
  });
  it("keeps engine omissions separate from transport omissions", async () => {
    vi.useFakeTimers();
    const { buffer, batches } = fixture();
    buffer.accept({ kind: "omitted", reason: "payload-limit", payloadBytes: 300_000 });
    buffer.startPublishing();
    await vi.advanceTimersByTimeAsync(25);
    expect(batches[0]!.counters).toMatchObject({
      receivedRecords: 1,
      applicationOmittedRecords: 1,
      transportOmittedRecords: 0,
    });
    buffer.close();
  });
  it("bounds generated record identifiers for every safe counter", () => {
    expect(
      () =>
        new NatsRecordBuffer(
          "g".repeat(128),
          () => undefined,
          () => undefined,
        ),
    ).toThrow();
    expect(
      () =>
        new NatsRecordBuffer(
          "bad/generation",
          () => undefined,
          () => undefined,
        ),
    ).toThrow();
  });
});
