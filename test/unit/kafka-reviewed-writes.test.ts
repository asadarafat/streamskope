import { describe, expect, it, vi, type Mock } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
  parseKafkaWriteInput,
  type KafkaWriteInput,
  type KafkaWriteOutcome,
} from "../../src/features/kafka/contracts";
import { KafkaReviewedWriteService } from "../../src/features/kafka/application/reviewed-write-service";
import { KafkaConnectionScopes } from "../../src/features/kafka/application/connection-scope";
import { RecordingActiveConnection } from "../support/kafka-backend-facade-fixture";

const topic: KafkaWriteInput = {
  kind: "topic",
  topic: "reviewed.topic",
  partitions: 3,
  replicationFactor: 1,
  configs: [{ name: "retention.ms", value: "86400000" }],
};
const acknowledgement: KafkaWriteOutcome = {
  state: "acknowledged",
  detail: "Broker accepted creation.",
  receipt: null,
  verification: "unavailable",
};
function setup(): {
  service: KafkaReviewedWriteService;
  applyWrite: Mock<() => Promise<KafkaWriteOutcome>>;
  reviewWrite: Mock<() => Promise<void>>;
  advance: () => void;
  disconnect: () => void;
  reconnect: () => void;
} {
  const reviewWrite = vi.fn((): Promise<void> => Promise.resolve());
  const applyWrite = vi.fn((): Promise<KafkaWriteOutcome> => Promise.resolve(acknowledgement));
  const connection = Object.assign(new RecordingActiveConnection(), { reviewWrite, applyWrite });
  let context: {
    connection: typeof connection;
    connectionName: string;
    generation: number;
  } | null = { connection, connectionName: "Cluster A", generation: 1 };
  let time = 1_000;
  const scopes = new KafkaConnectionScopes(() => context);
  const service = new KafkaReviewedWriteService(
    () => scopes.reviewedWrite(),
    () => time,
  );
  return {
    service,
    applyWrite,
    reviewWrite,
    advance: (): void => {
      time += 120_001;
    },
    disconnect: (): void => {
      context = null;
    },
    reconnect: (): void => {
      if (context !== null) context = { ...context, generation: 2 };
    },
  };
}

describe("reviewed Kafka writes", () => {
  it("does not write while reviewing and coalesces concurrent and repeated confirmations", async () => {
    const fixture = setup();
    const review = await fixture.service.review(topic);
    expect(review.connectionName).toBe("Cluster A");
    expect(fixture.applyWrite).not.toHaveBeenCalled();
    expect(
      await Promise.all([
        fixture.service.apply(review.planId),
        fixture.service.apply(review.planId),
      ]),
    ).toEqual([acknowledgement, acknowledgement]);
    expect(await fixture.service.apply(review.planId)).toEqual(acknowledgement);
    expect(fixture.applyWrite).toHaveBeenCalledTimes(1);
  });
  it.each(["advance", "disconnect", "reconnect"] as const)(
    "rejects %s after review before any write",
    async (invalidate) => {
      const fixture = setup();
      const review = await fixture.service.review(topic);
      fixture[invalidate]();
      await expect(fixture.service.apply(review.planId)).rejects.toThrow(/expired|connection/u);
      expect(fixture.applyWrite).not.toHaveBeenCalled();
    },
  );
  it("keeps an unknown result and never retries the same plan", async () => {
    const fixture = setup();
    fixture.applyWrite.mockRejectedValueOnce(new Error("Response lost after broker commit"));
    const review = await fixture.service.review(topic);
    expect(await fixture.service.apply(review.planId)).toMatchObject({ state: "unknown" });
    expect(await fixture.service.apply(review.planId)).toMatchObject({ state: "unknown" });
    expect(fixture.applyWrite).toHaveBeenCalledTimes(1);
  });
  it("refuses a review that finishes after its connection generation is revoked", async () => {
    const fixture = setup();
    let finish!: () => void;
    fixture.reviewWrite.mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          finish = resolve;
        }),
    );
    const pending = fixture.service.review(topic);
    const rejected = expect(pending).rejects.toThrow(/connection/u);
    fixture.reconnect();
    finish();
    await rejected;
    expect(fixture.applyWrite).not.toHaveBeenCalled();
  });
  it("does not issue a review when revocation happens between the port result and service continuation", async () => {
    const fixture = setup();
    const pending = fixture.service.review(topic);
    const rejected = expect(pending).rejects.toThrow(/connection/u);
    await Promise.resolve();
    fixture.reconnect();
    await rejected;
    expect(fixture.applyWrite).not.toHaveBeenCalled();
  });
  it("rejects a queued confirmation revoked before dispatch and never retries that plan", async () => {
    const fixture = setup();
    const review = await fixture.service.review(topic);
    const pending = fixture.service.apply(review.planId);
    fixture.reconnect();
    const result = await pending;
    expect(result).toMatchObject({ state: "rejected", verification: "not-applicable" });
    expect(await fixture.service.apply(review.planId)).toEqual(result);
    expect(fixture.applyWrite).not.toHaveBeenCalled();
  });
  it("keeps a delayed broker acknowledgement across reconnection without another dispatch", async () => {
    const fixture = setup();
    let finish!: (value: KafkaWriteOutcome) => void;
    fixture.applyWrite.mockImplementation(
      () =>
        new Promise<KafkaWriteOutcome>((resolve) => {
          finish = resolve;
        }),
    );
    const review = await fixture.service.review(topic);
    const pending = fixture.service.apply(review.planId);
    await Promise.resolve();
    expect(fixture.applyWrite).toHaveBeenCalledTimes(1);
    fixture.reconnect();
    finish(acknowledgement);
    expect(await pending).toEqual(acknowledgement);
    expect(await fixture.service.apply(review.planId)).toEqual(acknowledgement);
    expect(fixture.applyWrite).toHaveBeenCalledTimes(1);
  });
  it("preserves acknowledgement after connection loss and accepts no replacement input", async () => {
    const fixture = setup();
    fixture.applyWrite.mockImplementation(() => {
      fixture.disconnect();
      return Promise.resolve(acknowledgement);
    });
    const review = await fixture.service.review(topic);
    expect(await fixture.service.apply(review.planId)).toEqual(acknowledgement);
    expect(await fixture.service.apply(review.planId)).toEqual(acknowledgement);
    expect(() =>
      parseHostCommand({
        command: "writes.apply",
        id: "change",
        version: HOST_PROTOCOL_VERSION,
        payload: { planId: review.planId, input: { ...topic, topic: "other" } },
      }),
    ).toThrow();
  });
  it("retains its own snapshot of reviewed bytes", async () => {
    const fixture = setup();
    const review = await fixture.service.review(topic);
    (review.input as { topic: string }).topic = "changed-in-renderer";
    await fixture.service.apply(review.planId);
    expect(fixture.applyWrite).toHaveBeenCalledWith(topic);
  });
  it("bounds stored plans and refuses evicted identifiers", async () => {
    const fixture = setup();
    const first = await fixture.service.review(topic);
    for (let index = 0; index < 32; index += 1) await fixture.service.review(topic);
    await expect(fixture.service.apply(first.planId)).rejects.toThrow(/no longer available/u);
    expect(fixture.applyWrite).not.toHaveBeenCalled();
  });
  it("validates binary/null/duplicate header bytes and rejects incomplete or oversized records", () => {
    const input = {
      kind: "record",
      topic: "bytes",
      partition: 0,
      record: {
        state: "complete",
        encoding: "base64",
        key: null,
        value: "AP8=",
        headers: [
          { key: "eA==", value: "" },
          { key: "eA==", value: null },
        ],
      },
    };
    expect(parseKafkaWriteInput(input)).toEqual(input);
    expect(() =>
      parseKafkaWriteInput({ ...input, record: { state: "unavailable", reason: "masked" } }),
    ).toThrow();
    expect(() =>
      parseKafkaWriteInput({
        ...input,
        record: { ...input.record, value: Buffer.alloc(65_537).toString("base64") },
      }),
    ).toThrow(/64 KiB/u);
    expect(() =>
      parseKafkaWriteInput({ ...topic, configs: [topic.configs[0], topic.configs[0]] }),
    ).toThrow(/repeat/u);
    expect(() => parseKafkaWriteInput({ ...topic, replicationFactor: 0 })).toThrow();
  });
  it("requires a typed result and validates returned review destinations", () => {
    const envelope = {
      command: "writes.apply",
      id: "apply",
      version: HOST_PROTOCOL_VERSION,
      ok: true,
      result: { correlationId: "operation", outcome: acknowledgement },
    };
    expect(parseHostCommandResponse(envelope)).toEqual(envelope);
    expect(() =>
      parseHostCommandResponse({ ...envelope, result: { correlationId: "operation" } }),
    ).toThrow();
    expect(() =>
      parseHostCommandResponse({
        ...envelope,
        result: { ...envelope.result, outcome: { ...acknowledgement, state: "succeeded" } },
      }),
    ).toThrow();
  });
});
