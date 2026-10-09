import type { Consumer } from "@platformatic/kafka";
import { expect, it, vi } from "vitest";

import { kafkaFixtureFailure, waitForKafkaTopicOffsets } from "../support/kafka-topic-readiness";

function reader(): {
  metadata: ReturnType<typeof vi.fn>;
  listOffsets: ReturnType<typeof vi.fn>;
  clearMetadata: ReturnType<typeof vi.fn>;
} {
  const metadata = vi.fn().mockResolvedValue({
    topics: new Map([["fixture", { id: "topic-id", partitions: [{ leader: 1, isr: [1] }] }]]),
  });
  const listOffsets = vi.fn().mockResolvedValue(new Map([["fixture", { 0: 0n }]]));
  const clearMetadata = vi.fn();
  return { metadata, listOffsets, clearMetadata };
}

it("requires usable offsets after metadata and retries only read-only readiness", async () => {
  const consumer = reader();
  consumer.listOffsets.mockRejectedValueOnce(
    Object.assign(new Error("data listener is not ready"), { apiId: "NOT_LEADER_OR_FOLLOWER" }),
  );
  await expect(
    waitForKafkaTopicOffsets(consumer as unknown as Consumer, "fixture", "topic-id", 1),
  ).resolves.toEqual([0n]);
  expect(consumer.metadata).toHaveBeenCalledTimes(2);
  expect(consumer.listOffsets).toHaveBeenCalledTimes(2);
  expect(consumer.clearMetadata).toHaveBeenCalledOnce();
});

it("does not retry authorization failures mixed with transient metadata errors", async () => {
  const consumer = reader();
  consumer.listOffsets.mockRejectedValue(
    new AggregateError([
      { apiId: "NOT_LEADER_OR_FOLLOWER" },
      { apiId: "TOPIC_AUTHORIZATION_FAILED" },
    ]),
  );
  await expect(
    waitForKafkaTopicOffsets(consumer as unknown as Consumer, "fixture", "topic-id", 1),
  ).rejects.toThrow("NOT_LEADER_OR_FOLLOWER, TOPIC_AUTHORIZATION_FAILED");
  expect(consumer.listOffsets).toHaveBeenCalledOnce();
  expect(consumer.clearMetadata).not.toHaveBeenCalled();
});

it("does not retry an unexpected SDK failure", async () => {
  const consumer = reader();
  consumer.metadata.mockRejectedValue(new Error("secret request contents"));
  await expect(
    waitForKafkaTopicOffsets(consumer as unknown as Consumer, "fixture", "topic-id", 1),
  ).rejects.toThrow("no Kafka protocol code reported");
  expect(consumer.clearMetadata).not.toHaveBeenCalled();
});

it("retains nested protocol codes without SDK request details or credentials", () => {
  const failure = new AggregateError([
    Object.assign(new Error("token=private-fixture-token"), {
      apiId: "NOT_LEADER_OR_FOLLOWER",
      request: { password: "private-fixture-password" },
    }),
  ]);
  expect(kafkaFixtureFailure("Seed failed", failure).message).toBe(
    "Seed failed: NOT_LEADER_OR_FOLLOWER.",
  );
});
