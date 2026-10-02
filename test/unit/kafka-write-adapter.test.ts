import {
  Admin,
  Consumer,
  MultipleErrors,
  Producer,
  ProtocolError,
  type ClusterMetadata,
} from "@platformatic/kafka";
import { afterEach, expect, it, vi, type MockInstance } from "vitest";

import { PlatformaticReviewedWrites } from "../../src/features/kafka/engine/platformatic-writes";
import type { KafkaWriteInput } from "../../src/features/kafka/contracts";

afterEach(() => vi.restoreAllMocks());
const record: KafkaWriteInput = {
  kind: "record",
  topic: "orders",
  partition: 0,
  record: { state: "complete", encoding: "base64", key: null, value: "dGVzdA==", headers: [] },
};
const metadata: ClusterMetadata = {
  id: "test-cluster",
  controllerId: 1,
  lastUpdate: 0,
  brokers: new Map([[1, { host: "localhost", port: 9092, rack: null }]]),
  topics: new Map([
    [
      "orders",
      {
        id: "topic-id",
        partitionsCount: 1,
        lastUpdate: 0,
        partitions: [{ leader: 1, leaderEpoch: 0, replicas: [1], isr: [1], offlineReplicas: [] }],
      },
    ],
  ]),
};
function setup(): {
  writer: PlatformaticReviewedWrites;
  send: MockInstance<Producer["send"]>;
  controller: AbortController;
  readMetadata: MockInstance<Admin["metadata"]>;
} {
  vi.spyOn(Admin.prototype, "listTopics").mockResolvedValue(["orders"]);
  const readMetadata = vi.spyOn(Admin.prototype, "metadata").mockResolvedValue(metadata);
  vi.spyOn(Admin.prototype, "close").mockResolvedValue();
  vi.spyOn(Producer.prototype, "close").mockResolvedValue();
  vi.spyOn(Consumer.prototype, "close").mockResolvedValue();
  const send = vi
    .spyOn(Producer.prototype, "send")
    .mockResolvedValue({ offsets: [{ topic: "orders", partition: 0, offset: 42n }] });
  vi.spyOn(Consumer.prototype, "consume").mockRejectedValue(new Error("Read permission denied"));
  const controller = new AbortController();
  const writer = new PlatformaticReviewedWrites(
    { brokers: ["127.0.0.1:1"], tlsEnabled: false, operationTimeoutMs: 10_000 },
    controller.signal,
  );
  return { writer, send, controller, readMetadata };
}

it("retains the offset after read-back fails and performs no second send", async () => {
  const { writer, send } = setup();
  expect(await writer.apply(record)).toMatchObject({
    state: "acknowledged",
    verification: "unavailable",
    receipt: { topic: "orders", partition: 0, offset: "42" },
  });
  expect(send).toHaveBeenCalledTimes(1);
});

it.each(["TOPIC_AUTHORIZATION_FAILED", "INVALID_RECORD"])(
  "reports explicit broker rejection %s without retry",
  async (code) => {
    const { writer, send } = setup();
    send.mockRejectedValue(new MultipleErrors("Produce failed", [new ProtocolError(code)]));
    expect(await writer.apply(record)).toMatchObject({ state: "rejected", receipt: null });
    expect(send).toHaveBeenCalledTimes(1);
  },
);

it("reports cancellation after dispatch as unknown", async () => {
  const { writer, send, controller } = setup();
  send.mockImplementation(() => {
    controller.abort();
    return new Promise(() => undefined);
  });
  expect(await writer.apply(record)).toMatchObject({ state: "unknown", receipt: null });
  expect(send).toHaveBeenCalledTimes(1);
});

it("does not dispatch an already cancelled operation", async () => {
  const { writer, send, controller } = setup();
  controller.abort();
  expect(await writer.apply(record)).toMatchObject({ state: "rejected" });
  expect(send).not.toHaveBeenCalled();
});

it("preserves topic acknowledgement when refresh is denied", async () => {
  const { writer, readMetadata } = setup();
  const create = vi.spyOn(Admin.prototype, "createTopics").mockResolvedValue([]);
  readMetadata
    .mockResolvedValueOnce(metadata)
    .mockRejectedValueOnce(new Error("Metadata unavailable"));
  expect(
    await writer.apply({
      kind: "topic",
      topic: "new-topic",
      partitions: 1,
      replicationFactor: 1,
      configs: [],
    }),
  ).toMatchObject({ state: "acknowledged", verification: "unavailable" });
  expect(create).toHaveBeenCalledTimes(1);
});

it("rejects an existing name without calling CreateTopics", async () => {
  const { writer } = setup();
  const create = vi.spyOn(Admin.prototype, "createTopics");
  expect(
    await writer.apply({
      kind: "topic",
      topic: "orders",
      partitions: 3,
      replicationFactor: 1,
      configs: [],
    }),
  ).toMatchObject({ state: "rejected" });
  expect(create).not.toHaveBeenCalled();
});
