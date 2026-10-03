import { expect, it, vi } from "vitest";

import type { KafkaActiveConnection } from "../../src/features/kafka/application";
import { ObservationService } from "../../src/features/kafka/application/observation-service";
import {
  MemoryObservationStore,
  retainObservations,
} from "../../src/features/kafka/application/observation-store";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
  type KafkaConsumerGroupDetails,
} from "../../src/features/kafka/contracts";
import { parseObservationHistory } from "../../src/features/kafka/contracts/observation-validation";
import {
  OBSERVATION_LIMITS,
  observationLag,
  type TopicHealth,
  type ObservationInput,
} from "../../src/features/kafka/contracts/observations";

const input: ObservationInput = {
  topic: "events",
  groupId: "workers",
  thresholds: { lag: 25, requestMs: null },
};
class Connection implements KafkaActiveConnection {
  health: TopicHealth = {
    clusterId: "cluster-a",
    topicId: "topic-a",
    topic: "events",
    brokerCount: 2,
    controllerKnown: true,
    partitions: [0, 1].map((partition) => ({
      partition,
      leader: 1,
      replicas: 2,
      inSyncReplicas: 2,
      endOffset: "100",
    })),
  };
  group: KafkaConsumerGroupDetails = {
    id: "workers",
    state: "stable",
    protocol: "range",
    protocolType: "consumer",
    members: [],
    omittedAssignments: 0,
    omittedMembers: 0,
    omittedOffsets: 0,
    offsets: [0, 1].map((partition) => ({
      topic: "events",
      partition,
      committedOffset: partition ? "80" : "90",
      endOffset: "100",
      lag: partition ? "20" : "10",
    })),
  };
  failGroup = false;
  observeTopicHealth = vi.fn((): Promise<TopicHealth> => Promise.resolve(this.health));
  describeConsumerGroup(): Promise<KafkaConsumerGroupDetails> {
    return this.failGroup
      ? Promise.reject(new Error("private server error"))
      : Promise.resolve(this.group);
  }
  close(): Promise<void> {
    return Promise.resolve();
  }
  alterTopicConfiguration(): never {
    throw new Error("No writes during observation.");
  }
  describeBrokerConfiguration(): never {
    throw new Error("No configuration required.");
  }
  describeClusterMetadata(): never {
    throw new Error("Use selected provider.");
  }
  describeTopicConfiguration(): never {
    throw new Error("No configuration required.");
  }
  listTopics(): never {
    throw new Error("No full inventory required.");
  }
  openMessageStream(): never {
    throw new Error("No record reads during metadata collection.");
  }
}
function fixture(): {
  connection: Connection;
  store: MemoryObservationStore;
  service: ObservationService;
  advance(ms?: number): void;
  disconnect(): void;
} {
  const connection = new Connection(),
    store = new MemoryObservationStore();
  let now = 1_800_000_000_000;
  let context: { connection: Connection; generation: number; connectionName: string } | null = {
    connection,
    generation: 1,
    connectionName: "Fixture",
  };
  const service = new ObservationService(
    () => context,
    store,
    () => now,
  );
  return {
    connection,
    store,
    service,
    advance: (ms = 10_000): void => {
      now += ms;
    },
    disconnect: (): void => {
      context = null;
    },
  };
}
it("records exact selected-topic lag, provenance, bounded work and configured thresholds without writes", async () => {
  const f = fixture();
  const result = await f.service.capture(input),
    sample = result.series.samples[0]!;
  expect(observationLag(sample)).toBe(30);
  expect(sample).toMatchObject({
    source: "kafka-api",
    groupCoverage: "complete",
    providerCalls: 2,
    alerts: [{ metric: "lag", observed: 30, threshold: 25 }],
  });
  expect(result).toMatchObject({
    durability: "session",
    series: { clusterId: "cluster-a", topicId: "topic-a", topic: "events", groupId: "workers" },
  });
  expect(JSON.stringify(result)).not.toMatch(/private server error|clientHost|payload|password/);
  await expect(f.service.capture(input)).rejects.toThrow("ten seconds");
  expect(f.connection.observeTopicHealth).toHaveBeenCalledTimes(1);
  f.advance();
  expect((await f.service.capture(input)).series.samples).toHaveLength(2);
});
it("unavailable, omitted and ahead-of-end offsets are unknown and cannot trigger lag alerts", async () => {
  for (const mode of ["denied", "partial", "ahead", "missing"]) {
    const f = fixture();
    if (mode === "denied") f.connection.failGroup = true;
    if (mode === "partial") f.connection.group = { ...f.connection.group, omittedOffsets: 1 };
    if (mode === "ahead")
      f.connection.group = {
        ...f.connection.group,
        offsets: f.connection.group.offsets.map((p) => ({ ...p, committedOffset: "101" })),
      };
    if (mode === "missing")
      f.connection.health = {
        ...f.connection.health,
        partitions: f.connection.health.partitions.map((p) => ({ ...p, endOffset: null })),
      };
    const sample = (await f.service.capture(input)).series.samples[0]!;
    expect(observationLag(sample)).toBeNull();
    expect(sample.alerts).toEqual([]);
    expect(sample.state).toBe("partial");
  }
});
it("coalesces no concurrent collection, discards cancellation and connection changes, and does not retain stale results", async () => {
  for (const mode of ["cancel", "disconnect"]) {
    const f = fixture();
    let release!: (v: TopicHealth) => void;
    f.connection.observeTopicHealth.mockImplementation(
      () =>
        new Promise((resolve) => {
          release = resolve;
        }),
    );
    const pending = f.service.capture(input);
    const rejection = expect(pending).rejects.toThrow();
    await vi.waitFor(() => expect(release).toBeTypeOf("function"));
    await expect(f.service.capture(input)).rejects.toThrow("already running");
    if (mode === "cancel") f.service.cancel();
    else f.disconnect();
    release(f.connection.health);
    await rejection;
    expect((await f.service.history()).series).toEqual([]);
  }
});
it("breaks continuity after failed reads, cancellation and process restart while preserving valid history", async () => {
  const f = fixture();
  const first = (await f.service.capture(input)).series.samples[0]!;
  f.advance();
  f.connection.observeTopicHealth.mockRejectedValueOnce(new Error("failed read"));
  await expect(f.service.capture(input)).rejects.toThrow();
  f.advance();
  const after = (await f.service.capture(input)).series.samples.at(-1)!;
  expect(after.segmentId).not.toBe(first.segmentId);
  f.service.cancel();
  f.advance();
  const stopped = (await f.service.capture(input)).series.samples.at(-1)!;
  expect(stopped.segmentId).not.toBe(after.segmentId);
  const restarted = new ObservationService(
    () => null,
    f.store,
    () => stopped.observedAt,
  );
  expect((await restarted.history()).series[0]?.samples).toHaveLength(3);
});
it("bounds retention by samples, identities, time and bytes; unknown fields and forged history are refused", async () => {
  const f = fixture();
  const series = (await f.service.capture(input)).series;
  const first = series.samples[0]!;
  const many = {
    ...series,
    samples: Array.from({ length: 400 }, (_, i) => ({
      ...first,
      id: String(i),
      startedAt: first.startedAt + i * 10000,
      observedAt: first.observedAt + i * 10000,
    })),
  };
  const now = many.samples.at(-1)!.observedAt;
  const retained = retainObservations({ schemaVersion: 1, series: [] }, now, many);
  expect(retained.series[0]?.samples).toHaveLength(240);
  const histories = Array.from({ length: 12 }, (_, i) => ({ ...many, topic: `topic-${i}` }));
  const bounded = retainObservations({ schemaVersion: 1, series: histories }, now);
  expect(bounded.series).toHaveLength(8);
  expect(Buffer.byteLength(JSON.stringify(bounded))).toBeLessThanOrEqual(
    OBSERVATION_LIMITS.fileBytes,
  );
  expect(() => parseObservationHistory(bounded)).not.toThrow();
  expect(retainObservations(bounded, now + 86_400_001).series).toEqual([]);
  expect(() => parseObservationHistory({ ...bounded, credentials: "secret" })).toThrow();
  expect(() => parseObservationHistory({ schemaVersion: 1, series: [series, series] })).toThrow();
  expect(() =>
    parseObservationHistory({ schemaVersion: 1, series: [{ ...series, samples: [first, first] }] }),
  ).toThrow();
  expect((await f.service.clear()).series).toEqual([]);
});
it("validates commands, exact destructive confirmation and structured responses at both host boundaries", async () => {
  const f = fixture(),
    capture = await f.service.capture(input);
  const base = { version: HOST_PROTOCOL_VERSION, id: "capture", command: "observations.capture" };
  expect(parseHostCommand({ ...base, payload: input })).toMatchObject({ payload: input });
  expect(() =>
    parseHostCommand({ ...base, payload: { ...input, credentials: "secret" } }),
  ).toThrow();
  expect(() =>
    parseHostCommand({
      ...base,
      command: "observations.clear",
      payload: { confirmation: "clear" },
    }),
  ).toThrow();
  expect(
    parseHostCommandResponse({ ...base, ok: true, result: { correlationId: "c", capture } }),
  ).toMatchObject({ ok: true, result: { capture } });
  expect(() =>
    parseHostCommandResponse({
      ...base,
      ok: true,
      result: { correlationId: "c", capture: { ...capture, rawKeys: ["secret"] } },
    }),
  ).toThrow();
});
