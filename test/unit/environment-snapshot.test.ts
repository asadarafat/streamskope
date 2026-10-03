import { expect, it, vi } from "vitest";

import type { KafkaActiveConnection } from "../../src/features/kafka/application";
import {
  EnvironmentService,
  captureEnvironment,
} from "../../src/features/kafka/application/environment-service";
import {
  environmentDiff,
  exportEnvironment,
  parseEnvironmentSnapshot,
  type EnvironmentInput,
  type EnvironmentSnapshot,
} from "../../src/features/kafka/contracts/environment-snapshot";
import type { KafkaTopicConfigurationEntry } from "../../src/features/kafka/contracts";

function entry(value: string): KafkaTopicConfigurationEntry {
  return {
    name: "retention.ms",
    value,
    isDefault: false,
    isSensitive: false,
    readOnly: false,
    source: "topic",
    type: "long",
    synonyms: [],
    documentation: null,
  };
}
class Connection implements KafkaActiveConnection {
  closeCalls = 0;
  close(): Promise<void> {
    this.closeCalls++;
    return Promise.resolve();
  }
  describeBrokerConfiguration(): never {
    throw new Error("Unexpected broker query");
  }
  listTopics(): Promise<readonly string[]> {
    return Promise.resolve(Object.keys(this.values));
  }
  openMessageStream(): never {
    throw new Error("Unexpected stream");
  }
  values: Record<string, string> = { a: "1000", b: "1000" };
  topicId = "12345678-1234-1234-1234-123456789012";
  mode = "ok";
  writes: string[] = [];
  describeClusterMetadata(): Promise<{ clusterId: string; brokers: []; controllerId: null }> {
    return Promise.resolve({ clusterId: "target-cluster", brokers: [], controllerId: null });
  }
  describeTopicIdentity(): Promise<{ clusterId: string; topicId: string; partitions: number }> {
    return Promise.resolve({ clusterId: "target-cluster", topicId: this.topicId, partitions: 1 });
  }
  describeTopicConfiguration(topic: string): Promise<readonly KafkaTopicConfigurationEntry[]> {
    return Promise.resolve([
      entry(this.values[topic]!),
      { ...entry("private-token"), name: "password", type: "password", isSensitive: true },
    ]);
  }
  alterTopicConfiguration(
    topic: string,
    changes: readonly { name: string; value: string }[],
    validate: boolean,
  ): Promise<void> {
    if (validate) return Promise.resolve();
    this.writes.push(topic);
    if (this.mode === "fail-second" && topic === "b")
      return Promise.reject(new Error("lost receipt"));
    if (this.mode !== "readback") this.values[topic] = changes[0]!.value;
    return Promise.resolve();
  }
}
async function fixture(): Promise<{
  connection: Connection;
  service: EnvironmentService;
  input: EnvironmentInput;
  expire(): void;
  disconnect(): void;
}> {
  const connection = new Connection();
  let context: { connection: Connection; connectionName: string; generation: number } | null = {
    connection,
    connectionName: "Current",
    generation: 1,
  };
  let now = 1000;
  const service = new EnvironmentService(
    () => context,
    undefined,
    () => now,
  );
  const target = await service.capture(["b", "a"], null);
  const source: EnvironmentSnapshot = {
    ...target,
    clusterId: "source-cluster",
    topics: target.topics.map((t) => ({
      ...t,
      configs: t.configs.map((c) => (c.key === "retention.ms" ? { ...c, value: "2000" } : c)),
    })),
  };
  return {
    connection,
    service,
    input: {
      source,
      target,
      targetProfile: null,
      selected: [
        { topic: "a", key: "retention.ms" },
        { topic: "b", key: "retention.ms" },
      ],
    },
    expire: (): void => {
      now += 120001;
    },
    disconnect: (): void => {
      context = null;
    },
  };
}
it("exports a deterministic, versioned secret-free document and compares values independently of capture time", async () => {
  const f = await fixture();
  const text = exportEnvironment(f.input.target);
  expect(text).not.toContain("private-token");
  expect(text).not.toContain("password");
  expect(JSON.parse(text) as unknown).toMatchObject({
    format: "streamskope.topic-config/v1",
    clusterId: "target-cluster",
    topics: [{ name: "a" }, { name: "b" }],
  });
  expect(
    exportEnvironment({ ...f.input.target, topics: [...f.input.target.topics].reverse() }),
  ).toBe(text);
  expect(environmentDiff(f.input.source, f.input.target)).toEqual(
    ["a", "b"].map((topic) => ({
      topic,
      key: "retention.ms",
      source: "2000",
      target: "1000",
      supported: true,
      reason: "Existing mutable topic setting",
    })),
  );
  expect(
    environmentDiff(f.input.target, { ...f.input.target, observedAt: "2020-01-01T00:00:00.000Z" }),
  ).toEqual([]);
  expect(() => parseEnvironmentSnapshot({ ...f.input.target, credentials: "secret" })).toThrow();
  expect(() => parseEnvironmentSnapshot({ ...f.input.target, format: "v2" })).toThrow();
});
it("promotes only selected differences and coalesces repeated apply without background reconciliation", async () => {
  const f = await fixture();
  const p = await f.service.review({ ...f.input, selected: [f.input.selected[0]!] });
  expect(f.connection.writes).toEqual([]);
  const result = await f.service.apply(p.planId, p.confirmation);
  expect(result.results).toEqual([{ topic: "a", state: "acknowledged", verified: true }]);
  expect(f.connection.values).toEqual({ a: "2000", b: "1000" });
  expect(await f.service.apply(p.planId, p.confirmation)).toEqual(result);
  expect(f.connection.writes).toEqual(["a"]);
});
it("rejects configuration drift, replaced topic identities, expiration and lost source connection", async () => {
  for (const mode of ["value", "identity", "expiry", "connection"]) {
    const f = await fixture();
    const p = await f.service.review(f.input);
    if (mode === "value") f.connection.values.a = "3000";
    if (mode === "identity") f.connection.topicId = "recreated-topic";
    if (mode === "expiry") f.expire();
    if (mode === "connection") f.disconnect();
    if (mode === "value" || mode === "identity")
      expect((await f.service.apply(p.planId, p.confirmation)).results[0]?.state).toBe("rejected");
    else await expect(f.service.apply(p.planId, p.confirmation)).rejects.toThrow();
    expect(f.connection.writes).toEqual([]);
  }
});
it("retains successful partial results, marks uncertain dispatch and leaves subsequent writes unsent", async () => {
  const f = await fixture();
  f.connection.mode = "fail-second";
  const p = await f.service.review(f.input);
  expect((await f.service.apply(p.planId, p.confirmation)).results).toEqual([
    { topic: "a", state: "acknowledged", verified: true },
    { topic: "b", state: "unknown", verified: false },
  ]);
  const g = await fixture();
  g.connection.mode = "readback";
  const plan = await g.service.review(g.input);
  expect((await g.service.apply(plan.planId, plan.confirmation)).results).toEqual([
    { topic: "a", state: "acknowledged", verified: false },
    { topic: "b", state: "unsent", verified: false },
  ]);
});
it("rejects a target changed during Kafka validation before dispatching any write", async () => {
  const f = await fixture();
  const plan = await f.service.review(f.input);
  const original = f.connection.alterTopicConfiguration.bind(f.connection);
  vi.spyOn(f.connection, "alterTopicConfiguration").mockImplementation(
    (topic, changes, validate) => {
      if (validate) f.connection.values[topic] = "9000";
      return original(topic, changes, validate);
    },
  );
  expect((await f.service.apply(plan.planId, plan.confirmation)).results).toEqual([
    { topic: "a", state: "rejected", verified: false },
    { topic: "b", state: "unsent", verified: false },
  ]);
  expect(f.connection.writes).toEqual([]);
});
it("opens and closes saved destinations without switching the source and refuses stale destination profiles", async () => {
  const source = new Connection(),
    target = new Connection();
  let current = true;
  const close = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  const open = vi
    .fn()
    .mockImplementation(() =>
      Promise.resolve({ connection: target, name: "Other", current: () => current, close }),
    );
  const service = new EnvironmentService(
    () => ({ connection: source, generation: 1, connectionName: "Source" }),
    { open },
  );
  await service.capture(["a"], { id: "profile", revision: 2 });
  expect(close).toHaveBeenCalledOnce();
  current = false;
  await expect(service.capture(["a"], { id: "profile", revision: 2 })).rejects.toThrow();
  expect(close).toHaveBeenCalledTimes(2);
  expect(source.closeCalls).toBe(0);
});
it("refuses unknown identity, unsafe values and excessive scope", async () => {
  const c = new Connection();
  c.topicId = "00000000-0000-0000-0000-000000000000";
  await expect(captureEnvironment(c, ["a"], AbortSignal.timeout(1000))).rejects.toThrow();
  await expect(
    captureEnvironment(
      c,
      Array.from({ length: 21 }, (_, i) => String(i)),
      AbortSignal.timeout(1000),
    ),
  ).rejects.toThrow();
});
