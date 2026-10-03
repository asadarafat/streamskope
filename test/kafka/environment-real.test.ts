import { expect, it } from "vitest";

import {
  KafkaApplicationSession,
  KafkaProfileService,
  InMemoryKafkaProfileStore,
} from "../../src/features/kafka/application";
import { EnvironmentService } from "../../src/features/kafka/application/environment-service";
import { SavedReplayDestinations } from "../../src/features/kafka/application/replay-destination";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import {
  environmentDiff,
  exportEnvironment,
  type EnvironmentInput,
} from "../../src/features/kafka/contracts/environment-snapshot";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";

it("captures two real clusters, exports exact diff, promotes one selected setting and blocks later drift", async () => {
  const source = await startAuthorizationFixture();
  const target = await startAuthorizationFixture().catch(async (error: unknown) => {
    await source.dispose();
    throw error;
  });
  const engine = new StreamSkopeKafkaEngine(),
    session = new KafkaApplicationSession(engine);
  const profiles = new KafkaProfileService(
    new InMemoryKafkaProfileStore({ durability: "session", protection: "memory", state: "ready" }),
    { decode: (): Promise<never> => Promise.reject(new Error("No TLS material")) },
  );
  try {
    for (const f of [source, target])
      await f.admin.createTopics({ topics: ["compare-topic"], partitions: 1, replicas: 1 });
    await session.connect(source.connection);
    const connection = session.writeContext()!.connection;
    await connection.alterTopicConfiguration(
      "compare-topic",
      [{ name: "retention.ms", value: "1800000", isSensitive: false }],
      false,
    );
    const created = await profiles.create({
      transport: "plaintext",
      name: "Comparison target",
      brokers: target.connection.brokers,
    });
    const p = created.profiles.find((p) => p.name === "Comparison target")!;
    const service = new EnvironmentService(
      () => session.writeContext(),
      new SavedReplayDestinations(profiles, engine),
    );
    const from = await service.capture(["compare-topic"], null),
      to = await service.capture(["compare-topic"], { id: p.id, revision: p.revision ?? 1 });
    expect(from.clusterId).not.toBe(to.clusterId);
    expect(exportEnvironment(from)).not.toContain("password");
    expect(environmentDiff(from, to).some((d) => d.key === "retention.ms")).toBe(true);
    const input: EnvironmentInput = {
      source: from,
      target: to,
      targetProfile: { id: p.id, revision: p.revision ?? 1 },
      selected: [{ topic: "compare-topic", key: "retention.ms" }],
    };
    const plan = await service.review(input);
    expect((await service.apply(plan.planId, plan.confirmation)).results).toEqual([
      { topic: "compare-topic", state: "acknowledged", verified: true },
    ]);
    expect(session.writeContext()!.connection).toBe(connection);
    const current = await service.capture(["compare-topic"], input.targetProfile);
    expect(environmentDiff(from, current)).toEqual([]);
    const intent = {
      ...from,
      topics: from.topics.map((t) => ({
        ...t,
        configs: t.configs.map((c) => (c.key === "retention.ms" ? { ...c, value: "3600000" } : c)),
      })),
    };
    const stale = await service.review({ ...input, source: intent, target: current });
    const other = await engine.openConnection(target.connection, AbortSignal.timeout(10000));
    try {
      await other.alterTopicConfiguration(
        "compare-topic",
        [{ name: "retention.ms", value: "7200000", isSensitive: false }],
        false,
      );
    } finally {
      await other.close();
    }
    expect((await service.apply(stale.planId, stale.confirmation)).results[0]?.state).toBe(
      "rejected",
    );
  } finally {
    const cleanup = await Promise.allSettled([
      session.disconnect(),
      source.dispose(),
      target.dispose(),
    ]);
    expect(cleanup.every((r) => r.status === "fulfilled")).toBe(true);
  }
}, 180000);
