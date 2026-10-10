import { randomUUID } from "node:crypto";

import { Producer } from "@platformatic/kafka";
import { expect, it } from "vitest";

import { KafkaApplicationSession } from "../../src/features/kafka/application";
import { RelationshipService } from "../../src/features/kafka/application/relationship-service";
import { schemaImpact } from "../../src/features/kafka/contracts/relationships";
import { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import { ConnectHttpAdapter } from "../../src/features/kafka/engine/connect-http";
import { SchemaRegistryHttpAdapter } from "../../src/features/kafka/engine/schema-registry-http";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
import { startAuthorizationFixture } from "../support/kafka-authorization-fixture";
import { startConnectFixture } from "../support/connect-fixture";
import {
  loadFixtureConnection,
  loadFixtureConfig,
  fetchFixtureToken,
} from "../support/kafka-fixture";

it("discovers actual Connect/group relationships and Registry reference impact from protected Kafka framing candidates", async () => {
  const fixture = await loadFixtureConnection(),
    config = await loadFixtureConfig();
  if (!fixture.schemaRegistryEndpoint) throw new Error("A live Registry fixture is required.");
  const kafka = await startAuthorizationFixture();
  const worker = await startConnectFixture(kafka.connection.brokers[0]!).catch(
    async (error: unknown) => {
      await kafka.dispose();
      throw error;
    },
  );
  const registry = new SchemaRegistryHttpAdapter(new NodeBoundedJsonHttp()),
    connect = new ConnectHttpAdapter(new NodeBoundedJsonHttp());
  const registryContext = {
    baseUrl: fixture.schemaRegistryEndpoint,
    authorization: async (): Promise<string> =>
      `Bearer ${await fetchFixtureToken(fixture, config)}`,
  };
  const connectContext = worker.context;
  const topic = `a-streamskope-lineage-${randomUUID()}`,
    base = `${topic}-base`,
    subject = `${topic}-value`,
    name = "lineage-sink";
  const created = new Set<string>();
  const session = new KafkaApplicationSession(new StreamSkopeKafkaEngine());
  const producer = new Producer({
    clientId: "relationship-fixture",
    bootstrapBrokers: [...kafka.connection.brokers],
    retries: 0,
    autocreateTopics: false,
  });
  const signal = AbortSignal.timeout(120_000);
  try {
    await kafka.admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
    await registry.register(
      registryContext,
      {
        subject: base,
        version: "latest",
        normalize: false,
        schemaType: "PROTOBUF",
        schema: 'syntax = "proto3"; message Base { string label = 1; }',
        references: [],
      },
      signal,
    );
    created.add(base);
    const registered = await registry.register(
      registryContext,
      {
        subject,
        version: "latest",
        normalize: false,
        schemaType: "PROTOBUF",
        schema: 'syntax = "proto3"; import "base.proto"; message Event { Base base = 1; }',
        references: [{ name: "base.proto", subject: base, version: 1 }],
      },
      signal,
    );
    created.add(subject);
    expect(
      await connect.apply(
        connectContext,
        {
          name,
          action: "create",
          config: {
            "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
            "tasks.max": "1",
            topics: topic,
            file: "/tmp/streamskope-lineage.txt",
          },
        },
        signal,
      ),
    ).toMatchObject({ state: "acknowledged", cleanup: "confirmed" });
    await expect
      .poll(
        async () => (await connect.load(connectContext, name, signal))?.detail.tasks[0]?.state,
        { timeout: 45000 },
      )
      .toBe("RUNNING");
    const frame = Buffer.alloc(6);
    frame.writeUInt32BE(registered.id, 1);
    await producer.send({
      messages: Array.from({ length: 20 }, () => ({
        topic,
        key: Buffer.from("fixture-key"),
        value: frame,
      })),
    });
    await expect
      .poll(
        async () => (await connect.relationships(connectContext, name, signal)).reportedTopics,
        { timeout: 20000 },
      )
      .toEqual([topic]);
    await session.connect(kafka.connection);
    const owner = session.writeContext()!;
    const coordination = [...(await kafka.admin.listGroups()).values()].find(
      (g) => g.protocolType === "connect",
    );
    expect(coordination).toBeDefined();
    await expect(owner.connection.describeConsumerGroup!(coordination!.id)).rejects.toThrow(
      "unsupported coordination protocol",
    );
    await expect(
      owner.connection.offsetResetSnapshot!({
        groupId: coordination!.id,
        targets: [{ topic, partition: 0, offset: "0" }],
      }),
    ).rejects.toThrow("unsupported coordination protocol");
    Object.assign(owner.connection, {
      clusterServiceContext: (service: string) =>
        service === "connect"
          ? connectContext
          : service === "schemaRegistry"
            ? registryContext
            : null,
    });
    const service = new RelationshipService(() => session.writeContext(), connect, registry);
    const graph = await service.capture({
      topics: [topic],
      subject: base,
      version: 1,
      sampleRecords: true,
    });
    expect(graph.edges.some((e) => e.relation === "references")).toBe(true);
    expect(graph.edges.some((e) => e.relation === "framed-id" && e.evidence === "inferred")).toBe(
      true,
    );
    expect(graph.edges.some((e) => e.relation === "reads" && e.source === "Kafka Connect")).toBe(
      true,
    );
    expect(graph.edges.some((e) => e.relation === "assigned" && e.source === "Kafka groups")).toBe(
      true,
    );
    const impact = schemaImpact(graph).map(
      (p) => graph.nodes.find((n) => n.id === p.nodeId)?.label,
    );
    expect(impact).toEqual(expect.arrayContaining([subject, topic, name, `connect-${name}`]));
    expect(graph.coverage.find((c) => c.source === "Protected record sample")?.inspected).toBe(20);
    expect(graph.coverage.find((c) => c.source === "Schema Registry")?.state).toBe("limited");
    expect(JSON.stringify(graph)).not.toContain(config.oauthClientSecret);
  } finally {
    await producer.close();
    await session.disconnect();
    await worker.dispose();
    await kafka.dispose();
    for (const owned of [subject, base])
      if (created.has(owned))
        await registry.delete(
          registryContext,
          { mode: "permanent", target: { kind: "subject", subject: owned }, confirmation: owned },
          AbortSignal.timeout(15000),
        );
  }
}, 180_000);
