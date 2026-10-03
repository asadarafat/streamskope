import { expect, it, vi } from "vitest";

import { RelationshipService } from "../../src/features/kafka/application/relationship-service";
import { RelationshipBuilder } from "../../src/features/kafka/application/relationship-graph";
import {
  parseRelationshipGraph,
  parseRelationshipInput,
  schemaImpact,
  RELATIONSHIP_LIMITS,
} from "../../src/features/kafka/contracts/relationships";
import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
} from "../../src/features/kafka/contracts";
import { relationshipFixture } from "../support/relationship-fixture";
import { OBSERVED_AT } from "../support/observation-fixture";
const input = { topics: ["events"], subject: "base", version: 1, sampleRecords: true };
it("derives bounded source/topic/group lineage and transitive schema impact with provenance and no secrets", async () => {
  const f = relationshipFixture(),
    graph = await f.service.capture(input);
  expect(graph.edges.map((e) => e.relation)).toEqual(
    expect.arrayContaining([
      "references",
      "registered-id",
      "naming-match",
      "assigned",
      "committed",
      "writes",
      "configured",
      "framed-id",
    ]),
  );
  expect(graph.edges.every((e) => e.source && e.observedAt === OBSERVED_AT && e.detail)).toBe(true);
  expect(
    graph.edges
      .filter((e) => ["framed-id", "naming-match"].includes(e.relation))
      .every((e) => e.evidence === "inferred"),
  ).toBe(true);
  const affected = schemaImpact(graph).map(
    (i) => graph.nodes.find((n) => n.id === i.nodeId)?.label,
  );
  expect(affected).toEqual(expect.arrayContaining(["events-value", "events", "workers", "source"]));
  expect(affected).not.toContain("unrelated");
  expect(
    schemaImpact(graph).every((i) => i.path.length && i.path.every((p) => graph.edges[p])),
  ).toBe(true);
  expect(JSON.stringify(graph)).not.toMatch(/private|Bearer|cHJpdmF0ZQ/);
  expect(graph.coverage.find((c) => c.source === "Schema Registry")?.state).toBe("limited");
});
it("reports hidden, denied, missing and unconfigured providers without inventing links or exposing remote error text", async () => {
  const f = relationshipFixture();
  f.connection.listConsumerGroups = (): ReturnType<
    NonNullable<typeof f.connection.listConsumerGroups>
  > => Promise.reject(new Error("secret group error"));
  f.connection.clusterServiceContext = (): ReturnType<
    NonNullable<typeof f.connection.clusterServiceContext>
  > => null;
  const graph = await f.service.capture({ ...input, sampleRecords: false });
  expect(graph.edges).toEqual([]);
  expect(schemaImpact(graph)).toEqual([]);
  expect(graph.coverage.map((c) => c.state)).toEqual(
    expect.arrayContaining(["unavailable", "not-configured", "not-requested"]),
  );
  expect(JSON.stringify(graph)).not.toContain("secret");
});
it("caps group, connector and schema discovery and still marks unknown coverage", async () => {
  const f = relationshipFixture();
  const describe = vi.fn(f.connection.describeConsumerGroup?.bind(f.connection));
  f.connection.describeConsumerGroup = describe;
  f.connection.listConsumerGroups = (): ReturnType<
    NonNullable<typeof f.connection.listConsumerGroups>
  > =>
    Promise.resolve({
      groups: Array.from({ length: 30 }, (_, i) => ({
        id: `g${i}`,
        state: "stable",
        protocolType: "consumer",
        groupType: "consumer",
      })),
      omittedGroups: 5,
    });
  const connections = vi.fn(f.connect.relationships?.bind(f.connect));
  f.connect.relationships = connections;
  f.connect.list = (): ReturnType<NonNullable<typeof f.connect.list>> =>
    Promise.resolve({ names: Array.from({ length: 15 }, (_, i) => `c${i}`), plugins: [] });
  const load = vi.fn(f.registry.loadSubject.bind(f.registry));
  f.registry.loadSubject = load;
  f.registry.listSubjects = (): ReturnType<NonNullable<typeof f.registry.listSubjects>> =>
    Promise.resolve({
      subjects: Array.from({ length: 30 }, (_, i) => `s${i}`),
      omittedSubjects: 2,
    });
  const graph = await f.service.capture({ ...input, sampleRecords: false });
  expect(describe).toHaveBeenCalledTimes(20);
  expect(connections).toHaveBeenCalledTimes(10);
  expect(load.mock.calls.length).toBeLessThanOrEqual(24);
  expect(graph.coverage.find((c) => c.source === "Kafka groups")?.omitted).toBe(15);
  expect(graph.coverage.find((c) => c.source === "Kafka Connect")?.omitted).toBe(5);
  expect(graph.coverage.find((c) => c.source === "Schema Registry")?.omitted).toBe(12);
  const builder = new RelationshipBuilder(() => OBSERVED_AT);
  for (let i = 0; i < 100; i++) builder.node("topic", `topic-${i}`);
  expect(builder.nodes).toHaveLength(RELATIONSHIP_LIMITS.nodes);
  expect(builder.truncated).toBe(true);
});
it("cancels concurrent discovery and discards results when the connection changes", async () => {
  for (const mode of ["cancel", "disconnect"]) {
    const f = relationshipFixture();
    let release!: () => void;
    const metadata = f.connection.describeClusterMetadata.bind(f.connection);
    f.connection.describeClusterMetadata = (): ReturnType<
      NonNullable<typeof f.connection.describeClusterMetadata>
    > =>
      new Promise<void>((resolve) => {
        release = resolve;
      }).then(() => metadata());
    const pending = f.service.capture(input),
      rejected = expect(pending).rejects.toThrow();
    await expect(f.service.capture(input)).rejects.toThrow("already running");
    if (mode === "cancel") f.service.cancel();
    else f.disconnect();
    release();
    await rejected;
    await f.service.idle();
  }
});
it("validates exact scope and bounded protocol edges, rejecting forged graph endpoints", async () => {
  const graph = await relationshipFixture().service.capture(input);
  const base = { command: "relationships.capture", id: "r", version: HOST_PROTOCOL_VERSION };
  expect(parseHostCommand({ ...base, payload: input })).toMatchObject({ payload: input });
  expect(
    parseHostCommandResponse({ ...base, ok: true, result: { correlationId: "c", graph } }),
  ).toMatchObject({ ok: true });
  for (const v of [
    { ...input, topics: [] },
    { ...input, topics: ["a", "b", "c", "d"] },
    { ...input, version: null },
    { ...input, credentials: "secret" },
  ])
    expect(() => parseRelationshipInput(v)).toThrow();
  expect(() =>
    parseRelationshipGraph({ ...graph, edges: [{ ...graph.edges[0], to: "missing" }] }),
  ).toThrow();
  expect(() =>
    parseRelationshipGraph({ ...graph, nodes: [...graph.nodes, graph.nodes[0]] }),
  ).toThrow();
  expect(() => parseRelationshipGraph({ ...graph, rawConfig: "secret" })).toThrow();
  const service = new RelationshipService(() => null, undefined, undefined);
  await expect(service.capture(input)).rejects.toThrow("Connect Kafka");
});
