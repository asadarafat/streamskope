import {
  RELATIONSHIP_LIMITS as limits,
  parseRelationshipGraph,
  parseRelationshipInput,
  type RelationshipInput,
  type RelationshipGraph,
} from "../contracts/relationships";

import type { KafkaApplicationSession } from "./session";
import type { ConnectPort } from "./connect-service";
import type { SchemaRegistryPort } from "./schema-registry-types";
import { RelationshipBuilder } from "./relationship-graph";
import {
  collectConnectRelationships,
  collectGroupRelationships,
  collectSchemaRelationships,
} from "./relationship-providers";
import { sampleObservationRecords } from "./observation-record-sample";

type Context = NonNullable<ReturnType<KafkaApplicationSession["writeContext"]>>;
export class RelationshipService {
  private controller: AbortController | undefined;
  private operation: Promise<RelationshipGraph> | undefined;
  constructor(
    private readonly context: () => Context | null,
    private readonly connect: ConnectPort | undefined,
    private readonly registry: SchemaRegistryPort | undefined,
    private readonly now = Date.now,
  ) {}
  cancel(): void {
    this.controller?.abort();
  }
  idle(): Promise<void> {
    return Promise.allSettled([this.operation]).then(() => undefined);
  }
  capture(value: RelationshipInput): Promise<RelationshipGraph> {
    if (this.operation)
      return Promise.reject(new Error("A relationship discovery is already running."));
    const input = parseRelationshipInput(value),
      context = this.context();
    if (!context) return Promise.reject(new Error("Connect Kafka first."));
    this.controller = new AbortController();
    const signal = AbortSignal.any([
      this.controller.signal,
      AbortSignal.timeout(limits.deadlineMs),
    ]);
    this.operation = this.collect(context, input, signal).finally(() => {
      this.operation = undefined;
      this.controller = undefined;
    });
    return this.operation;
  }
  private async collect(
    context: Context,
    input: RelationshipInput,
    signal: AbortSignal,
  ): Promise<RelationshipGraph> {
    const startedAt = this.now(),
      graph = new RelationshipBuilder(this.now),
      connection = context.connection;
    signal.throwIfAborted();
    const metadata = await connection.describeClusterMetadata(signal);
    signal.throwIfAborted();
    if (!metadata.clusterId) throw new Error("Cluster identity unavailable.");
    const visible = await connection.listTopics(signal);
    signal.throwIfAborted();
    if (input.topics.some((topic) => !visible.includes(topic)))
      throw new Error("Selected topic unavailable.");
    for (const topic of input.topics) graph.node("topic", topic);
    graph.coverage.push({
      source: "Kafka metadata",
      state: "complete",
      inspected: input.topics.length,
      omitted: null,
      detail:
        "Selected topics are visible to this client. Other topics, hidden resources and general producer identities are outside this graph.",
    });
    const target = await collectSchemaRelationships(
      graph,
      connection,
      this.registry,
      input,
      signal,
    );
    await collectGroupRelationships(graph, connection, input.topics, signal);
    await collectConnectRelationships(
      graph,
      connection,
      this.connect,
      metadata.clusterId,
      input.topics,
      signal,
    );
    if (input.sampleRecords) {
      for (const topic of input.topics) {
        const sample = await sampleObservationRecords(
          connection,
          topic,
          this.now(),
          signal,
          (message) => {
            if (message.original?.state !== "complete") return;
            for (const field of ["key", "value"] as const) {
              const value = message.original[field];
              if (value === null) continue;
              const prefix = atob(value.slice(0, 8));
              if (prefix.length < 5 || prefix.charCodeAt(0) !== 0) continue;
              const bytes = Uint8Array.from(prefix, (c) => c.charCodeAt(0)),
                id = new DataView(bytes.buffer).getUint32(1);
              if (id < 1 || id > 2147483647) continue;
              graph.edge(
                graph.node("topic", topic),
                graph.node("schema-id", String(id)),
                "framed-id",
                "inferred",
                "Protected record sample",
                `Confluent-style ${field} header candidate at partition ${message.partition}, offset ${message.offset}. Magic byte and ID are read; payload decoding and reader-schema use are unverified.`,
              );
            }
          },
        );
        graph.coverage.push({
          source: "Protected record sample",
          state:
            sample.state === "complete"
              ? "complete"
              : sample.state === "partial"
                ? "limited"
                : "unavailable",
          inspected: sample.count,
          omitted: null,
          detail: `${topic}: ${sample.reason}; ${sample.bytes} bytes in the preceding minute; at most 200 records, 2 MiB, five seconds. Masked/unavailable originals cannot identify schema IDs; arbitrary bytes can resemble a framing header.`,
        });
      }
    } else
      graph.coverage.push({
        source: "Protected record sample",
        state: "not-requested",
        inspected: 0,
        omitted: null,
        detail: "Record reads are off. Topic/schema naming matches alone do not prove use.",
      });
    signal.throwIfAborted();
    const current = this.context();
    if (!current || current.connection !== connection || current.generation !== context.generation)
      throw new Error("Connection changed.");
    if (graph.truncated)
      graph.coverage.push({
        source: "Kafka metadata",
        state: "limited",
        inspected: graph.nodes.length,
        omitted: null,
        detail: "Graph or reference queue limit reached. Missing relationships remain unknown.",
      });
    return parseRelationshipGraph({
      startedAt,
      observedAt: this.now(),
      clusterId: metadata.clusterId,
      target,
      nodes: graph.nodes,
      edges: graph.edges,
      coverage: graph.coverage,
    });
  }
}
