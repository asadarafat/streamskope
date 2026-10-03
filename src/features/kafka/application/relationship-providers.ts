import { RELATIONSHIP_LIMITS as limits, type RelationshipInput } from "../contracts/relationships";

import type { SchemaRegistryPort } from "./schema-registry-types";
import type { KafkaActiveConnection } from "./types";
import type { ConnectPort } from "./connect-service";
import { RelationshipBuilder } from "./relationship-graph";

export async function collectGroupRelationships(
  graph: RelationshipBuilder,
  connection: KafkaActiveConnection,
  topics: readonly string[],
  signal: AbortSignal,
): Promise<void> {
  let inspected = 0,
    omitted: number | null = null,
    failed = 0;
  try {
    if (!connection.listConsumerGroups || !connection.describeConsumerGroup)
      throw new Error("Unavailable group API.");
    const inventory = await connection.listConsumerGroups(signal);
    const selected = [...inventory.groups]
      .filter((g) => g.protocolType === "consumer" || g.protocolType === "")
      .sort((a, b) => a.id.localeCompare(b.id, "en-US"))
      .slice(0, limits.groups);
    omitted = inventory.omittedGroups + Math.max(0, inventory.groups.length - selected.length);
    for (const item of selected) {
      signal.throwIfAborted();
      try {
        const group = await connection.describeConsumerGroup(item.id, signal);
        inspected++;
        if (group.id !== item.id) {
          failed++;
          continue;
        }
        if (group.omittedAssignments || group.omittedMembers || group.omittedOffsets) failed++;
        for (const topic of topics) {
          const assigned = group.members.some((m) =>
            m.assignments.some((a) => a.topic === topic && a.partitions.length),
          );
          const committed = group.offsets.some(
            (o) => o.topic === topic && o.committedOffset !== null,
          );
          if (!assigned && !committed) continue;
          const from = graph.node("topic", topic),
            to = graph.node("group", group.id);
          if (assigned)
            graph.edge(
              from,
              to,
              "assigned",
              "observed",
              "Kafka groups",
              "Group description contains a current partition assignment. Assignment does not establish successful processing.",
            );
          if (committed)
            graph.edge(
              from,
              to,
              "committed",
              "observed",
              "Kafka groups",
              "The group has a committed offset for this topic. This may be historical or manually reset; it does not establish a live consumer.",
            );
        }
      } catch {
        signal.throwIfAborted();
        failed++;
      }
    }
  } catch {
    signal.throwIfAborted();
    failed++;
  }
  graph.coverage.push({
    source: "Kafka groups",
    state: failed ? (inspected ? "limited" : "unavailable") : omitted ? "limited" : "complete",
    inspected,
    omitted,
    detail: `${failed} unavailable/incomplete group reads. At most ${limits.groups} API-visible consumer-protocol/empty groups are inspected in name order; other protocols are omitted and authorization may hide others. Member identities are discarded.`,
  });
}
export async function collectConnectRelationships(
  graph: RelationshipBuilder,
  connection: KafkaActiveConnection,
  port: ConnectPort | undefined,
  clusterId: string,
  topics: readonly string[],
  signal: AbortSignal,
): Promise<void> {
  const context = connection.clusterServiceContext?.("connect");
  let inspected = 0,
    omitted: number | null = null,
    failed = 0;
  if (!context || !port?.relationships) {
    graph.coverage.push({
      source: "Kafka Connect",
      state: "not-configured",
      inspected,
      omitted,
      detail:
        "Configure a supported Connect endpoint in this profile. Other producers and connector clusters remain unknown.",
    });
    return;
  }
  try {
    const reportedCluster = await port.clusterId?.(context, signal);
    if (reportedCluster !== clusterId) {
      graph.coverage.push({
        source: "Kafka Connect",
        state: "unavailable",
        inspected: 0,
        omitted: null,
        detail:
          "Connect cluster identity is missing or differs from the current Kafka cluster. No connector relationships were associated with these topics.",
      });
      return;
    }
    const inventory = await port.list(context, signal);
    const names = [...inventory.names].sort().slice(0, limits.connectors);
    omitted = inventory.names.length - names.length;
    for (const name of names) {
      signal.throwIfAborted();
      try {
        const result = await port.relationships(context, name, signal);
        inspected++;
        if (result.reportedTopics === null || result.regexSubscription) failed++;
        for (const topic of topics) {
          const reported = result.reportedTopics?.includes(topic),
            configured = result.configuredTopics.includes(topic);
          if (!reported && !configured) continue;
          const connector = graph.node("connector", name),
            topicId = graph.node("topic", topic);
          if (reported)
            graph.edge(
              result.type === "sink" ? topicId : connector,
              result.type === "sink" ? connector : topicId,
              result.type === "sink"
                ? "reads"
                : result.type === "source"
                  ? "writes"
                  : "reports-topic",
              "observed",
              "Kafka Connect",
              "Connect topic tracking reports use since connector creation or tracking reset. It is not evidence of current traffic; worker tracking may be disabled or stale.",
            );
          if (configured)
            graph.edge(
              topicId,
              connector,
              "configured",
              "declared",
              "Kafka Connect",
              "The connector's explicit topics configuration names this topic. Configuration does not prove record flow.",
            );
        }
      } catch {
        signal.throwIfAborted();
        failed++;
      }
    }
  } catch {
    signal.throwIfAborted();
    failed++;
  }
  graph.coverage.push({
    source: "Kafka Connect",
    state: failed ? (inspected ? "limited" : "unavailable") : omitted ? "limited" : "complete",
    inspected,
    omitted,
    detail: `${failed} unavailable/incomplete reads or regex subscriptions. At most ${limits.connectors} connectors on this endpoint; regexes are not evaluated and other producers remain unknown.`,
  });
}
export async function collectSchemaRelationships(
  graph: RelationshipBuilder,
  connection: KafkaActiveConnection,
  port: SchemaRegistryPort | undefined,
  input: RelationshipInput,
  signal: AbortSignal,
): Promise<string | null> {
  const context = connection.clusterServiceContext?.("schemaRegistry");
  const target = input.subject === null ? null : graph.node("schema", input.subject, input.version);
  if (!context || !port) {
    graph.coverage.push({
      source: "Schema Registry",
      state: "not-configured",
      inspected: 0,
      omitted: null,
      detail:
        "Configure Schema Registry for reference discovery and schema impact. A requested version without a successful read is unresolved.",
    });
    return target;
  }
  let inspected = 0,
    failed = 0,
    omitted: number | null = null;
  const queue: { subject: string; version: number | "latest" }[] =
    input.subject !== null && input.version !== null
      ? [{ subject: input.subject, version: input.version }]
      : [];
  const seen = new Set<string>();
  try {
    const inventory = await port.listSubjects(context, signal);
    const subjects = [...inventory.subjects].sort().slice(0, limits.subjects);
    omitted = inventory.omittedSubjects + inventory.subjects.length - subjects.length;
    queue.push(...subjects.map((subject) => ({ subject, version: "latest" as const })));
  } catch {
    signal.throwIfAborted();
    failed++;
  }
  while (queue.length && seen.size < limits.schemaReads) {
    signal.throwIfAborted();
    const identity = queue.shift()!,
      key = JSON.stringify(identity);
    if (seen.has(key)) continue;
    seen.add(key);
    try {
      const detail = await port.loadSubject(context, identity, signal),
        schema = detail.schema;
      if (
        schema.subject !== identity.subject ||
        (identity.version !== "latest" && schema.version !== identity.version)
      )
        throw new Error("Registry identity changed.");
      inspected++;
      const node = graph.node("schema", schema.subject, schema.version),
        id = graph.node("schema-id", String(schema.id));
      graph.edge(
        id,
        node,
        "registered-id",
        "declared",
        "Schema Registry",
        "Registry associates this schema ID with the returned subject version. ID reuse across subjects does not identify a consumer's reader schema.",
      );
      for (const reference of schema.references) {
        graph.edge(
          node,
          graph.node("schema", reference.subject, reference.version),
          "references",
          "declared",
          "Schema Registry",
          "This exact schema version declares a dependency on the referenced subject/version. The dependency may remain unresolved within the read budget.",
        );
        if (queue.length < limits.schemaReads * 2)
          queue.push({ subject: reference.subject, version: reference.version });
        else graph.truncated = true;
      }
      for (const topic of input.topics)
        if (schema.subject === `${topic}-value` || schema.subject === `${topic}-key`)
          graph.edge(
            graph.node("topic", topic),
            node,
            "naming-match",
            "inferred",
            "Schema Registry",
            "TopicNameStrategy naming convention only; the topic may use a different subject strategy or older writer version. No record use is established by this name.",
          );
    } catch {
      signal.throwIfAborted();
      failed++;
    }
  }
  graph.coverage.push({
    source: "Schema Registry",
    state: inspected ? "limited" : "unavailable",
    inspected,
    omitted,
    detail: `${failed} failed reads; ${queue.length} queued identities left. Only the first ${limits.subjects} visible subjects' latest versions, the exact requested version and bounded references (${limits.schemaReads} reads) are inspected. Historical dependent versions, hidden subjects and external consumers remain unknown.`,
  });
  return target;
}
