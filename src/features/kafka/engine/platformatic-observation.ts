import {
  ListOffsetTimestamps,
  ProtocolError,
  ResponseError,
  type ClusterMetadata,
  type metadataV12,
} from "@platformatic/kafka";

import type { ObservationGroupHealth, TopicHealth } from "../contracts/observations";
import { observationIssue } from "../application/observation-errors";

import { KafkaEngineFailure, mapKafkaAdminFailure, mapKafkaConsumerGroupFailure } from "./failure";
import { consumerGroupState, requireConsumerGroupProtocol } from "./platformatic-group-protocol";
import type { PlatformaticAdminClient } from "./platformatic-admin";

/** Kafka can return usable metadata inside a partition leader-unavailable response. */
function leaderUnavailableMetadata(error: unknown, topic: string): ClusterMetadata | null {
  if (
    !(error instanceof ResponseError) ||
    !error.errors.length ||
    !error.errors.every((e: unknown) => e instanceof ProtocolError && e.apiCode === 5)
  )
    return null;
  const response = error.response as metadataV12.MetadataResponse;
  if (
    !response ||
    typeof response.clusterId !== "string" ||
    !response.clusterId ||
    !Array.isArray(response.brokers) ||
    !Array.isArray(response.topics)
  )
    return null;
  const selected = response.topics.find((t) => t.name === topic);
  if (
    !selected ||
    selected.errorCode !== 0 ||
    !selected.topicId ||
    !selected.partitions.length ||
    selected.partitions.length > 128 ||
    selected.partitions.some((p, i) => p.partitionIndex !== i || ![0, 5].includes(p.errorCode))
  )
    return null;
  return {
    id: response.clusterId,
    controllerId: response.controllerId,
    lastUpdate: Date.now(),
    brokers: new Map(
      response.brokers.map((b) => [b.nodeId, { host: b.host, port: b.port, rack: b.rack }]),
    ),
    topics: new Map([
      [
        topic,
        {
          id: selected.topicId,
          lastUpdate: Date.now(),
          partitionsCount: selected.partitions.length,
          partitions: selected.partitions.map((p) => ({
            leader: p.leaderId,
            leaderEpoch: p.leaderEpoch,
            replicas: p.replicaNodes,
            isr: p.isrNodes,
            offlineReplicas: p.offlineReplicas,
          })),
        },
      ],
    ]),
  };
}

export async function observeSelectedTopic(
  admin: PlatformaticAdminClient,
  topic: string,
  signal?: AbortSignal,
): Promise<TopicHealth> {
  signal?.throwIfAborted();
  let metadata: ClusterMetadata;
  try {
    metadata = await admin.metadata({
      forceUpdate: true,
      topics: [topic],
      autocreateTopics: false,
    });
  } catch (error) {
    const salvaged = leaderUnavailableMetadata(error, topic);
    if (!salvaged) {
      if (
        error instanceof ResponseError &&
        error.errors.some(
          (e: unknown) => e instanceof ProtocolError && (e.apiCode === 3 || e.apiCode === 100),
        )
      )
        throw new KafkaEngineFailure({
          cause: error,
          code: "TOPIC_NOT_FOUND",
          stage: "kafka",
          retryable: false,
          summary: "The selected topic is unavailable or was removed.",
          recovery: "Refresh topics and select an existing topic.",
        });
      throw error;
    }
    metadata = salvaged;
  }
  signal?.throwIfAborted();
  const selected = metadata.topics.get(topic);
  if (
    !metadata.id ||
    !selected?.id ||
    !selected.partitions.length ||
    selected.partitions.length > 128
  )
    throw new KafkaEngineFailure({
      code: selected ? "UNSUPPORTED_OPERATION" : "TOPIC_NOT_FOUND",
      stage: "kafka",
      retryable: false,
      summary: selected
        ? "Observation requires Kafka resource identities and 1–128 topic partitions."
        : "The selected topic was not returned by Kafka.",
      recovery: "Refresh topics and select an existing supported topic.",
    });
  let ends = new Map<number, string | null>();
  const issues: NonNullable<TopicHealth["issues"]>[number][] = [];
  const available = selected.partitions.flatMap((p, partitionIndex) =>
    p.leader < 0 ? [] : [{ partitionIndex, timestamp: ListOffsetTimestamps.LATEST }],
  );
  if (available.length)
    try {
      const offsets = await admin.listOffsets({ topics: [{ name: topic, partitions: available }] });
      ends = new Map(
        offsets
          .filter((t) => t.name === topic)
          .flatMap((t) =>
            t.partitions.map(
              (p) => [p.partitionIndex, p.offset >= 0n ? p.offset.toString() : null] as const,
            ),
          ),
      );
    } catch (error) {
      signal?.throwIfAborted();
      issues.push(observationIssue(mapKafkaAdminFailure(error, topic), "end-offsets"));
    }
  signal?.throwIfAborted();
  if (selected.partitions.some((_, i) => ends.get(i) == null) && !issues.length)
    issues.push({
      measurement: "end-offsets",
      code: "OBSERVATION_INCOMPLETE",
      summary:
        "Some partition end positions are unavailable, including partitions without a leader.",
      recovery:
        "Inspect the affected partition leaders and broker availability, then capture again.",
      retryable: true,
    });
  return {
    clusterId: metadata.id,
    topicId: selected.id,
    topic,
    brokerCount: metadata.brokers.size,
    controllerKnown: metadata.brokers.has(metadata.controllerId),
    issues,
    partitions: selected.partitions.map((p, partition) => ({
      partition,
      leader: p.leader >= 0 ? p.leader : null,
      replicas: p.replicas.length,
      inSyncReplicas: p.isr.length,
      endOffset: ends.get(partition) ?? null,
    })),
  };
}

export async function observeSelectedGroup(
  admin: PlatformaticAdminClient,
  groupId: string,
  topic: string,
  partitions: readonly number[],
  signal?: AbortSignal,
): Promise<ObservationGroupHealth> {
  signal?.throwIfAborted();
  await requireConsumerGroupProtocol(admin, groupId);
  signal?.throwIfAborted();
  // Fetch precisely these committed positions. No latest reads or unrelated topic caps.
  const [descriptions, committed] = await Promise.allSettled([
    admin.describeGroups({ groups: [groupId], includeAuthorizedOperations: false }),
    admin.listConsumerGroupOffsets({
      groups: [{ groupId, topics: [{ name: topic, partitionIndexes: [...partitions] }] }],
      requireStable: false,
    }),
  ]);
  signal?.throwIfAborted();
  const issues: NonNullable<ObservationGroupHealth["issues"]>[number][] = [];
  const group = descriptions.status === "fulfilled" ? descriptions.value.get(groupId) : undefined;
  if (descriptions.status === "rejected")
    issues.push(
      observationIssue(mapKafkaConsumerGroupFailure(descriptions.reason, groupId), "group-members"),
    );
  else if (!group)
    issues.push(
      observationIssue(
        mapKafkaConsumerGroupFailure(new Error("Consumer group not found"), groupId),
        "group-members",
      ),
    );
  if (committed.status === "rejected")
    issues.push(
      observationIssue(mapKafkaConsumerGroupFailure(committed.reason, groupId), "group-offsets"),
    );
  const offsets =
    (committed.status === "fulfilled" ? committed.value : [])
      .find((g) => g.groupId === groupId)
      ?.topics.find((t) => t.name === topic)?.partitions ?? [];
  if (group && group.members.size > 1000)
    issues.push({
      measurement: "group-members",
      code: "OBSERVATION_INCOMPLETE",
      summary:
        "Visible group member count is capped at 1,000; selected-topic offset coverage is unaffected.",
      recovery:
        "Inspect group membership using an administrative tool for the complete member population.",
      retryable: false,
    });
  return {
    id: groupId,
    state: group ? consumerGroupState(group.state) : null,
    members: group ? Math.min(group.members.size, 1000) : null,
    issues,
    offsets: partitions.map((partition) => {
      const offset = offsets.find((p) => p.partitionIndex === partition)?.committedOffset;
      return {
        partition,
        committedOffset: offset === undefined || offset < 0n ? null : offset.toString(),
      };
    }),
  };
}
