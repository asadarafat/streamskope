import { setTimeout as delay } from "node:timers/promises";

import type { Consumer } from "@platformatic/kafka";

const pendingMetadata = [
  "UNKNOWN_TOPIC_OR_PARTITION",
  "LEADER_NOT_AVAILABLE",
  "NOT_LEADER_OR_FOLLOWER",
  "FENCED_LEADER_EPOCH",
];

function protocolCodes(error: unknown): readonly string[] {
  const codes = new Set<string>();
  const queue: unknown[] = [error];
  for (let index = 0; index < queue.length && index < 32; index += 1) {
    const item = queue[index];
    if (typeof item !== "object" || item === null) continue;
    const detail = item as { apiId?: unknown; errors?: unknown; cause?: unknown };
    if (typeof detail.apiId === "string" && /^[A-Z_]{1,80}$/.test(detail.apiId))
      codes.add(detail.apiId);
    if (Array.isArray(detail.errors)) queue.push(...(detail.errors as unknown[]).slice(0, 16));
    if (detail.cause !== undefined) queue.push(detail.cause);
  }
  return [...codes];
}

/** Codes only: fixture failures must not dump SDK requests or credentials. */
export function kafkaFixtureFailure(phase: string, error: unknown): Error {
  return new Error(
    `${phase}: ${protocolCodes(error).join(", ") || "no Kafka protocol code reported"}.`,
  );
}

/** CreateTopics metadata is not proof that the data listener can serve every partition. */
export async function waitForKafkaTopicOffsets(
  consumer: Consumer,
  topic: string,
  topicId: string,
  partitions: number,
): Promise<readonly bigint[]> {
  const readyBy = Date.now() + 15_000;
  for (;;) {
    try {
      const metadata = await consumer.metadata({
        topics: [topic],
        forceUpdate: true,
        autocreateTopics: false,
      });
      const current = metadata.topics.get(topic);
      if (
        current?.id === topicId &&
        current.partitions.length === partitions &&
        current.partitions.every(
          (partition) => partition.leader >= 0 && partition.isr.includes(partition.leader),
        )
      ) {
        const offsets = (await consumer.listOffsets({ topics: [topic], timestamp: -1n })).get(
          topic,
        );
        if (
          offsets !== undefined &&
          Object.keys(offsets).length === partitions &&
          Array.from({ length: partitions }, (_, partition) => offsets[partition]).every(
            (offset) => typeof offset === "bigint" && offset >= 0n,
          )
        )
          return Array.from({ length: partitions }, (_, partition) => offsets[partition]!);
      }
    } catch (error) {
      const codes = protocolCodes(error);
      // The SDK converts Metadata UNKNOWN_TOPIC into this exact user error and drops
      // its protocol cause. Only the newly created, owned topic may be pending here.
      const unknownOwnedTopic =
        codes.length === 0 &&
        error instanceof Error &&
        "code" in error &&
        error.code === "PLT_KFK_USER" &&
        error.message === `Unknown topic ${topic}.`;
      if (
        !unknownOwnedTopic &&
        (codes.length === 0 || !codes.every((code) => pendingMetadata.includes(code)))
      )
        throw kafkaFixtureFailure("Fixture topic readiness failed", error);
    }
    if (Date.now() >= readyBy)
      throw new Error("The owned Kafka topic did not expose its expected identity and offsets.");
    consumer.clearMetadata();
    await delay(100);
  }
}
