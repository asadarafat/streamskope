import type { KafkaReadCoverage } from "../contracts/query-search";

/** Host-owned read state. Renderers receive an opaque continuation identifier instead. */
export interface KafkaReadCheckpoint {
  readonly clusterId: string;
  readonly topicId: string;
  readonly partitionCount: number;
  readonly coverage: KafkaReadCoverage;
}

export class KafkaReadCheckpointError extends Error {
  constructor(
    readonly reason: "identity-changed" | "partitions-changed" | "retention-changed" | "invalid",
  ) {
    super(
      reason === "identity-changed"
        ? "The Kafka cluster or topic identity changed since this read began."
        : reason === "partitions-changed"
          ? "The topic partition inventory changed since this read began."
          : reason === "retention-changed"
            ? "The remaining captured offsets are no longer retained by Kafka."
            : "The saved read checkpoint is invalid for this request.",
    );
    this.name = "KafkaReadCheckpointError";
  }
}
