import {
  kafkaOriginalRecordByteLength,
  parseKafkaOriginalRecord,
  type KafkaCompleteRecord,
} from "./record-bytes";
import { HostContractValidationError } from "./validation-error";
import {
  boundedText,
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  positiveBoundedInteger,
  record,
  text,
} from "./validation-primitives";

export type KafkaWriteInput =
  | {
      readonly kind: "record";
      readonly topic: string;
      readonly partition: number;
      readonly record: KafkaCompleteRecord;
    }
  | {
      readonly kind: "topic";
      readonly topic: string;
      readonly partitions: number;
      readonly replicationFactor: number;
      readonly configs: readonly { readonly name: string; readonly value: string }[];
    };

export interface KafkaWriteReview {
  readonly planId: string;
  readonly connectionName: string;
  readonly expiresAt: string;
  readonly input: KafkaWriteInput;
}

export interface KafkaWriteOutcome {
  readonly state: "acknowledged" | "rejected" | "unknown";
  readonly detail: string;
  readonly receipt: {
    readonly topic: string;
    readonly partition: number;
    readonly offset: string;
  } | null;
  readonly verification: "verified" | "unavailable" | "not-applicable";
}

export function parseKafkaWriteInput(value: unknown, path = "write"): KafkaWriteInput {
  const input = record(value, path);
  const topic = text(input.topic, `${path}.topic`, 249);
  if (!/^[a-zA-Z0-9._-]+$/u.test(topic) || topic === "." || topic === "..")
    throw new HostContractValidationError(`${path}.topic`, "must be a valid Kafka topic name");
  if (input.kind === "record") {
    exactKeys(input, ["kind", "topic", "partition", "record"], path);
    const bytes = parseKafkaOriginalRecord(input.record, `${path}.record`);
    if (bytes.state !== "complete" || kafkaOriginalRecordByteLength(bytes) > 65_536)
      throw new HostContractValidationError(
        `${path}.record`,
        "requires complete bytes, at most 64 KiB including headers",
      );
    return {
      kind: "record",
      topic,
      partition: nonNegativeInteger(input.partition, `${path}.partition`),
      record: bytes,
    };
  }
  if (input.kind !== "topic")
    throw new HostContractValidationError(`${path}.kind`, "must be record or topic");
  exactKeys(input, ["kind", "topic", "partitions", "replicationFactor", "configs"], path);
  if (!Array.isArray(input.configs) || input.configs.length > 32)
    throw new HostContractValidationError(`${path}.configs`, "must contain at most 32 settings");
  const configs = input.configs.map((item: unknown, index) => {
    const entry = record(item, `${path}.configs[${index}]`);
    exactKeys(entry, ["name", "value"], `${path}.configs[${index}]`);
    return {
      name: text(entry.name, `${path}.configs[${index}].name`, 128),
      value: boundedText(entry.value, `${path}.configs[${index}].value`, 1_024),
    };
  });
  if (new Set(configs.map(({ name }) => name)).size !== configs.length)
    throw new HostContractValidationError(
      `${path}.configs`,
      "must not repeat a configuration name",
    );
  return {
    kind: "topic",
    topic,
    partitions: positiveBoundedInteger(input.partitions, `${path}.partitions`, 1_000),
    replicationFactor: positiveBoundedInteger(
      input.replicationFactor,
      `${path}.replicationFactor`,
      32,
    ),
    configs,
  };
}

export function parseKafkaWriteReview(value: unknown): KafkaWriteReview {
  const input = record(value, "review");
  exactKeys(input, ["planId", "connectionName", "expiresAt", "input"], "review");
  return {
    planId: text(input.planId, "review.planId", 128),
    connectionName: text(input.connectionName, "review.connectionName", 256),
    expiresAt: text(input.expiresAt, "review.expiresAt", 64),
    input: parseKafkaWriteInput(input.input),
  };
}

export function parseKafkaWriteOutcome(value: unknown): KafkaWriteOutcome {
  const input = record(value, "outcome");
  exactKeys(input, ["state", "detail", "receipt", "verification"], "outcome");
  const receipt = input.receipt === null ? null : record(input.receipt, "outcome.receipt");
  if (receipt !== null) exactKeys(receipt, ["topic", "partition", "offset"], "outcome.receipt");
  return {
    state: declaredValue(
      input.state,
      ["acknowledged", "rejected", "unknown"] as const,
      "outcome.state",
    ),
    detail: text(input.detail, "outcome.detail", 2_048),
    verification: declaredValue(
      input.verification,
      ["verified", "unavailable", "not-applicable"] as const,
      "outcome.verification",
    ),
    receipt:
      receipt === null
        ? null
        : {
            topic: text(receipt.topic, "outcome.receipt.topic", 249),
            partition: nonNegativeInteger(receipt.partition, "outcome.receipt.partition"),
            offset: text(receipt.offset, "outcome.receipt.offset", 32),
          },
  };
}
