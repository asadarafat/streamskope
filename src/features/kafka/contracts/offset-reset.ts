import { HostContractValidationError } from "./validation-error";
import {
  record,
  exactKeys,
  text,
  boundedText,
  declaredValue,
  nonNegativeInteger,
  truth,
} from "./validation-primitives";

export const OFFSET_RESET_LIMITS = { partitions: 32, examples: 3, lifetimeMs: 120_000 } as const;
export interface OffsetResetTarget {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
}
export interface OffsetResetInput {
  readonly groupId: string;
  readonly targets: readonly OffsetResetTarget[];
}
export interface OffsetResetPartition extends OffsetResetTarget {
  readonly before: string | null;
  readonly low: string;
  readonly high: string;
  /** Offset distance is an upper bound, not a message count (compaction/transactions/gaps). */
  readonly replayUpperBound: string;
}
export interface OffsetResetSnapshot {
  readonly inactive: boolean;
  readonly state: string;
  readonly groupRead: "allowed" | "denied" | "unknown";
  readonly partitions: readonly OffsetResetPartition[];
}
export interface OffsetResetExample {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  readonly key: string | null;
  readonly value: string | null;
}
export interface OffsetResetReview {
  readonly planId: string;
  readonly connectionName: string;
  readonly expiresAt: string;
  readonly input: OffsetResetInput;
  readonly baseline: OffsetResetSnapshot;
  readonly examples: readonly OffsetResetExample[];
  readonly exampleStatus: "sampled" | "empty" | "unavailable";
}
export interface OffsetResetResult extends OffsetResetTarget {
  readonly state: "acknowledged" | "rejected" | "unknown" | "unsent";
  readonly observed: string | null;
  readonly verified: boolean;
}
export interface OffsetResetOutcome {
  readonly groupId: string;
  readonly partitions: readonly OffsetResetResult[];
  readonly detail: string;
}
export function offsetPosition(value: unknown, path: string): string {
  const v = text(value, path, 19);
  if (!/^(0|[1-9][0-9]*)$/u.test(v) || BigInt(v) > 9_223_372_036_854_775_807n)
    throw new HostContractValidationError(path, "must be a non-negative Kafka offset");
  return v;
}
function target(value: unknown, path: string): OffsetResetTarget {
  const v = record(value, path);
  const topic = text(v.topic, `${path}.topic`, 249);
  if (!/^[a-zA-Z0-9._-]+$/u.test(topic) || topic === "." || topic === "..")
    throw new HostContractValidationError(path, "requires a valid topic");
  return {
    topic,
    partition: nonNegativeInteger(v.partition, `${path}.partition`),
    offset: offsetPosition(v.offset, `${path}.offset`),
  };
}
function list(
  value: unknown,
  path: string,
  maximum: number = OFFSET_RESET_LIMITS.partitions,
): readonly unknown[] {
  if (!Array.isArray(value) || value.length > maximum)
    throw new HostContractValidationError(path, `must contain at most ${maximum} items`);
  return value;
}
export function parseOffsetResetInput(value: unknown): OffsetResetInput {
  const v = record(value, "reset");
  exactKeys(v, ["groupId", "targets"], "reset");
  const targets = list(v.targets, "reset.targets").map((item, index) => {
    exactKeys(record(item, "target"), ["topic", "partition", "offset"], "target");
    return target(item, `reset.targets[${index}]`);
  });
  if (
    !targets.length ||
    new Set(targets.map((t) => `${t.topic}:${t.partition}`)).size !== targets.length
  )
    throw new HostContractValidationError(
      "reset.targets",
      "requires distinct nonempty partition targets",
    );
  return { groupId: text(v.groupId, "reset.groupId", 512), targets };
}
export function parseOffsetResetReview(value: unknown): OffsetResetReview {
  const v = record(value, "resetReview");
  exactKeys(
    v,
    ["planId", "connectionName", "expiresAt", "input", "baseline", "examples", "exampleStatus"],
    "resetReview",
  );
  const b = record(v.baseline, "baseline");
  exactKeys(b, ["inactive", "state", "groupRead", "partitions"], "baseline");
  return {
    planId: text(v.planId, "planId", 128),
    connectionName: text(v.connectionName, "connectionName", 256),
    expiresAt: text(v.expiresAt, "expiresAt", 64),
    input: parseOffsetResetInput(v.input),
    baseline: {
      inactive: truth(b.inactive, "inactive"),
      state: text(b.state, "state", 64),
      groupRead: declaredValue(b.groupRead, ["allowed", "denied", "unknown"] as const, "groupRead"),
      partitions: list(b.partitions, "partitions").map((item) => {
        const p = record(item, "partition");
        exactKeys(
          p,
          ["topic", "partition", "offset", "before", "low", "high", "replayUpperBound"],
          "partition",
        );
        return {
          ...target(p, "partition"),
          before: p.before === null ? null : offsetPosition(p.before, "before"),
          low: offsetPosition(p.low, "low"),
          high: offsetPosition(p.high, "high"),
          replayUpperBound: offsetPosition(p.replayUpperBound, "replayUpperBound"),
        };
      }),
    },
    examples: list(v.examples, "examples", 3).map((item) => {
      const p = record(item, "example");
      exactKeys(p, ["topic", "partition", "offset", "key", "value"], "example");
      return {
        ...target(p, "example"),
        key: p.key === null ? null : boundedText(p.key, "key", 512),
        value: p.value === null ? null : boundedText(p.value, "value", 512),
      };
    }),
    exampleStatus: declaredValue(
      v.exampleStatus,
      ["sampled", "empty", "unavailable"] as const,
      "exampleStatus",
    ),
  };
}
export function parseOffsetResetOutcome(value: unknown): OffsetResetOutcome {
  const v = record(value, "resetOutcome");
  exactKeys(v, ["groupId", "partitions", "detail"], "resetOutcome");
  return {
    groupId: text(v.groupId, "groupId", 512),
    detail: text(v.detail, "detail", 2048),
    partitions: list(v.partitions, "partitions").map((item) => {
      const p = record(item, "partition");
      exactKeys(p, ["topic", "partition", "offset", "state", "observed", "verified"], "partition");
      return {
        ...target(p, "partition"),
        state: declaredValue(
          p.state,
          ["acknowledged", "rejected", "unknown", "unsent"] as const,
          "state",
        ),
        observed: p.observed === null ? null : offsetPosition(p.observed, "observed"),
        verified: truth(p.verified, "verified"),
      };
    }),
  };
}
