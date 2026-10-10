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
export type OffsetResetPosition =
  | { readonly kind: "earliest" | "latest" }
  | { readonly kind: "timestamp"; readonly timestampMs: string };
export interface OffsetResetSelectionInput {
  readonly groupId: string;
  readonly partitions: readonly { readonly topic: string; readonly partition: number }[];
  readonly position: OffsetResetPosition;
}
export type OffsetResetRequest = OffsetResetInput | OffsetResetSelectionInput;
export interface OffsetResetPartition extends OffsetResetTarget {
  readonly before: string | null;
  readonly low: string;
  readonly high: string;
  /** Offset distance is an upper bound, not a message count (compaction/transactions/gaps). */
  readonly replayUpperBound: string;
}
export interface OffsetResetSnapshot {
  readonly clusterId: string;
  readonly topics: readonly { readonly topic: string; readonly topicId: string }[];
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
  readonly selection?: OffsetResetSelectionInput;
}
export interface OffsetResetResult extends OffsetResetTarget {
  readonly state: "acknowledged" | "rejected" | "unknown" | "unsent";
  readonly observed: string | null;
  readonly verified: boolean;
  readonly cleanup: "confirmed" | "unresolved";
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
  const partition = nonNegativeInteger(v.partition, `${path}.partition`);
  if (partition > 2_147_483_647)
    throw new HostContractValidationError(path, "requires a Kafka int32 partition");
  return {
    topic,
    partition,
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
export function parseOffsetResetRequest(value: unknown): OffsetResetRequest {
  const v = record(value, "reset");
  if (!Object.hasOwn(v, "position")) return parseOffsetResetInput(value);
  exactKeys(v, ["groupId", "partitions", "position"], "reset");
  const position = record(v.position, "position");
  const kind = declaredValue(
    position.kind,
    ["earliest", "latest", "timestamp"] as const,
    "position.kind",
  );
  exactKeys(position, kind === "timestamp" ? ["kind", "timestampMs"] : ["kind"], "position");
  const partitions = list(v.partitions, "reset.partitions").map((item, index) => {
    const p = record(item, `reset.partitions[${index}]`);
    exactKeys(p, ["topic", "partition"], "partition");
    const parsed = target({ ...p, offset: "0" }, "partition");
    return { topic: parsed.topic, partition: parsed.partition };
  });
  if (
    !partitions.length ||
    new Set(partitions.map((p) => `${p.topic}:${p.partition}`)).size !== partitions.length
  )
    throw new HostContractValidationError(
      "reset.partitions",
      "requires distinct nonempty selected partitions",
    );
  return {
    groupId: text(v.groupId, "reset.groupId", 512),
    partitions,
    position:
      kind === "timestamp"
        ? { kind, timestampMs: offsetPosition(position.timestampMs, "position.timestampMs") }
        : { kind },
  };
}
export function parseOffsetResetReview(value: unknown): OffsetResetReview {
  const v = record(value, "resetReview");
  exactKeys(
    v,
    [
      "planId",
      "connectionName",
      "expiresAt",
      "input",
      "baseline",
      "examples",
      "exampleStatus",
      "selection",
    ],
    "resetReview",
  );
  const b = record(v.baseline, "baseline");
  exactKeys(b, ["clusterId", "topics", "inactive", "state", "groupRead", "partitions"], "baseline");
  const input = parseOffsetResetInput(v.input);
  const parsedSelection =
    v.selection === undefined ? undefined : parseOffsetResetRequest(v.selection);
  if (parsedSelection !== undefined && !("position" in parsedSelection))
    throw new HostContractValidationError("selection", "requires a broker position selector");
  const selection = parsedSelection;
  if (
    selection !== undefined &&
    (selection.groupId !== input.groupId ||
      selection.partitions.length !== input.targets.length ||
      selection.partitions.some(
        (p, i) =>
          p.topic !== input.targets[i]?.topic || p.partition !== input.targets[i]?.partition,
      ))
  )
    throw new HostContractValidationError(
      "selection",
      "must describe the resolved target partitions",
    );
  const review: OffsetResetReview = {
    planId: text(v.planId, "planId", 128),
    connectionName: text(v.connectionName, "connectionName", 256),
    expiresAt: text(v.expiresAt, "expiresAt", 64),
    input,
    ...(selection === undefined ? {} : { selection }),
    baseline: {
      clusterId: text(b.clusterId, "clusterId", 128),
      topics: list(b.topics, "topics").map((item) => {
        const t = record(item, "topicIdentity");
        exactKeys(t, ["topic", "topicId"], "topicIdentity");
        const topicId = text(t.topicId, "topicId", 36);
        if (
          !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/iu.test(topicId) ||
          /^0{8}(?:-0{4}){3}-0{12}$/u.test(topicId)
        )
          throw new HostContractValidationError("topicId", "requires a real topic UUID");
        return {
          topic: target({ ...t, partition: 0, offset: "0" }, "topicIdentity").topic,
          topicId,
        };
      }),
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
  if (
    review.baseline.partitions.length !== input.targets.length ||
    review.baseline.partitions.some(
      (p, i) =>
        p.topic !== input.targets[i]?.topic ||
        p.partition !== input.targets[i]?.partition ||
        p.offset !== input.targets[i]?.offset,
    ) ||
    new Set(review.baseline.topics.map((t) => t.topic)).size !== review.baseline.topics.length ||
    review.baseline.topics.length !== new Set(input.targets.map((t) => t.topic)).size ||
    input.targets.some((p) => !review.baseline.topics.some((t) => t.topic === p.topic)) ||
    review.examples.some(
      (e) =>
        !input.targets.some(
          (t) =>
            t.topic === e.topic &&
            t.partition === e.partition &&
            BigInt(e.offset) >= BigInt(t.offset),
        ),
    )
  )
    throw new HostContractValidationError(
      "resetReview",
      "baseline identities, positions and samples must match the selected partitions",
    );
  return review;
}
export function parseOffsetResetOutcome(value: unknown): OffsetResetOutcome {
  const v = record(value, "resetOutcome");
  exactKeys(v, ["groupId", "partitions", "detail"], "resetOutcome");
  return {
    groupId: text(v.groupId, "groupId", 512),
    detail: text(v.detail, "detail", 2048),
    partitions: list(v.partitions, "partitions").map((item) => {
      const p = record(item, "partition");
      exactKeys(
        p,
        ["topic", "partition", "offset", "state", "observed", "verified", "cleanup"],
        "partition",
      );
      return {
        ...target(p, "partition"),
        state: declaredValue(
          p.state,
          ["acknowledged", "rejected", "unknown", "unsent"] as const,
          "state",
        ),
        observed: p.observed === null ? null : offsetPosition(p.observed, "observed"),
        verified: truth(p.verified, "verified"),
        cleanup: declaredValue(p.cleanup, ["confirmed", "unresolved"] as const, "cleanup"),
      };
    }),
  };
}
