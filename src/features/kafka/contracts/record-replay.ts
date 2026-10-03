import {
  parseRecordBase64,
  parseKafkaOriginalRecord,
  type KafkaCompleteRecord,
} from "./record-bytes";
import {
  parseRecordBatchInput,
  parseRecordBatchOutcome,
  type RecordBatchInput,
  type RecordBatchOutcome,
} from "./schema-samples";
import type { KafkaWriteDestination } from "./reviewed-writes";
import { offsetPosition } from "./offset-reset";
import { HostContractValidationError } from "./validation-error";
import {
  record,
  exactKeys,
  text,
  boundedText,
  nonNegativeInteger,
  positiveBoundedInteger,
  declaredValue,
} from "./validation-primitives";

export interface ReplayRecord {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  readonly timestampMs: string | null;
  readonly original: KafkaCompleteRecord;
}
export interface ReplayTransform {
  /** null leaves the key untouched; {value:null} explicitly replaces it with null. */
  readonly key: { readonly value: string | null } | null;
  readonly removeHeaders: readonly string[];
  readonly appendHeaders: KafkaCompleteRecord["headers"];
  readonly valueText: { readonly search: string; readonly replacement: string } | null;
}
export interface RecordReplayInput {
  readonly targetProfile: { readonly id: string; readonly revision: number } | null;
  readonly topic: string;
  readonly partition: number;
  readonly ratePerSecond: number;
  readonly records: readonly ReplayRecord[];
  readonly transform: ReplayTransform;
}
export interface RecordReplayReview {
  readonly planId: string;
  readonly sourceName: string;
  readonly targetName: string;
  readonly expiresAt: string;
  readonly input: RecordReplayInput;
  readonly batch: RecordBatchInput;
  readonly destination: KafkaWriteDestination;
}
export interface RecordReplayOutcome extends RecordBatchOutcome {
  readonly cleanup: "complete" | "unavailable";
}
export function replayConfirmation(review: RecordReplayReview): string {
  return `${review.targetName} / ${review.input.topic} / ${review.input.partition}`;
}
export const UNCHANGED_REPLAY_TRANSFORM: ReplayTransform = {
  key: null,
  removeHeaders: [],
  appendHeaders: [],
  valueText: null,
};

export function parseReplayTransform(value: unknown): ReplayTransform {
  const v = record(value, "transform");
  exactKeys(v, ["key", "removeHeaders", "appendHeaders", "valueText"], "transform");
  let key: ReplayTransform["key"] = null;
  if (v.key !== null) {
    const k = record(v.key, "transform.key");
    exactKeys(k, ["value"], "transform.key");
    key = {
      value: k.value === null ? null : parseRecordBase64(k.value, "transform.key.value", 16_384),
    };
  }
  if (
    !Array.isArray(v.removeHeaders) ||
    v.removeHeaders.length > 16 ||
    !Array.isArray(v.appendHeaders) ||
    v.appendHeaders.length > 16
  )
    throw new HostContractValidationError("transform.headers", "at most 16 removals and additions");
  const envelope = parseKafkaOriginalRecord({
    state: "complete",
    encoding: "base64",
    key: null,
    value: null,
    headers: v.appendHeaders,
  });
  if (envelope.state !== "complete") throw new Error("Invalid headers");
  let valueText: ReplayTransform["valueText"] = null;
  if (v.valueText !== null) {
    const t = record(v.valueText, "transform.valueText");
    exactKeys(t, ["search", "replacement"], "transform.valueText");
    valueText = {
      search: text(t.search, "search", 256),
      replacement: boundedText(t.replacement, "replacement", 1024),
    };
  }
  return {
    key,
    valueText,
    removeHeaders: v.removeHeaders.map((k: unknown) => parseRecordBase64(k, "removeHeaders", 512)),
    appendHeaders: envelope.headers,
  };
}
function encode(text: string): string {
  return btoa(Array.from(new TextEncoder().encode(text), (b) => String.fromCharCode(b)).join(""));
}
/** Declarative byte-preserving transforms; never evaluates supplied code. */
export function transformReplayRecord(
  original: KafkaCompleteRecord,
  transform: ReplayTransform,
): KafkaCompleteRecord {
  let value = original.value;
  if (transform.valueText && value !== null) {
    const bytes = Uint8Array.from(atob(value), (c) => c.charCodeAt(0));
    const decoded = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);
    const parts = decoded.split(transform.valueText.search);
    const outputLength =
      decoded.length +
      (parts.length - 1) *
        (transform.valueText.replacement.length - transform.valueText.search.length);
    if (outputLength > 16_384) throw new Error("Transformed value exceeds the record bound.");
    value = encode(parts.join(transform.valueText.replacement));
  }
  return {
    state: "complete",
    encoding: "base64",
    key: transform.key === null ? original.key : transform.key.value,
    value,
    headers: [
      ...original.headers.filter((h) => !transform.removeHeaders.includes(h.key)),
      ...transform.appendHeaders,
    ],
  };
}
export function parseRecordReplayInput(value: unknown): RecordReplayInput {
  const v = record(value, "replay");
  exactKeys(
    v,
    ["targetProfile", "topic", "partition", "ratePerSecond", "records", "transform"],
    "replay",
  );
  if (!Array.isArray(v.records) || v.records.length === 0 || v.records.length > 50)
    throw new HostContractValidationError("replay.records", "select one to 50 complete records");
  const records = v.records.map((value: unknown): ReplayRecord => {
    const r = record(value, "replay.record");
    exactKeys(r, ["topic", "partition", "offset", "timestampMs", "original"], "replay.record");
    const original = parseKafkaOriginalRecord(r.original);
    if (original.state !== "complete")
      throw new HostContractValidationError(
        "replay.record",
        "complete original bytes are required",
      );
    return {
      topic: text(r.topic, "topic", 249),
      partition: nonNegativeInteger(r.partition, "partition"),
      offset: offsetPosition(r.offset, "offset"),
      timestampMs: r.timestampMs === null ? null : offsetPosition(r.timestampMs, "timestampMs"),
      original,
    };
  });
  if (new Set(records.map((r) => `${r.topic}:${r.partition}:${r.offset}`)).size !== records.length)
    throw new HostContractValidationError("replay.records", "source identities must be distinct");
  const batch = parseRecordBatchInput({
    topic: v.topic,
    partition: v.partition,
    ratePerSecond: v.ratePerSecond,
    records: records.map((r) => r.original),
    timestamps: records.map((r) => r.timestampMs),
  });
  let targetProfile: RecordReplayInput["targetProfile"] = null;
  if (v.targetProfile !== null) {
    const p = record(v.targetProfile, "targetProfile");
    exactKeys(p, ["id", "revision"], "targetProfile");
    targetProfile = {
      id: text(p.id, "profileId", 128),
      revision: positiveBoundedInteger(p.revision, "revision", Number.MAX_SAFE_INTEGER),
    };
  }
  return {
    targetProfile,
    topic: batch.topic,
    partition: batch.partition,
    ratePerSecond: batch.ratePerSecond,
    records,
    transform: parseReplayTransform(v.transform),
  };
}
export function replayBatch(input: RecordReplayInput): RecordBatchInput {
  return parseRecordBatchInput({
    topic: input.topic,
    partition: input.partition,
    ratePerSecond: input.ratePerSecond,
    records: input.records.map((r) => transformReplayRecord(r.original, input.transform)),
    timestamps: input.records.map((r) => r.timestampMs),
  });
}
export function parseRecordReplayReview(value: unknown): RecordReplayReview {
  const v = record(value, "replayReview");
  exactKeys(
    v,
    ["planId", "sourceName", "targetName", "expiresAt", "input", "batch", "destination"],
    "replayReview",
  );
  const d = record(v.destination, "destination");
  exactKeys(d, ["clusterId", "topicId", "partitions"], "destination");
  return {
    planId: text(v.planId, "planId", 128),
    sourceName: text(v.sourceName, "sourceName", 256),
    targetName: text(v.targetName, "targetName", 256),
    expiresAt: text(v.expiresAt, "expiresAt", 64),
    input: parseRecordReplayInput(v.input),
    batch: parseRecordBatchInput(v.batch),
    destination: {
      clusterId: text(d.clusterId, "clusterId", 256),
      topicId: text(d.topicId, "topicId", 256),
      partitions: positiveBoundedInteger(d.partitions, "partitions", 100_000),
    },
  };
}
export function parseRecordReplayOutcome(value: unknown): RecordReplayOutcome {
  const v = record(value, "replayOutcome");
  exactKeys(v, ["total", "unsent", "outcomes", "stopReason", "cleanup"], "replayOutcome");
  return {
    ...parseRecordBatchOutcome({
      total: v.total,
      unsent: v.unsent,
      outcomes: v.outcomes,
      stopReason: v.stopReason,
    }),
    cleanup: declaredValue(v.cleanup, ["complete", "unavailable"] as const, "cleanup"),
  };
}
