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
  parseStructuredReplayTransform,
  parseReplayRecordEncoding,
  validateReplayEncoding,
  type StructuredReplayTransform,
  type ReplayRecordEncoding,
} from "./structured-replay";
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
  readonly structured?: StructuredReplayTransform;
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
  readonly encoding?: readonly ReplayRecordEncoding[];
}
export interface RecordReplayOutcome extends RecordBatchOutcome {
  readonly cleanup: "complete" | "unavailable";
  readonly jobId?: string;
  readonly journal?: "confirmed" | "unavailable";
  readonly durability?: "durable" | "session" | "unavailable";
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
  exactKeys(v, ["key", "removeHeaders", "appendHeaders", "valueText", "structured"], "transform");
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
  const structured =
    v.structured === undefined ? undefined : parseStructuredReplayTransform(v.structured);
  if ((structured?.key && key !== null) || (structured?.value && valueText !== null))
    throw new HostContractValidationError(
      "transform",
      "structured and byte/text replacement cannot edit the same field",
    );
  return {
    key,
    valueText,
    removeHeaders: v.removeHeaders.map((k: unknown) => parseRecordBase64(k, "removeHeaders", 512)),
    appendHeaders: envelope.headers,
    ...(structured === undefined ? {} : { structured }),
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
  if (transform.structured)
    throw new Error("Structured transformation requires the host authoring path.");
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
  if (input.transform.structured)
    throw new Error("Structured replay requires host encoding and destination writer review.");
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
    [
      "planId",
      "sourceName",
      "targetName",
      "expiresAt",
      "input",
      "batch",
      "destination",
      "encoding",
    ],
    "replayReview",
  );
  const d = record(v.destination, "destination");
  exactKeys(d, ["clusterId", "topicId", "partitions"], "destination");
  const input = parseRecordReplayInput(v.input);
  let encoding: readonly ReplayRecordEncoding[] | undefined;
  if (v.encoding !== undefined) {
    if (!Array.isArray(v.encoding) || v.encoding.length !== input.records.length)
      throw new Error("Writer evidence must match every reviewed record.");
    encoding = v.encoding.map(parseReplayRecordEncoding);
  }
  if ((input.transform.structured !== undefined) !== (encoding !== undefined))
    throw new Error("Structured reviews require explicit writer evidence.");
  const batch = parseRecordBatchInput(v.batch);
  if (input.transform.structured && encoding) {
    if (
      batch.records.length !== input.records.length ||
      batch.topic !== input.topic ||
      batch.partition !== input.partition ||
      batch.ratePerSecond !== input.ratePerSecond ||
      JSON.stringify(batch.timestamps) !== JSON.stringify(input.records.map((r) => r.timestampMs))
    )
      throw new Error("Structured batch differs from its reviewed input.");
    for (const [index, source] of input.records.entries()) {
      validateReplayEncoding(
        source.original,
        batch.records[index]!,
        input.transform.structured,
        encoding[index]!,
      );
      const ordinary = transformReplayRecord(source.original, {
        key: input.transform.key,
        valueText: input.transform.valueText,
        removeHeaders: input.transform.removeHeaders,
        appendHeaders: input.transform.appendHeaders,
      });
      const output = batch.records[index]!;
      if (
        (!input.transform.structured.key && ordinary.key !== output.key) ||
        (!input.transform.structured.value && ordinary.value !== output.value) ||
        JSON.stringify(ordinary.headers) !== JSON.stringify(output.headers)
      )
        throw new Error("Unmapped bytes differ from reviewed transformations.");
    }
  }
  return {
    planId: text(v.planId, "planId", 128),
    sourceName: text(v.sourceName, "sourceName", 256),
    targetName: text(v.targetName, "targetName", 256),
    expiresAt: text(v.expiresAt, "expiresAt", 64),
    input,
    batch,
    destination: {
      clusterId: text(d.clusterId, "clusterId", 256),
      topicId: text(d.topicId, "topicId", 256),
      partitions: positiveBoundedInteger(d.partitions, "partitions", 100_000),
    },
    ...(encoding === undefined ? {} : { encoding }),
  };
}
export function parseRecordReplayOutcome(value: unknown): RecordReplayOutcome {
  const v = record(value, "replayOutcome");
  exactKeys(
    v,
    ["total", "unsent", "outcomes", "stopReason", "cleanup", "jobId", "journal", "durability"],
    "replayOutcome",
  );
  if (
    (v.jobId === undefined) !== (v.journal === undefined) ||
    (v.jobId === undefined) !== (v.durability === undefined)
  )
    throw new Error("Repair journal metadata must be complete.");
  return {
    ...parseRecordBatchOutcome({
      total: v.total,
      unsent: v.unsent,
      outcomes: v.outcomes,
      stopReason: v.stopReason,
    }),
    cleanup: declaredValue(v.cleanup, ["complete", "unavailable"] as const, "cleanup"),
    ...(v.jobId === undefined
      ? {}
      : {
          jobId: text(v.jobId, "jobId", 128),
          journal: declaredValue(v.journal, ["confirmed", "unavailable"] as const, "journal"),
          durability: declaredValue(
            v.durability,
            ["durable", "session", "unavailable"] as const,
            "durability",
          ),
        }),
  };
}
