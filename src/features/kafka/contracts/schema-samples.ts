import type { KafkaCompleteRecord } from "./record-bytes";
import { parseKafkaOriginalRecord, kafkaOriginalRecordByteLength } from "./record-bytes";
import { parseSchemaInspectionInput, type SchemaInspectionInput } from "./schema-inspection";
import {
  parseKafkaWriteInput,
  parseKafkaWriteOutcome,
  type KafkaWriteOutcome,
} from "./reviewed-writes";
import { HostContractValidationError } from "./validation-error";
import {
  exactKeys,
  record,
  boundedText,
  nonNegativeInteger,
  positiveBoundedInteger,
  text,
} from "./validation-primitives";

export const SCHEMA_SAMPLE_LIMITS = {
  count: 50,
  recordBytes: 16_384,
  batchBytes: 524_288,
  ratePerSecond: 10,
  depth: 8,
  fields: 128,
  durationMs: 60_000,
} as const;
export interface SchemaSampleInput extends SchemaInspectionInput {
  readonly seed: number;
  readonly count: number;
  readonly messageType: string;
}
export interface SchemaSamples {
  readonly schema: SchemaInspectionInput;
  readonly schemaId: number;
  readonly encoding: string;
  readonly seed: number;
  readonly samples: readonly { readonly json: string; readonly record: KafkaCompleteRecord }[];
}
export interface RecordBatchInput {
  readonly topic: string;
  readonly partition: number;
  readonly ratePerSecond: number;
  readonly records: readonly KafkaCompleteRecord[];
}
export interface RecordBatchReview {
  readonly planId: string;
  readonly connectionName: string;
  readonly expiresAt: string;
  readonly input: RecordBatchInput;
}
export interface RecordBatchOutcome {
  readonly total: number;
  readonly unsent: number;
  readonly outcomes: readonly KafkaWriteOutcome[];
  readonly stopReason:
    "complete" | "cancelled" | "connection-changed" | "write-failed" | "deadline";
}
function boundedRecords(values: unknown, path: string): KafkaCompleteRecord[] {
  if (!Array.isArray(values) || values.length < 1 || values.length > SCHEMA_SAMPLE_LIMITS.count)
    throw new HostContractValidationError(path, "requires one to 50 records");
  let bytes = 0;
  return values.map((value: unknown) => {
    const parsed = parseKafkaOriginalRecord(value, path);
    if (
      parsed.state !== "complete" ||
      kafkaOriginalRecordByteLength(parsed) > SCHEMA_SAMPLE_LIMITS.recordBytes
    )
      throw new HostContractValidationError(path, "requires complete records, at most 16 KiB each");
    bytes += kafkaOriginalRecordByteLength(parsed);
    if (bytes > SCHEMA_SAMPLE_LIMITS.batchBytes)
      throw new HostContractValidationError(path, "batch exceeds 512 KiB");
    return parsed;
  });
}
export function parseSchemaSampleInput(value: unknown): SchemaSampleInput {
  const input = record(value, "samples");
  exactKeys(input, ["subject", "version", "seed", "count", "messageType"], "samples");
  const seed = nonNegativeInteger(input.seed, "samples.seed");
  if (seed > 0xffffffff)
    throw new HostContractValidationError("samples.seed", "requires a 32-bit unsigned integer");
  return {
    ...parseSchemaInspectionInput({ subject: input.subject, version: input.version }),
    seed,
    count: positiveBoundedInteger(input.count, "samples.count", SCHEMA_SAMPLE_LIMITS.count),
    messageType: boundedText(input.messageType, "samples.messageType", 512),
  };
}
export function parseSchemaSamples(value: unknown): SchemaSamples {
  const input = record(value, "samples");
  exactKeys(input, ["schema", "schemaId", "encoding", "seed", "samples"], "samples");
  if (
    !Array.isArray(input.samples) ||
    input.samples.length > SCHEMA_SAMPLE_LIMITS.count ||
    input.samples.length === 0
  )
    throw new HostContractValidationError("samples", "invalid sample count");
  const entries = input.samples.map((value: unknown) => {
    const entry = record(value, "sample");
    exactKeys(entry, ["json", "record"], "sample");
    const json = text(entry.json, "sample.json", 65_536);
    JSON.parse(json);
    return { json, record: entry.record };
  });
  const records = boundedRecords(
    entries.map((entry) => entry.record),
    "samples.records",
  );
  return {
    schema: parseSchemaInspectionInput(input.schema),
    schemaId: nonNegativeInteger(input.schemaId, "samples.schemaId"),
    encoding: text(input.encoding, "samples.encoding", 512),
    seed: nonNegativeInteger(input.seed, "samples.seed"),
    samples: entries.map((entry, index) => ({ json: entry.json, record: records[index]! })),
  };
}
export function parseRecordBatchInput(value: unknown): RecordBatchInput {
  const input = record(value, "batch");
  exactKeys(input, ["topic", "partition", "ratePerSecond", "records"], "batch");
  const records = boundedRecords(input.records, "batch.records");
  const validated = parseKafkaWriteInput({
    kind: "record",
    topic: input.topic,
    partition: input.partition,
    record: records[0],
  });
  if (validated.kind !== "record") throw new Error("Invalid batch destination");
  return {
    topic: validated.topic,
    partition: validated.partition,
    records,
    ratePerSecond: positiveBoundedInteger(
      input.ratePerSecond,
      "batch.ratePerSecond",
      SCHEMA_SAMPLE_LIMITS.ratePerSecond,
    ),
  };
}
export function parseRecordBatchReview(value: unknown): RecordBatchReview {
  const input = record(value, "review");
  exactKeys(input, ["planId", "connectionName", "expiresAt", "input"], "review");
  return {
    planId: text(input.planId, "review.planId", 128),
    connectionName: text(input.connectionName, "review.connectionName", 256),
    expiresAt: text(input.expiresAt, "review.expiresAt", 64),
    input: parseRecordBatchInput(input.input),
  };
}
export function parseRecordBatchOutcome(value: unknown): RecordBatchOutcome {
  const input = record(value, "outcome");
  exactKeys(input, ["total", "unsent", "outcomes", "stopReason"], "outcome");
  const total = positiveBoundedInteger(input.total, "outcome.total", SCHEMA_SAMPLE_LIMITS.count);
  const unsent = nonNegativeInteger(input.unsent, "outcome.unsent");
  if (!Array.isArray(input.outcomes) || input.outcomes.length + unsent !== total)
    throw new HostContractValidationError("outcome", "must account for every record");
  const reason = input.stopReason;
  if (
    reason !== "complete" &&
    reason !== "cancelled" &&
    reason !== "connection-changed" &&
    reason !== "write-failed" &&
    reason !== "deadline"
  )
    throw new HostContractValidationError("outcome.stopReason", "invalid stop reason");
  const outcomes = input.outcomes.map(parseKafkaWriteOutcome);
  if (
    reason === "complete" &&
    (unsent !== 0 || outcomes.some((entry) => entry.state !== "acknowledged"))
  )
    throw new HostContractValidationError(
      "outcome",
      "complete requires every record to be acknowledged",
    );
  return {
    total,
    unsent,
    outcomes,
    stopReason: reason,
  };
}
