import { parseHostError } from "./host-error-validation";
import { parseKafkaRecordProtection } from "./operational-preference-validation";
import { parseKafkaReadCoverage, parseKafkaSearchFilter } from "./query-search";
import {
  RECORD_EXPORT_LIMITS,
  RECORD_EXPORT_REASONS,
  RECORD_EXPORT_STATES,
  type RecordExportArtifact,
  type RecordExportCounts,
  type RecordExportInput,
  type RecordExportLimits,
  type RecordExportOperation,
  type RecordExportOutput,
  type RecordExportRange,
  type RecordExportReceipt,
  type RecordExportSettings,
  type RecordExportSnapshot,
  type RecordExportSource,
} from "./record-export";
import { parseRecordCodecPreferences } from "./structured-record";
import { HostContractValidationError } from "./validation-error";
import {
  canonicalIsoTimestamp,
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  nullableText,
  positiveBoundedInteger,
  record,
  text,
  truth,
} from "./validation-primitives";

export function parseRecordExportId(value: unknown, path: string): string {
  const id = text(value, path, 36);
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(id))
    throw new HostContractValidationError(path, "must be a UUID");
  return id;
}

function parseRange(value: unknown, path: string): RecordExportRange {
  const range = record(value, path);
  const mode = declaredValue(range.mode, ["earliest", "time-window"], `${path}.mode`);
  if (mode === "earliest") {
    exactKeys(range, ["mode"], path);
    return { mode };
  }
  exactKeys(range, ["mode", "startTimeMs", "endTimeMs"], path);
  const startTimeMs = nonNegativeInteger(range.startTimeMs, `${path}.startTimeMs`);
  const endTimeMs = nonNegativeInteger(range.endTimeMs, `${path}.endTimeMs`);
  if (endTimeMs <= startTimeMs || endTimeMs > 8_640_000_000_000_000)
    throw new HostContractValidationError(path, "must have a valid end time after its start");
  return { mode, startTimeMs, endTimeMs };
}

export function parseRecordExportInput(value: unknown, path = "recordExport"): RecordExportInput {
  const input = record(value, path);
  exactKeys(input, ["requestId", "topic", "range", "search", "format", "maxRecords"], path);
  const topic = text(input.topic, `${path}.topic`, 512);
  if (topic.trim().length === 0)
    throw new HostContractValidationError(`${path}.topic`, "must not be blank");
  return {
    requestId: parseRecordExportId(input.requestId, `${path}.requestId`),
    topic,
    range: parseRange(input.range, `${path}.range`),
    search: parseKafkaSearchFilter(input.search, `${path}.search`),
    format: declaredValue(input.format, ["csv", "jsonl"], `${path}.format`),
    maxRecords: positiveBoundedInteger(
      input.maxRecords,
      `${path}.maxRecords`,
      RECORD_EXPORT_LIMITS.records,
    ),
  };
}

function parseLimits(value: unknown, path: string): RecordExportLimits {
  const limits = record(value, path);
  exactKeys(limits, Object.keys(RECORD_EXPORT_LIMITS), path);
  return Object.fromEntries(
    Object.entries(RECORD_EXPORT_LIMITS).map(([key, maximum]) => [
      key,
      positiveBoundedInteger(limits[key], `${path}.${key}`, maximum),
    ]),
  ) as unknown as RecordExportLimits;
}

function parseSettings(value: unknown, path: string): RecordExportSettings {
  const settings = record(value, path);
  exactKeys(settings, ["codecs", "protection"], path);
  return {
    codecs: parseRecordCodecPreferences(settings.codecs, `${path}.codecs`),
    protection: parseKafkaRecordProtection(settings.protection, `${path}.protection`),
  };
}

function parseSource(value: unknown, path: string): RecordExportSource {
  const source = record(value, path);
  exactKeys(source, ["connectionName", "clusterId", "topicId"], path);
  return {
    connectionName: text(source.connectionName, `${path}.connectionName`, 256),
    clusterId: nullableText(source.clusterId, `${path}.clusterId`, 256),
    topicId: nullableText(source.topicId, `${path}.topicId`, 256),
  };
}

function parseCounts(value: unknown, path: string): RecordExportCounts {
  const counts = record(value, path);
  const keys = [
    "passes",
    "scannedRecords",
    "scannedBytes",
    "writtenRecords",
    "writtenBytes",
    "unavailableRecords",
    "decodeErrorRecords",
    "originalUnavailableRecords",
  ] as const;
  exactKeys(counts, keys, path);
  return Object.fromEntries(
    keys.map((key) => [key, nonNegativeInteger(counts[key], `${path}.${key}`)]),
  ) as unknown as RecordExportCounts;
}

function parseHash(value: unknown, path: string): string {
  const hash = text(value, path, 64);
  if (!/^[0-9a-f]{64}$/u.test(hash))
    throw new HostContractValidationError(path, "must be a lowercase SHA-256 digest");
  return hash;
}

function parseOutput(value: unknown, path: string): RecordExportOutput {
  const output = record(value, path);
  exactKeys(output, ["format", "fileName", "bytes", "sha256"], path);
  const format = declaredValue(output.format, ["csv", "jsonl"], `${path}.format`);
  const fileName = text(output.fileName, `${path}.fileName`, 200);
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/u.test(fileName) || !fileName.endsWith(`.${format}`))
    throw new HostContractValidationError(
      `${path}.fileName`,
      "must be a safe basename with the declared format",
    );
  const bytes = nonNegativeInteger(output.bytes, `${path}.bytes`);
  if (bytes > RECORD_EXPORT_LIMITS.bytes)
    throw new HostContractValidationError(`${path}.bytes`, "exceeds the export limit");
  return { format, fileName, bytes, sha256: parseHash(output.sha256, `${path}.sha256`) };
}

function parseArtifact(value: unknown, path: string): RecordExportArtifact {
  const artifact = record(value, path);
  exactKeys(artifact, ["artifactId", "output", "receiptBytes", "receiptSha256", "expiresAt"], path);
  return {
    artifactId: parseRecordExportId(artifact.artifactId, `${path}.artifactId`),
    output: parseOutput(artifact.output, `${path}.output`),
    receiptBytes: positiveBoundedInteger(
      artifact.receiptBytes,
      `${path}.receiptBytes`,
      RECORD_EXPORT_LIMITS.receiptBytes,
    ),
    receiptSha256: parseHash(artifact.receiptSha256, `${path}.receiptSha256`),
    expiresAt: canonicalIsoTimestamp(artifact.expiresAt, `${path}.expiresAt`),
  };
}

function assertComplete(
  value: Pick<RecordExportOperation, "coverage" | "counts" | "reason">,
  path: string,
): void {
  if (
    value.reason !== "range-complete" ||
    value.coverage?.reason !== "range-complete" ||
    value.counts.unavailableRecords !== 0 ||
    value.coverage.unavailableRecords !== 0 ||
    value.coverage.scannedRecords !== value.counts.scannedRecords ||
    value.coverage.scannedBytes !== value.counts.scannedBytes ||
    value.coverage.matchedRecords !== value.counts.writtenRecords
  )
    throw new HostContractValidationError(
      path,
      "completion requires fully scanned coverage without unavailable matches",
    );
}

function assertCounts(
  value: Pick<RecordExportOperation, "counts" | "input" | "limits">,
  path: string,
): void {
  if (
    value.counts.writtenRecords > Math.min(value.input.maxRecords, value.limits.records) ||
    value.counts.writtenBytes > value.limits.bytes ||
    value.counts.decodeErrorRecords > value.counts.writtenRecords ||
    value.counts.originalUnavailableRecords > value.counts.writtenRecords
  )
    throw new HostContractValidationError(
      `${path}.counts`,
      "exceeds the declared export limits or written record count",
    );
}

function parseOperation(value: unknown, path: string): RecordExportOperation {
  const operation = record(value, path);
  exactKeys(
    operation,
    [
      "jobId",
      "state",
      "input",
      "source",
      "settings",
      "limits",
      "startedAt",
      "completedAt",
      "counts",
      "coverage",
      "reason",
      "artifact",
      "error",
    ],
    path,
  );
  const parsed: RecordExportOperation = {
    jobId: parseRecordExportId(operation.jobId, `${path}.jobId`),
    state: declaredValue(operation.state, RECORD_EXPORT_STATES, `${path}.state`),
    input: parseRecordExportInput(operation.input, `${path}.input`),
    source: parseSource(operation.source, `${path}.source`),
    settings: parseSettings(operation.settings, `${path}.settings`),
    limits: parseLimits(operation.limits, `${path}.limits`),
    startedAt: canonicalIsoTimestamp(operation.startedAt, `${path}.startedAt`),
    completedAt:
      operation.completedAt === null
        ? null
        : canonicalIsoTimestamp(operation.completedAt, `${path}.completedAt`),
    counts: parseCounts(operation.counts, `${path}.counts`),
    coverage:
      operation.coverage === null
        ? null
        : parseKafkaReadCoverage(operation.coverage, `${path}.coverage`),
    reason:
      operation.reason === null
        ? null
        : declaredValue(operation.reason, RECORD_EXPORT_REASONS, `${path}.reason`),
    artifact:
      operation.artifact === null ? null : parseArtifact(operation.artifact, `${path}.artifact`),
    error: operation.error === null ? null : parseHostError(operation.error, `${path}.error`),
  };
  if (
    parsed.artifact &&
    (!["completed", "partial"].includes(parsed.state) ||
      parsed.completedAt === null ||
      parsed.artifact.output.format !== parsed.input.format ||
      parsed.artifact.output.bytes !== parsed.counts.writtenBytes)
  )
    throw new HostContractValidationError(
      `${path}.artifact`,
      "must describe the completed or partial output",
    );
  if (
    ["completed", "partial"].includes(parsed.state) &&
    (parsed.artifact === null || parsed.reason === null)
  )
    throw new HostContractValidationError(
      path,
      "a completed or partial export requires a sealed artifact and stopping reason",
    );
  if (parsed.state === "completed") assertComplete(parsed, path);
  if (parsed.state === "partial" && parsed.reason === "range-complete")
    throw new HostContractValidationError(
      path,
      "a partial export requires its actual stopping reason",
    );
  assertCounts(parsed, path);
  return parsed;
}

export function parseRecordExportSnapshot(
  value: unknown,
  path = "recordExportSnapshot",
): RecordExportSnapshot {
  const snapshot = record(value, path);
  exactKeys(snapshot, ["scopeId", "revision", "available", "operation"], path);
  return {
    scopeId: parseRecordExportId(snapshot.scopeId, `${path}.scopeId`),
    revision: nonNegativeInteger(snapshot.revision, `${path}.revision`),
    available: truth(snapshot.available, `${path}.available`),
    operation:
      snapshot.operation === null ? null : parseOperation(snapshot.operation, `${path}.operation`),
  };
}

export function parseRecordExportReceipt(
  value: unknown,
  path = "recordExportReceipt",
): RecordExportReceipt {
  const receipt = record(value, path);
  exactKeys(
    receipt,
    [
      "schema",
      "limits",
      "jobId",
      "input",
      "source",
      "settings",
      "startedAt",
      "completedAt",
      "outcome",
      "reason",
      "counts",
      "coverage",
      "output",
    ],
    path,
  );
  const parsed: RecordExportReceipt = {
    schema: declaredValue(receipt.schema, ["streamskope.record-export/v1"], `${path}.schema`),
    limits: parseLimits(receipt.limits, `${path}.limits`),
    jobId: parseRecordExportId(receipt.jobId, `${path}.jobId`),
    input: parseRecordExportInput(receipt.input, `${path}.input`),
    source: parseSource(receipt.source, `${path}.source`),
    settings: parseSettings(receipt.settings, `${path}.settings`),
    startedAt: canonicalIsoTimestamp(receipt.startedAt, `${path}.startedAt`),
    completedAt: canonicalIsoTimestamp(receipt.completedAt, `${path}.completedAt`),
    outcome: declaredValue(receipt.outcome, ["complete", "partial"], `${path}.outcome`),
    reason: declaredValue(receipt.reason, RECORD_EXPORT_REASONS, `${path}.reason`),
    counts: parseCounts(receipt.counts, `${path}.counts`),
    coverage:
      receipt.coverage === null
        ? null
        : parseKafkaReadCoverage(receipt.coverage, `${path}.coverage`),
    output: parseOutput(receipt.output, `${path}.output`),
  };
  if (
    parsed.output.format !== parsed.input.format ||
    parsed.output.bytes !== parsed.counts.writtenBytes ||
    (parsed.outcome === "complete") !== (parsed.reason === "range-complete")
  )
    throw new HostContractValidationError(
      path,
      "must describe the actual output and completion reason",
    );
  if (parsed.outcome === "complete") assertComplete(parsed, path);
  assertCounts(parsed, path);
  return parsed;
}
