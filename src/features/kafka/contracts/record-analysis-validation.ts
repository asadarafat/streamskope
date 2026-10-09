import {
  parseFiniteRecordFields,
  parseRecordReadId,
  parseRecordReadSettings,
  parseRecordReadSource,
} from "./finite-record-validation";
import { parseHostError } from "./host-error-validation";
import { utf8ByteLength } from "./message-limits";
import { parseKafkaReadCoverage } from "./query-search";
import {
  RECORD_ANALYSIS_LIMITS,
  RECORD_ANALYSIS_REASONS,
  RECORD_ANALYSIS_STATES,
  RECORD_ANALYSIS_UNAVAILABLE,
  recordAnalysisGroupIdentity,
  type RecordAnalysisCell,
  type RecordAnalysisColumn,
  type RecordAnalysisColumnCounts,
  type RecordAnalysisCounts,
  type RecordAnalysisGroupKey,
  type RecordAnalysisGrouping,
  type RecordAnalysisInput,
  type RecordAnalysisLimits,
  type RecordAnalysisOperation,
  type RecordAnalysisResult,
  type RecordAnalysisRow,
  type RecordAnalysisSnapshot,
} from "./record-analysis";
import { compileKafkaProjectionPath } from "./rule-expression-parser";
import { HostContractValidationError } from "./validation-error";
import {
  canonicalIsoTimestamp,
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  positiveBoundedInteger,
  record,
  text,
} from "./validation-primitives";

function requireValue(condition: boolean, path: string, message: string): asserts condition {
  if (!condition) throw new HostContractValidationError(path, message);
}
function array(value: unknown, path: string, maximum: number): unknown[] {
  requireValue(
    Array.isArray(value) && value.length <= maximum,
    path,
    `must contain at most ${String(maximum)} entries`,
  );
  return value;
}
function jsonBytes(value: unknown): number {
  return utf8ByteLength(JSON.stringify(value));
}
function columnId(value: unknown, path: string): string {
  const id = text(value, path, 64);
  requireValue(
    /^[A-Za-z0-9_-]{1,64}$/u.test(id),
    path,
    "must use letters, digits, underscores or hyphens",
  );
  return id;
}
function parseColumn(value: unknown, path: string): RecordAnalysisColumn {
  const input = record(value, path);
  exactKeys(input, ["id", "label", "source", "path"], path);
  const selector = text(input.path, `${path}.path`, RECORD_ANALYSIS_LIMITS.pathCharacters);
  try {
    compileKafkaProjectionPath(selector);
  } catch {
    throw new HostContractValidationError(
      `${path}.path`,
      "must be a root JSONPath with only property or index segments, within the path limits",
    );
  }
  const label = text(input.label, `${path}.label`, 64);
  requireValue(label.trim().length > 0, `${path}.label`, "must not be blank");
  return {
    id: columnId(input.id, `${path}.id`),
    label,
    source: declaredValue(input.source, ["key", "value"], `${path}.source`),
    path: selector,
  };
}

export function parseRecordAnalysisInput(
  value: unknown,
  path = "recordAnalysis",
): RecordAnalysisInput {
  const input = record(value, path);
  exactKeys(
    input,
    ["requestId", "topic", "range", "search", "maxRecords", "columns", "groupBy"],
    path,
  );
  const columns = array(input.columns, `${path}.columns`, RECORD_ANALYSIS_LIMITS.columns).map(
    (column, index) => parseColumn(column, `${path}.columns[${String(index)}]`),
  );
  requireValue(
    new Set(columns.map((column) => column.id)).size === columns.length,
    `${path}.columns`,
    "must have unique field IDs",
  );
  const groupBy = input.groupBy === null ? null : columnId(input.groupBy, `${path}.groupBy`);
  requireValue(
    groupBy === null || columns.some((column) => column.id === groupBy),
    `${path}.groupBy`,
    "must reference a selected field",
  );
  return {
    requestId: parseRecordReadId(input.requestId, `${path}.requestId`),
    ...parseFiniteRecordFields(input, path),
    columns,
    groupBy,
  };
}

function parseLimits(value: unknown, path: string): RecordAnalysisLimits {
  const input = record(value, path);
  exactKeys(input, Object.keys(RECORD_ANALYSIS_LIMITS), path);
  return Object.fromEntries(
    Object.entries(RECORD_ANALYSIS_LIMITS).map(([key, maximum]) => [
      key,
      positiveBoundedInteger(input[key], `${path}.${key}`, maximum),
    ]),
  ) as unknown as RecordAnalysisLimits;
}
function parseCounts(value: unknown, path: string): RecordAnalysisCounts {
  const input = record(value, path);
  const keys = [
    "passes",
    "scannedRecords",
    "scannedBytes",
    "countedRecords",
    "unavailableRecords",
  ] as const;
  exactKeys(input, keys, path);
  return Object.fromEntries(
    keys.map((key) => [key, nonNegativeInteger(input[key], `${path}.${key}`)]),
  ) as unknown as RecordAnalysisCounts;
}
function parseCell(value: unknown, path: string, maximumScalarBytes: number): RecordAnalysisCell {
  const input = record(value, path);
  const state = declaredValue(
    input.state,
    ["scalar", "missing", "null-key", "tombstone", "masked", "unavailable"],
    `${path}.state`,
  );
  if (state === "scalar") {
    exactKeys(input, ["state", "value"], path);
    const scalar = input.value;
    requireValue(
      scalar === null ||
        typeof scalar === "string" ||
        typeof scalar === "boolean" ||
        (typeof scalar === "number" && Number.isFinite(scalar)),
      `${path}.value`,
      "must be a finite JSON scalar",
    );
    requireValue(
      jsonBytes(scalar) <= maximumScalarBytes,
      `${path}.value`,
      "exceeds the scalar byte limit",
    );
    return { state, value: scalar };
  }
  if (state === "unavailable") {
    exactKeys(input, ["state", "reason"], path);
    return {
      state,
      reason: declaredValue(input.reason, RECORD_ANALYSIS_UNAVAILABLE, `${path}.reason`),
    };
  }
  exactKeys(input, ["state"], path);
  return { state };
}
function parseColumnCounts(
  value: unknown,
  column: RecordAnalysisColumn,
  count: number,
  path: string,
): RecordAnalysisColumnCounts {
  const input = record(value, path);
  const keys = ["scalar", "missing", "nullKey", "tombstone", "masked", "unavailable"] as const;
  exactKeys(input, ["columnId", ...keys], path);
  requireValue(
    input.columnId === column.id,
    `${path}.columnId`,
    "must match the ordered selected field",
  );
  const counts = Object.fromEntries(
    keys.map((key) => [key, nonNegativeInteger(input[key], `${path}.${key}`)]),
  ) as unknown as Omit<RecordAnalysisColumnCounts, "columnId">;
  requireValue(
    keys.reduce((sum, key) => sum + counts[key], 0) === count,
    path,
    "field totals must match the counted records",
  );
  return { columnId: column.id, ...counts };
}
function parseGrouping(
  value: unknown,
  input: RecordAnalysisInput,
  limits: RecordAnalysisLimits,
  count: number,
  path: string,
): RecordAnalysisGrouping | null {
  if (input.groupBy === null) {
    requireValue(value === null, path, "must be null without a count-by field");
    return null;
  }
  const grouping = record(value, path);
  exactKeys(grouping, ["groups", "groupedRecords", "excluded"], path);
  const keys = new Set<string>();
  const groups = array(grouping.groups, `${path}.groups`, limits.groups).map((value, index) => {
    const at = `${path}.groups[${String(index)}]`;
    const group = record(value, at);
    exactKeys(group, ["key", "count"], at);
    const cell = parseCell(group.key, `${at}.key`, limits.groupKeyBytes);
    requireValue(
      cell.state !== "masked" && cell.state !== "unavailable",
      `${at}.key`,
      "cannot group excluded fields",
    );
    const key: RecordAnalysisGroupKey = cell;
    const identity = recordAnalysisGroupIdentity(key);
    requireValue(
      utf8ByteLength(identity) <= limits.groupKeyBytes,
      `${at}.key`,
      "exceeds the exact group key byte limit",
    );
    requireValue(!keys.has(identity), `${at}.key`, "must be unique");
    keys.add(identity);
    return { key, count: positiveBoundedInteger(group.count, `${at}.count`, limits.records) };
  });
  const groupedRecords = nonNegativeInteger(grouping.groupedRecords, `${path}.groupedRecords`);
  const rawExcluded = record(grouping.excluded, `${path}.excluded`);
  exactKeys(rawExcluded, ["masked", "unavailable"], `${path}.excluded`);
  const excluded = {
    masked: nonNegativeInteger(rawExcluded.masked, `${path}.excluded.masked`),
    unavailable: nonNegativeInteger(rawExcluded.unavailable, `${path}.excluded.unavailable`),
  };
  requireValue(
    groups.reduce((sum, group) => sum + group.count, 0) === groupedRecords &&
      groupedRecords + excluded.masked + excluded.unavailable === count,
    path,
    "grouped and excluded totals must match counted records",
  );
  return { groups, groupedRecords, excluded };
}
function parseRow(
  value: unknown,
  input: RecordAnalysisInput,
  limits: RecordAnalysisLimits,
  path: string,
): RecordAnalysisRow {
  const row = record(value, path);
  exactKeys(row, ["partition", "offset", "timestamp", "cells"], path);
  const cells = array(row.cells, `${path}.cells`, input.columns.length).map((cell, index) =>
    parseCell(cell, `${path}.cells[${String(index)}]`, limits.cellBytes),
  );
  requireValue(
    cells.length === input.columns.length,
    `${path}.cells`,
    "must match selected fields",
  );
  const offset = text(row.offset, `${path}.offset`, 20);
  requireValue(
    /^(?:0|[1-9]\d*)$/u.test(offset) && BigInt(offset) <= 9_223_372_036_854_775_807n,
    `${path}.offset`,
    "must be a Kafka offset",
  );
  return {
    partition: nonNegativeInteger(row.partition, `${path}.partition`),
    offset,
    timestamp: canonicalIsoTimestamp(row.timestamp, `${path}.timestamp`),
    cells,
  };
}
function parseResult(
  value: unknown,
  operation: Pick<RecordAnalysisOperation, "input" | "limits" | "counts">,
  path: string,
): RecordAnalysisResult {
  const input = record(value, path);
  exactKeys(
    input,
    ["columns", "grouping", "preview", "previewOmittedRecords", "previewBytes", "workUnits"],
    path,
  );
  const { counts, limits } = operation;
  const rawColumns = array(input.columns, `${path}.columns`, operation.input.columns.length);
  requireValue(
    rawColumns.length === operation.input.columns.length,
    `${path}.columns`,
    "must match selected fields",
  );
  const columns = rawColumns.map((column, index) =>
    parseColumnCounts(
      column,
      operation.input.columns[index]!,
      counts.countedRecords,
      `${path}.columns[${String(index)}]`,
    ),
  );
  const grouping = parseGrouping(
    input.grouping,
    operation.input,
    limits,
    counts.countedRecords,
    `${path}.grouping`,
  );
  const preview = array(input.preview, `${path}.preview`, limits.previewRows).map((row, index) =>
    parseRow(row, operation.input, limits, `${path}.preview[${String(index)}]`),
  );
  requireValue(
    new Set(preview.map((row) => `${String(row.partition)}:${row.offset}`)).size === preview.length,
    `${path}.preview`,
    "must not repeat record locators",
  );
  const previewOmittedRecords = nonNegativeInteger(
    input.previewOmittedRecords,
    `${path}.previewOmittedRecords`,
  );
  const previewBytes = nonNegativeInteger(input.previewBytes, `${path}.previewBytes`);
  const workUnits = nonNegativeInteger(input.workUnits, `${path}.workUnits`);
  requireValue(
    preview.length + previewOmittedRecords === counts.countedRecords,
    path,
    "preview and omitted records must match counted records",
  );
  requireValue(
    previewBytes === jsonBytes(preview) && previewBytes <= limits.previewBytes,
    `${path}.previewBytes`,
    "must match the bounded serialized preview",
  );
  requireValue(workUnits <= limits.work, `${path}.workUnits`, "exceeds the work limit");
  const result = { columns, grouping, preview, previewOmittedRecords, previewBytes, workUnits };
  requireValue(jsonBytes(result) <= limits.resultBytes, path, "exceeds the result byte limit");
  return result;
}

function parseOperation(value: unknown, path: string): RecordAnalysisOperation {
  const raw = record(value, path);
  exactKeys(
    raw,
    [
      "jobId",
      "input",
      "state",
      "source",
      "settings",
      "limits",
      "startedAt",
      "completedAt",
      "counts",
      "coverage",
      "reason",
      "result",
      "error",
    ],
    path,
  );
  const input = parseRecordAnalysisInput(raw.input, `${path}.input`);
  const limits = parseLimits(raw.limits, `${path}.limits`);
  const counts = parseCounts(raw.counts, `${path}.counts`);
  const state = declaredValue(raw.state, RECORD_ANALYSIS_STATES, `${path}.state`);
  const reason =
    raw.reason === null
      ? null
      : declaredValue(raw.reason, RECORD_ANALYSIS_REASONS, `${path}.reason`);
  const coverage =
    raw.coverage === null ? null : parseKafkaReadCoverage(raw.coverage, `${path}.coverage`);
  const startedAt = canonicalIsoTimestamp(raw.startedAt, `${path}.startedAt`);
  const completedAt =
    raw.completedAt === null ? null : canonicalIsoTimestamp(raw.completedAt, `${path}.completedAt`);
  const result =
    raw.result === null
      ? null
      : parseResult(raw.result, { input, limits, counts }, `${path}.result`);
  requireValue(
    input.maxRecords <= limits.records && input.columns.length <= limits.columns,
    path,
    "input exceeds effective host limits",
  );
  requireValue(
    input.columns.every(
      (column) =>
        column.path.length <= limits.pathCharacters &&
        compileKafkaProjectionPath(column.path).segments.length <= limits.pathSegments,
    ),
    `${path}.input.columns`,
    "exceeds effective path limits",
  );
  requireValue(
    counts.countedRecords + counts.unavailableRecords <= counts.scannedRecords,
    `${path}.counts`,
    "counted and unavailable records must not exceed scanned records",
  );
  requireValue(
    counts.countedRecords <= input.maxRecords &&
      counts.scannedRecords <= limits.scanRecords &&
      counts.scannedBytes <= limits.scanBytes &&
      counts.passes <= limits.passes &&
      counts.unavailableRecords <= counts.scannedRecords,
    `${path}.counts`,
    "exceeds effective read limits",
  );
  requireValue(
    completedAt === null || Date.parse(completedAt) >= Date.parse(startedAt),
    `${path}.completedAt`,
    "must not precede the start",
  );
  if (state === "preparing" || state === "reading")
    requireValue(
      completedAt === null && reason === null && raw.error === null,
      path,
      "active reads cannot claim a terminal outcome",
    );
  if (state === "stopping")
    requireValue(
      completedAt === null && (reason === null || reason === "revoked") && raw.error === null,
      path,
      "stopping reads must await their final outcome",
    );
  if (state === "completed" || state === "partial") {
    requireValue(
      result !== null && completedAt !== null && reason !== null && raw.error === null,
      path,
      "terminal results require a result, time and reason without an error",
    );
    requireValue(
      coverage === null ||
        (coverage.matchedRecords === counts.countedRecords &&
          coverage.scannedRecords === counts.scannedRecords &&
          coverage.scannedBytes === counts.scannedBytes &&
          coverage.unavailableRecords === counts.unavailableRecords),
      `${path}.coverage`,
      "must match terminal count accounting",
    );
  }
  if (state === "partial")
    requireValue(
      reason !== null &&
        !["read-failed", "analysis-failed", "cleanup-failed", "revoked", "range-complete"].includes(
          reason,
        ),
      `${path}.reason`,
      "must describe a bounded partial result, not a failed operation",
    );
  if ((state === "completed" || state === "partial") && result !== null) {
    const bounds = new Map(coverage?.partitions.map((item) => [item.partition, item]));
    requireValue(
      result.preview.every((row) => {
        const partition = bounds.get(row.partition);
        return (
          partition !== undefined &&
          BigInt(row.offset) >= BigInt(partition.startOffset) &&
          BigInt(row.offset) < BigInt(partition.nextOffset)
        );
      }),
      `${path}.result.preview`,
      "must remain within acknowledged captured offsets",
    );
  }
  if (state === "completed") {
    requireValue(
      reason === "range-complete" &&
        coverage?.reason === "range-complete" &&
        counts.unavailableRecords === 0,
      path,
      "complete counts require full captured coverage without unknown filters",
    );
  } else
    requireValue(
      reason !== "range-complete",
      `${path}.reason`,
      "range-complete requires completed state",
    );
  if (state === "failed")
    requireValue(
      result === null && completedAt !== null && reason !== null && raw.error !== null,
      path,
      "failed operations must remove results and explain the outcome",
    );
  if (state === "revoked")
    requireValue(
      result === null && completedAt !== null && reason === "revoked",
      path,
      "revoked operations must remove results and report revoked authority",
    );
  return {
    jobId: parseRecordReadId(raw.jobId, `${path}.jobId`),
    input,
    state,
    source: parseRecordReadSource(raw.source, `${path}.source`),
    settings: parseRecordReadSettings(raw.settings, `${path}.settings`),
    limits,
    startedAt,
    completedAt,
    counts,
    coverage,
    reason,
    result,
    error: raw.error === null ? null : parseHostError(raw.error, `${path}.error`),
  };
}

export function parseRecordAnalysisSnapshot(
  value: unknown,
  path = "recordAnalysisSnapshot",
): RecordAnalysisSnapshot {
  const input = record(value, path);
  exactKeys(input, ["scopeId", "revision", "operation"], path);
  const snapshot = {
    scopeId: parseRecordReadId(input.scopeId, `${path}.scopeId`),
    revision: nonNegativeInteger(input.revision, `${path}.revision`),
    operation:
      input.operation === null ? null : parseOperation(input.operation, `${path}.operation`),
  };
  requireValue(
    jsonBytes(snapshot) <= 1_024 * 1_024,
    path,
    "exceeds the analysis snapshot byte limit",
  );
  return snapshot;
}
