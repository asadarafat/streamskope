import {
  FINITE_RECORD_READ_LIMITS,
  type FiniteRecordInput,
  type FiniteRecordRange,
  type RecordReadSettings,
  type RecordReadSource,
} from "./finite-record-read";
import { parseKafkaRecordProtection } from "./operational-preference-validation";
import { parseKafkaSearchFilter } from "./query-search";
import { parseRecordCodecPreferences } from "./structured-record";
import { HostContractValidationError } from "./validation-error";
import {
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  nullableText,
  positiveBoundedInteger,
  record,
  text,
} from "./validation-primitives";

export function parseRecordReadId(value: unknown, path: string): string {
  const id = text(value, path, 36);
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(id))
    throw new HostContractValidationError(path, "must be a UUID");
  return id;
}

export function parseFiniteRecordRange(value: unknown, path: string): FiniteRecordRange {
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

/** The operation parser owns exact keys; these finite read fields are shared. */
export function parseFiniteRecordFields(
  input: Record<string, unknown>,
  path: string,
): FiniteRecordInput {
  const topic = text(input.topic, `${path}.topic`, 512);
  if (topic.trim().length === 0)
    throw new HostContractValidationError(`${path}.topic`, "must not be blank");
  return {
    topic,
    range: parseFiniteRecordRange(input.range, `${path}.range`),
    search: parseKafkaSearchFilter(input.search, `${path}.search`),
    maxRecords: positiveBoundedInteger(
      input.maxRecords,
      `${path}.maxRecords`,
      FINITE_RECORD_READ_LIMITS.records,
    ),
  };
}

export function parseRecordReadSettings(value: unknown, path: string): RecordReadSettings {
  const settings = record(value, path);
  exactKeys(settings, ["codecs", "protection"], path);
  return {
    codecs: parseRecordCodecPreferences(settings.codecs, `${path}.codecs`),
    protection: parseKafkaRecordProtection(settings.protection, `${path}.protection`),
  };
}

export function parseRecordReadSource(value: unknown, path: string): RecordReadSource {
  const source = record(value, path);
  exactKeys(source, ["connectionName", "clusterId", "topicId"], path);
  return {
    connectionName: text(source.connectionName, `${path}.connectionName`, 256),
    clusterId: nullableText(source.clusterId, `${path}.clusterId`, 256),
    topicId: nullableText(source.topicId, `${path}.topicId`, 256),
  };
}
