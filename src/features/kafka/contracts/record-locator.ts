import { parseRecordReadId } from "./finite-record-validation";
import type { KafkaMessage } from "./types";
import { HostContractValidationError } from "./validation-error";
import { exactKeys, nonNegativeInteger, record, text } from "./validation-primitives";

export const KAFKA_RECORD_LOCATOR_LIMITS = Object.freeze({
  bookmarksPerView: 32,
  bookmarksPerLibrary: 256,
  nameCharacters: 128,
  responseBytes: 1_048_576,
  durationMs: 30_000,
});

/** Evidence from the delivering reader, not a later metadata lookup. */
export interface KafkaRecordProvenance {
  readonly clusterId: string;
  readonly topicId: string;
  readonly leaderEpoch: number;
}
export interface KafkaRecordLocator extends KafkaRecordProvenance {
  readonly schemaVersion: 1;
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
}
export interface KafkaRecordBookmark {
  readonly id: string;
  readonly name: string;
  readonly locator: KafkaRecordLocator;
}
export interface KafkaSavedRecordContext {
  readonly selected: KafkaRecordLocator | null;
  readonly comparison: KafkaRecordLocator | null;
  readonly bookmarks: readonly KafkaRecordBookmark[];
}
export interface KafkaRecordLocatorLoadInput {
  readonly requestId: string;
  readonly locator: KafkaRecordLocator;
}
export const KAFKA_RECORD_LOCATOR_REASONS = [
  "expired",
  "resource-replaced",
  "record-replaced",
  "topic-missing",
  "inaccessible",
  "record-missing",
  "unavailable",
  "cancelled",
  "revoked",
] as const;
export type KafkaRecordLocatorReason = (typeof KAFKA_RECORD_LOCATOR_REASONS)[number];
export type KafkaRecordLocatorOutcome = KafkaRecordLocatorLoadInput &
  (
    | { readonly state: "loaded"; readonly message: KafkaMessage }
    | { readonly state: KafkaRecordLocatorReason; readonly detail: string }
  );

function boundedInt32(value: unknown, path: string): number {
  const result = nonNegativeInteger(value, path);
  if (result > 2_147_483_647)
    throw new HostContractValidationError(path, "must be a nonnegative signed 32-bit integer");
  return result;
}
function provenanceFields(input: Record<string, unknown>, path: string): KafkaRecordProvenance {
  const clusterId = text(input.clusterId, `${path}.clusterId`, 256);
  const topicId = text(input.topicId, `${path}.topicId`, 36);
  if (!/^[A-Za-z0-9_.-]+$/u.test(clusterId) || /^0+$/u.test(clusterId.replaceAll("-", "")))
    throw new HostContractValidationError(`${path}.clusterId`, "requires a stable cluster ID");
  if (
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u.test(topicId) ||
    /^0+$/u.test(topicId.replaceAll("-", ""))
  )
    throw new HostContractValidationError(`${path}.topicId`, "requires a nonzero Kafka topic UUID");
  return {
    clusterId,
    topicId,
    leaderEpoch: boundedInt32(input.leaderEpoch, `${path}.leaderEpoch`),
  };
}
export function parseKafkaRecordProvenance(
  value: unknown,
  path = "provenance",
): KafkaRecordProvenance {
  const input = record(value, path);
  exactKeys(input, ["clusterId", "topicId", "leaderEpoch"], path);
  return provenanceFields(input, path);
}
export function parseKafkaRecordLocator(value: unknown, path = "locator"): KafkaRecordLocator {
  const input = record(value, path);
  exactKeys(
    input,
    ["schemaVersion", "clusterId", "topicId", "leaderEpoch", "topic", "partition", "offset"],
    path,
  );
  if (input.schemaVersion !== 1)
    throw new HostContractValidationError(
      `${path}.schemaVersion`,
      "unsupported record locator version",
    );
  const topic = text(input.topic, `${path}.topic`, 249);
  if (!/^[A-Za-z0-9._-]+$/u.test(topic) || topic === "." || topic === "..")
    throw new HostContractValidationError(`${path}.topic`, "requires a valid Kafka topic name");
  const offset = text(input.offset, `${path}.offset`, 19);
  if (!/^(0|[1-9][0-9]*)$/u.test(offset) || BigInt(offset) > 9_223_372_036_854_775_807n)
    throw new HostContractValidationError(
      `${path}.offset`,
      "requires a canonical signed 64-bit Kafka offset",
    );
  return {
    schemaVersion: 1,
    ...provenanceFields(input, path),
    topic,
    partition: boundedInt32(input.partition, `${path}.partition`),
    offset,
  };
}
export function createEmptyKafkaSavedRecordContext(): KafkaSavedRecordContext {
  return { selected: null, comparison: null, bookmarks: [] };
}
export function sameKafkaRecordLocator(a: KafkaRecordLocator, b: KafkaRecordLocator): boolean {
  return (
    a.clusterId === b.clusterId &&
    a.topicId === b.topicId &&
    a.topic === b.topic &&
    a.partition === b.partition &&
    a.offset === b.offset &&
    a.leaderEpoch === b.leaderEpoch
  );
}
export function kafkaRecordLocator(
  message: Pick<KafkaMessage, "topic" | "partition" | "offset"> & {
    readonly provenance?: KafkaRecordProvenance | undefined;
  },
): KafkaRecordLocator | null {
  if (message.provenance === undefined) return null;
  try {
    return parseKafkaRecordLocator({
      schemaVersion: 1,
      ...message.provenance,
      topic: message.topic,
      partition: message.partition,
      offset: message.offset,
    });
  } catch {
    return null;
  }
}
export function parseKafkaSavedRecordContext(
  value: unknown,
  path = "records",
): KafkaSavedRecordContext {
  const input = record(value, path);
  exactKeys(input, ["selected", "comparison", "bookmarks"], path);
  const selected =
    input.selected === null ? null : parseKafkaRecordLocator(input.selected, `${path}.selected`);
  const comparison =
    input.comparison === null
      ? null
      : parseKafkaRecordLocator(input.comparison, `${path}.comparison`);
  if (
    !Array.isArray(input.bookmarks) ||
    input.bookmarks.length > KAFKA_RECORD_LOCATOR_LIMITS.bookmarksPerView
  )
    throw new HostContractValidationError(
      `${path}.bookmarks`,
      "at most 32 bookmarks fit in a view",
    );
  const bookmarks = input.bookmarks.map((value: unknown, index: number): KafkaRecordBookmark => {
    const itemPath = `${path}.bookmarks[${String(index)}]`;
    const entry = record(value, itemPath);
    exactKeys(entry, ["id", "name", "locator"], itemPath);
    const name = text(
      entry.name,
      `${itemPath}.name`,
      KAFKA_RECORD_LOCATOR_LIMITS.nameCharacters,
    ).trim();
    if (!name) throw new HostContractValidationError(`${itemPath}.name`, "must not be blank");
    return {
      id: text(entry.id, `${itemPath}.id`, 128),
      name,
      locator: parseKafkaRecordLocator(entry.locator, `${itemPath}.locator`),
    };
  });
  if (
    new Set(bookmarks.map((entry) => entry.id)).size !== bookmarks.length ||
    new Set(bookmarks.map((entry) => entry.name.toLowerCase())).size !== bookmarks.length ||
    bookmarks.some((entry, i) =>
      bookmarks.slice(0, i).some((other) => sameKafkaRecordLocator(entry.locator, other.locator)),
    )
  )
    throw new HostContractValidationError(
      `${path}.bookmarks`,
      "bookmark IDs, names and positions must be unique",
    );
  const locators = [selected, comparison, ...bookmarks.map((entry) => entry.locator)].filter(
    (entry) => entry !== null,
  );
  if (new Set(locators.map((entry) => entry.clusterId)).size > 1)
    throw new HostContractValidationError(
      path,
      "all saved positions in a view must identify the same cluster",
    );
  return { selected, comparison, bookmarks };
}
export function parseKafkaRecordLocatorLoadInput(
  value: unknown,
  path = "locatorLoad",
): KafkaRecordLocatorLoadInput {
  const input = record(value, path);
  exactKeys(input, ["requestId", "locator"], path);
  return {
    requestId: parseRecordReadId(input.requestId, `${path}.requestId`),
    locator: parseKafkaRecordLocator(input.locator, `${path}.locator`),
  };
}
