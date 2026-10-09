import { parseKafkaInvestigationQuery, type KafkaInvestigationQuery } from "./investigation-query";
import {
  createDefaultKafkaInvestigationView,
  parseKafkaInvestigationView,
  type KafkaInvestigationView,
} from "./investigation-view";
import { HostContractValidationError } from "./validation-error";
import { declaredValue, exactKeys, record, text } from "./validation-primitives";

export const KAFKA_QUERY_LIBRARY_LIMITS = {
  queries: 100,
  fileBytes: 1_048_576,
  nameCharacters: 128,
} as const;

export interface KafkaSavedView {
  readonly id: string;
  readonly name: string;
  readonly profileId?: string;
  /** A topic view requires its query; a standalone group has no fabricated topic. */
  readonly configuration: KafkaInvestigationQuery | null;
  readonly view: KafkaInvestigationView;
}
/** The existing queries host routes and store now carry the canonical saved view. */
export type KafkaSavedQuery = KafkaSavedView;
export interface KafkaQueryLibrarySnapshot {
  readonly durability: "session" | "durable";
  readonly queries: readonly KafkaSavedView[];
}

function parseIdentity(
  input: Record<string, unknown>,
  path: string,
): Pick<KafkaSavedView, "id" | "name" | "profileId"> {
  const name = text(input.name, `${path}.name`, KAFKA_QUERY_LIBRARY_LIMITS.nameCharacters).trim();
  if (name.length === 0) throw new HostContractValidationError(`${path}.name`, "must not be blank");
  return {
    id: text(input.id, `${path}.id`, 128),
    name,
    ...(input.profileId === undefined
      ? {}
      : { profileId: text(input.profileId, `${path}.profileId`, 128) }),
  };
}
export function parseKafkaSavedView(value: unknown, path = "savedView"): KafkaSavedView {
  const input = record(value, path);
  exactKeys(input, ["id", "name", "profileId", "configuration", "view"], path);
  const view = parseKafkaInvestigationView(input.view, `${path}.view`);
  const configuration =
    input.configuration === null ? null : parseKafkaInvestigationQuery(input.configuration);
  if (view.destination.kind === "topic" && configuration === null)
    throw new HostContractValidationError(
      `${path}.configuration`,
      "a topic view requires query settings",
    );
  return { ...parseIdentity(input, path), configuration, view };
}
export const parseKafkaSavedQuery = parseKafkaSavedView;

function parseLegacySavedQuery(value: unknown, path: string): KafkaSavedView {
  const input = record(value, path);
  exactKeys(input, ["id", "name", "profileId", "configuration"], path);
  return {
    ...parseIdentity(input, path),
    configuration: parseKafkaInvestigationQuery(input.configuration),
    view: createDefaultKafkaInvestigationView(),
  };
}

function parseStoredView(value: unknown, path: string): KafkaSavedView {
  const input = record(value, path);
  // Only the disk representation omits an exact default descriptor. Wire entries
  // always carry the complete descriptor, and group-only entries need a destination.
  return parseKafkaSavedView(
    input.view === undefined ? { ...input, view: createDefaultKafkaInvestigationView() } : input,
    path,
  );
}

function parseEntries(
  value: unknown,
  parse: (entry: unknown, path: string) => KafkaSavedView,
): readonly KafkaSavedView[] {
  if (!Array.isArray(value) || value.length > KAFKA_QUERY_LIBRARY_LIMITS.queries)
    throw new HostContractValidationError("queryLibrary.queries", "too many saved views");
  const queries = value.map((query: unknown, index: number) =>
    parse(query, `queries[${String(index)}]`),
  );
  if (
    new Set(queries.map((query) => query.id)).size !== queries.length ||
    new Set(queries.map((query) => query.name.toLowerCase())).size !== queries.length
  )
    throw new HostContractValidationError("queryLibrary", "view IDs and names must be unique");
  return queries;
}

/** Pure inspection retains the actual disk format; it never grants old hosts new capabilities. */
export function inspectKafkaQueryLibraryDocument(value: unknown): {
  readonly schemaVersion: 1 | 2;
  readonly queries: readonly KafkaSavedView[];
} {
  const input = record(value, "queryLibrary");
  exactKeys(input, ["schemaVersion", "queries"], "queryLibrary");
  if (
    (input.schemaVersion !== 1 && input.schemaVersion !== 2) ||
    !Array.isArray(input.queries) ||
    input.queries.length > KAFKA_QUERY_LIBRARY_LIMITS.queries
  )
    throw new HostContractValidationError(
      "queryLibrary",
      "unsupported or oversized investigation library",
    );
  const queries = parseEntries(
    input.queries,
    input.schemaVersion === 1 ? parseLegacySavedQuery : parseStoredView,
  );
  return { schemaVersion: input.schemaVersion, queries };
}

/** Compact defaults prevent migration from expanding an otherwise valid full library. */
export function serializeKafkaQueryLibraryDocument(queries: readonly KafkaSavedView[]): string {
  const defaultView = JSON.stringify(createDefaultKafkaInvestigationView());
  const entries = parseEntries(queries, parseKafkaSavedView).map((entry) => {
    const { view, ...stored } = entry;
    return JSON.stringify(view) === defaultView ? stored : entry;
  });
  return `${JSON.stringify({ schemaVersion: 2, queries: entries })}\n`;
}

/** Canonical in-memory form; only an explicit store mutation writes format 2. */
export function parseKafkaQueryLibraryDocument(value: unknown): {
  readonly schemaVersion: 2;
  readonly queries: readonly KafkaSavedView[];
} {
  return { schemaVersion: 2, queries: inspectKafkaQueryLibraryDocument(value).queries };
}
export function parseKafkaQueryLibrarySnapshot(value: unknown): KafkaQueryLibrarySnapshot {
  const input = record(value, "queryLibrary");
  exactKeys(input, ["durability", "queries"], "queryLibrary");
  return {
    durability: declaredValue(input.durability, ["session", "durable"], "queryLibrary.durability"),
    queries: parseEntries(input.queries, parseKafkaSavedView),
  };
}
