import { parseKafkaInvestigationQuery, type KafkaInvestigationQuery } from "./investigation-query";
import { HostContractValidationError } from "./validation-error";
import { declaredValue, exactKeys, record, text } from "./validation-primitives";

export const KAFKA_QUERY_LIBRARY_LIMITS = {
  queries: 100,
  fileBytes: 1_048_576,
  nameCharacters: 128,
} as const;

export interface KafkaSavedQuery {
  readonly id: string;
  readonly name: string;
  readonly profileId?: string;
  readonly configuration: KafkaInvestigationQuery;
}

export interface KafkaQueryLibrarySnapshot {
  readonly durability: "session" | "durable";
  readonly queries: readonly KafkaSavedQuery[];
}

export function parseKafkaSavedQuery(value: unknown, path = "savedQuery"): KafkaSavedQuery {
  const input = record(value, path);
  exactKeys(input, ["id", "name", "profileId", "configuration"], path);
  const name = text(input.name, `${path}.name`, KAFKA_QUERY_LIBRARY_LIMITS.nameCharacters).trim();
  if (name.length === 0) throw new HostContractValidationError(`${path}.name`, "must not be blank");
  return {
    id: text(input.id, `${path}.id`, 128),
    name,
    ...(input.profileId === undefined
      ? {}
      : { profileId: text(input.profileId, `${path}.profileId`, 128) }),
    configuration: parseKafkaInvestigationQuery(input.configuration),
  };
}

export function parseKafkaQueryLibraryDocument(value: unknown): {
  readonly schemaVersion: 1;
  readonly queries: readonly KafkaSavedQuery[];
} {
  const input = record(value, "queryLibrary");
  exactKeys(input, ["schemaVersion", "queries"], "queryLibrary");
  if (
    input.schemaVersion !== 1 ||
    !Array.isArray(input.queries) ||
    input.queries.length > KAFKA_QUERY_LIBRARY_LIMITS.queries
  ) {
    throw new HostContractValidationError("queryLibrary", "unsupported or oversized query library");
  }
  const queries = input.queries.map((query: unknown, index: number) =>
    parseKafkaSavedQuery(query, `queries[${index}]`),
  );
  if (
    new Set(queries.map((query) => query.id)).size !== queries.length ||
    new Set(queries.map((query) => query.name.toLowerCase())).size !== queries.length
  ) {
    throw new HostContractValidationError("queryLibrary", "query IDs and names must be unique");
  }
  return { schemaVersion: 1, queries };
}

export function parseKafkaQueryLibrarySnapshot(value: unknown): KafkaQueryLibrarySnapshot {
  const input = record(value, "queryLibrary");
  exactKeys(input, ["durability", "queries"], "queryLibrary");
  return {
    durability: declaredValue(input.durability, ["session", "durable"], "queryLibrary.durability"),
    queries: parseKafkaQueryLibraryDocument({ schemaVersion: 1, queries: input.queries }).queries,
  };
}
