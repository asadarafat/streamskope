import {
  KAFKA_CONFIGURATION_LIMITS,
  KAFKA_CONFIGURATION_SOURCES,
  KAFKA_CONFIGURATION_TYPES,
  type KafkaConfigurationEntry,
  type KafkaConfigurationSynonym,
} from "./configuration-types";
import { HostContractValidationError } from "./validation-error";
import {
  boundedText,
  declaredValue,
  exactKeys,
  record,
  text,
  truth,
} from "./validation-primitives";

function nullableBoundedText(value: unknown, path: string, maximum: number): string | null {
  return value === null ? null : boundedText(value, path, maximum);
}

function parseKafkaConfigurationSynonym(value: unknown, path: string): KafkaConfigurationSynonym {
  const synonym = record(value, path);
  exactKeys(synonym, ["name", "source", "value"], path);
  return {
    name: text(synonym.name, `${path}.name`, KAFKA_CONFIGURATION_LIMITS.nameCharacters),
    source: declaredValue(synonym.source, KAFKA_CONFIGURATION_SOURCES, `${path}.source`),
    value: nullableBoundedText(
      synonym.value,
      `${path}.value`,
      KAFKA_CONFIGURATION_LIMITS.valueCharacters,
    ),
  };
}

export function parseKafkaConfigurationEntry(
  value: unknown,
  path: string,
): KafkaConfigurationEntry {
  const entry = record(value, path);
  exactKeys(
    entry,
    [
      "documentation",
      "isDefault",
      "isSensitive",
      "name",
      "readOnly",
      "source",
      "synonyms",
      "type",
      "value",
    ],
    path,
  );
  if (
    !Array.isArray(entry.synonyms) ||
    entry.synonyms.length > KAFKA_CONFIGURATION_LIMITS.synonymsPerEntry
  ) {
    throw new HostContractValidationError(
      `${path}.synonyms`,
      `must contain at most ${KAFKA_CONFIGURATION_LIMITS.synonymsPerEntry} entries`,
    );
  }
  const parsed: KafkaConfigurationEntry = {
    documentation: nullableBoundedText(
      entry.documentation,
      `${path}.documentation`,
      KAFKA_CONFIGURATION_LIMITS.documentationCharacters,
    ),
    isDefault: truth(entry.isDefault, `${path}.isDefault`),
    isSensitive: truth(entry.isSensitive, `${path}.isSensitive`),
    name: text(entry.name, `${path}.name`, KAFKA_CONFIGURATION_LIMITS.nameCharacters),
    readOnly: truth(entry.readOnly, `${path}.readOnly`),
    source: declaredValue(entry.source, KAFKA_CONFIGURATION_SOURCES, `${path}.source`),
    synonyms: entry.synonyms.map((synonym, index) =>
      parseKafkaConfigurationSynonym(synonym, `${path}.synonyms[${index}]`),
    ),
    type: declaredValue(entry.type, KAFKA_CONFIGURATION_TYPES, `${path}.type`),
    value: nullableBoundedText(
      entry.value,
      `${path}.value`,
      KAFKA_CONFIGURATION_LIMITS.valueCharacters,
    ),
  };
  if (
    parsed.isSensitive &&
    (parsed.value !== null || parsed.synonyms.some((synonym) => synonym.value !== null))
  ) {
    throw new HostContractValidationError(path, "must not expose sensitive configuration values");
  }
  return parsed;
}

export function parseKafkaConfigurationEntries(
  value: unknown,
  path: string,
): readonly KafkaConfigurationEntry[] {
  if (!Array.isArray(value) || value.length > KAFKA_CONFIGURATION_LIMITS.entries) {
    throw new HostContractValidationError(
      path,
      `must contain at most ${KAFKA_CONFIGURATION_LIMITS.entries} entries`,
    );
  }
  const entries = value.map((entry, index) =>
    parseKafkaConfigurationEntry(entry, `${path}[${index}]`),
  );
  if (new Set(entries.map((entry) => entry.name)).size !== entries.length) {
    throw new HostContractValidationError(path, "must contain unique configuration names");
  }
  return entries;
}
