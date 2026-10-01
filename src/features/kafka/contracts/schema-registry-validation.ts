import {
  SCHEMA_REGISTRY_LIMITS,
  SCHEMA_REGISTRY_STATES,
  SCHEMA_REGISTRY_TYPES,
  type SchemaCompatibilityCheckInput,
  type SchemaCompatibilitySnapshot,
  type SchemaDeletionInput,
  type SchemaDefinitionInput,
  type SchemaReference,
  type SchemaRegistrationInput,
  type SchemaRegistryDetailSnapshot,
  type SchemaRegistryInventorySnapshot,
  type SchemaRegistryState,
  type SchemaSubjectVersionIdentity,
  type SchemaVersionDetail,
} from "./schema-registry-types";
import type { HostError } from "./types";
import { HostContractValidationError } from "./validation-error";
import {
  boundedText,
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  nullableText,
  record,
  text,
  truth,
} from "./validation-primitives";

function positiveVersion(value: unknown, path: string): number {
  const version = nonNegativeInteger(value, path);
  if (version < 1 || version > SCHEMA_REGISTRY_LIMITS.versions) {
    throw new HostContractValidationError(path, "must be a supported positive version");
  }
  return version;
}

function versionSelector(value: unknown, path: string): number | "latest" {
  return value === "latest" ? value : positiveVersion(value, path);
}

function schemaReference(value: unknown, path: string): SchemaReference {
  const reference = record(value, path);
  exactKeys(reference, ["name", "subject", "version"], path);
  return {
    name: text(reference.name, `${path}.name`, SCHEMA_REGISTRY_LIMITS.subjectCharacters),
    subject: text(reference.subject, `${path}.subject`, SCHEMA_REGISTRY_LIMITS.subjectCharacters),
    version: positiveVersion(reference.version, `${path}.version`),
  };
}

function schemaDefinition(value: unknown, path: string): SchemaDefinitionInput {
  const definition = record(value, path);
  const references = definition.references;
  if (!Array.isArray(references) || references.length > SCHEMA_REGISTRY_LIMITS.references) {
    throw new HostContractValidationError(`${path}.references`, "exceeds the reference bound");
  }
  return {
    references: references.map((reference, index) =>
      schemaReference(reference, `${path}.references[${String(index)}]`),
    ),
    schema: boundedText(
      definition.schema,
      `${path}.schema`,
      SCHEMA_REGISTRY_LIMITS.schemaCharacters,
    ),
    schemaType: declaredValue(definition.schemaType, SCHEMA_REGISTRY_TYPES, `${path}.schemaType`),
  };
}

export function parseSchemaIdentity(value: unknown, path: string): SchemaSubjectVersionIdentity {
  const identity = record(value, path);
  exactKeys(identity, ["subject", "version"], path);
  return {
    subject: text(identity.subject, `${path}.subject`, SCHEMA_REGISTRY_LIMITS.subjectCharacters),
    version: versionSelector(identity.version, `${path}.version`),
  };
}

export function parseSchemaCompatibilityCheckInput(
  value: unknown,
  path: string,
): SchemaCompatibilityCheckInput {
  const input = record(value, path);
  exactKeys(input, ["references", "schema", "schemaType", "subject", "version"], path);
  return {
    ...schemaDefinition(input, path),
    subject: text(input.subject, `${path}.subject`, SCHEMA_REGISTRY_LIMITS.subjectCharacters),
    version: versionSelector(input.version, `${path}.version`),
  };
}

export function parseSchemaRegistrationInput(
  value: unknown,
  path: string,
): SchemaRegistrationInput {
  const input = record(value, path);
  exactKeys(input, ["normalize", "references", "schema", "schemaType", "subject", "version"], path);
  return {
    ...schemaDefinition(input, path),
    normalize: truth(input.normalize, `${path}.normalize`),
    subject: text(input.subject, `${path}.subject`, SCHEMA_REGISTRY_LIMITS.subjectCharacters),
    version: versionSelector(input.version, `${path}.version`),
  };
}

export function parseSchemaDeletionInput(value: unknown, path: string): SchemaDeletionInput {
  const input = record(value, path);
  exactKeys(input, ["confirmation", "mode", "target"], path);
  const target = record(input.target, `${path}.target`);
  const kind = declaredValue(target.kind, ["subject", "version"], `${path}.target.kind`);
  exactKeys(
    target,
    kind === "subject" ? ["kind", "subject"] : ["kind", "subject", "version"],
    `${path}.target`,
  );
  const subject = text(
    target.subject,
    `${path}.target.subject`,
    SCHEMA_REGISTRY_LIMITS.subjectCharacters,
  );
  const parsedTarget =
    kind === "subject"
      ? ({ kind, subject } as const)
      : ({
          kind,
          subject,
          version: positiveVersion(target.version, `${path}.target.version`),
        } as const);
  const confirmation = text(input.confirmation, `${path}.confirmation`, 1_024);
  const expected = kind === "subject" ? subject : `${subject}@${String(parsedTarget.version)}`;
  if (confirmation !== expected) {
    throw new HostContractValidationError(`${path}.confirmation`, "must exactly match the target");
  }
  return {
    confirmation,
    mode: declaredValue(input.mode, ["permanent", "soft"], `${path}.mode`),
    target: parsedTarget,
  };
}

function optionalError(
  value: Record<string, unknown>,
  path: string,
  parseError: (value: unknown, path: string) => HostError,
): HostError | undefined {
  return Object.hasOwn(value, "error") ? parseError(value.error, `${path}.error`) : undefined;
}

interface SchemaSnapshotBase {
  readonly connectionName: string | null;
  readonly endpoint: string | null;
  readonly refreshedAt: string | null;
  readonly state: SchemaRegistryState;
}

function baseSnapshot(value: Record<string, unknown>, path: string): SchemaSnapshotBase {
  return {
    connectionName: nullableText(value.connectionName, `${path}.connectionName`, 256),
    endpoint: nullableText(value.endpoint, `${path}.endpoint`, 2_048),
    refreshedAt: nullableText(value.refreshedAt, `${path}.refreshedAt`, 128),
    state: declaredValue(value.state, SCHEMA_REGISTRY_STATES, `${path}.state`),
  };
}

export function parseSchemaInventorySnapshot(
  value: unknown,
  path: string,
  parseError: (value: unknown, path: string) => HostError,
): SchemaRegistryInventorySnapshot {
  const snapshot = record(value, path);
  exactKeys(
    snapshot,
    ["connectionName", "endpoint", "error", "omittedSubjects", "refreshedAt", "state", "subjects"],
    path,
  );
  if (
    !Array.isArray(snapshot.subjects) ||
    snapshot.subjects.length > SCHEMA_REGISTRY_LIMITS.subjects
  ) {
    throw new HostContractValidationError(`${path}.subjects`, "exceeds the subject bound");
  }
  const error = optionalError(snapshot, path, parseError);
  return {
    ...baseSnapshot(snapshot, path),
    ...(error === undefined ? {} : { error }),
    omittedSubjects: nonNegativeInteger(snapshot.omittedSubjects, `${path}.omittedSubjects`),
    subjects: snapshot.subjects.map((subject, index) =>
      text(subject, `${path}.subjects[${String(index)}]`, SCHEMA_REGISTRY_LIMITS.subjectCharacters),
    ),
  };
}

function parseSchemaVersion(value: unknown, path: string): SchemaVersionDetail {
  const schema = record(value, path);
  exactKeys(schema, ["id", "references", "schema", "schemaType", "subject", "version"], path);
  return {
    ...schemaDefinition(schema, path),
    id: nonNegativeInteger(schema.id, `${path}.id`),
    subject: text(schema.subject, `${path}.subject`, SCHEMA_REGISTRY_LIMITS.subjectCharacters),
    version: positiveVersion(schema.version, `${path}.version`),
  };
}

export function parseSchemaDetailSnapshot(
  value: unknown,
  path: string,
  parseError: (value: unknown, path: string) => HostError,
): SchemaRegistryDetailSnapshot {
  const snapshot = record(value, path);
  exactKeys(
    snapshot,
    [
      "compatibilityLevel",
      "connectionName",
      "endpoint",
      "error",
      "refreshedAt",
      "schema",
      "state",
      "subject",
      "versions",
    ],
    path,
  );
  if (
    !Array.isArray(snapshot.versions) ||
    snapshot.versions.length > SCHEMA_REGISTRY_LIMITS.versions
  ) {
    throw new HostContractValidationError(`${path}.versions`, "exceeds the version bound");
  }
  const error = optionalError(snapshot, path, parseError);
  return {
    ...baseSnapshot(snapshot, path),
    ...(error === undefined ? {} : { error }),
    compatibilityLevel: nullableText(
      snapshot.compatibilityLevel,
      `${path}.compatibilityLevel`,
      128,
    ),
    schema: snapshot.schema === null ? null : parseSchemaVersion(snapshot.schema, `${path}.schema`),
    subject: nullableText(
      snapshot.subject,
      `${path}.subject`,
      SCHEMA_REGISTRY_LIMITS.subjectCharacters,
    ),
    versions: snapshot.versions.map((version, index) =>
      positiveVersion(version, `${path}.versions[${String(index)}]`),
    ),
  };
}

export function parseSchemaCompatibilitySnapshot(
  value: unknown,
  path: string,
): SchemaCompatibilitySnapshot {
  const snapshot = record(value, path);
  exactKeys(snapshot, ["compatible", "messages", "subject", "version"], path);
  if (
    !Array.isArray(snapshot.messages) ||
    snapshot.messages.length > SCHEMA_REGISTRY_LIMITS.compatibilityMessages
  ) {
    throw new HostContractValidationError(`${path}.messages`, "exceeds the message bound");
  }
  return {
    compatible: truth(snapshot.compatible, `${path}.compatible`),
    messages: snapshot.messages.map((message, index) =>
      text(message, `${path}.messages[${String(index)}]`, 2_048),
    ),
    subject: text(snapshot.subject, `${path}.subject`, SCHEMA_REGISTRY_LIMITS.subjectCharacters),
    version: versionSelector(snapshot.version, `${path}.version`),
  };
}
