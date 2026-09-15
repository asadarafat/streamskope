import type { HostError } from "./types";

export const SCHEMA_REGISTRY_TYPES = ["AVRO", "JSON", "PROTOBUF"] as const;
export const SCHEMA_REGISTRY_STATES = [
  "unavailable",
  "not-configured",
  "loading",
  "ready",
  "empty",
  "stale",
  "denied",
  "invalid-response",
  "failed",
] as const;
export const SCHEMA_REGISTRY_LIMITS = {
  compatibilityMessages: 64,
  references: 128,
  schemaCharacters: 2 * 1_048_576,
  subjectCharacters: 512,
  subjects: 5_000,
  versions: 10_000,
} as const;

export type SchemaRegistryType = (typeof SCHEMA_REGISTRY_TYPES)[number];
export type SchemaRegistryState = (typeof SCHEMA_REGISTRY_STATES)[number];

export interface SchemaReference {
  readonly name: string;
  readonly subject: string;
  readonly version: number;
}

export interface SchemaDefinitionInput {
  readonly references: readonly SchemaReference[];
  readonly schema: string;
  readonly schemaType: SchemaRegistryType;
}

export interface SchemaSubjectVersionIdentity {
  readonly subject: string;
  readonly version: SchemaVersionSelector;
}

export type SchemaVersionSelector = number | "latest";

export interface SchemaCompatibilityCheckInput extends SchemaDefinitionInput {
  readonly subject: string;
  readonly version: SchemaVersionSelector;
}

export interface SchemaRegistrationInput extends SchemaDefinitionInput {
  readonly normalize: boolean;
  readonly subject: string;
  readonly version: SchemaVersionSelector;
}

export type SchemaDeletionTarget =
  | { readonly kind: "subject"; readonly subject: string }
  | { readonly kind: "version"; readonly subject: string; readonly version: number };

export interface SchemaDeletionInput {
  readonly confirmation: string;
  readonly mode: "permanent" | "soft";
  readonly target: SchemaDeletionTarget;
}

export interface SchemaRegistryInventorySnapshot {
  readonly connectionName: string | null;
  readonly endpoint: string | null;
  readonly error?: HostError;
  readonly omittedSubjects: number;
  readonly refreshedAt: string | null;
  readonly state: SchemaRegistryState;
  readonly subjects: readonly string[];
}

export interface SchemaVersionDetail extends SchemaDefinitionInput {
  readonly id: number;
  readonly subject: string;
  readonly version: number;
}

export interface SchemaRegistryDetailSnapshot {
  readonly compatibilityLevel: string | null;
  readonly connectionName: string | null;
  readonly endpoint: string | null;
  readonly error?: HostError;
  readonly refreshedAt: string | null;
  readonly schema: SchemaVersionDetail | null;
  readonly state: SchemaRegistryState;
  readonly subject: string | null;
  readonly versions: readonly number[];
}

export interface SchemaCompatibilitySnapshot {
  readonly compatible: boolean;
  readonly messages: readonly string[];
  readonly subject: string;
  readonly version: SchemaVersionSelector;
}
