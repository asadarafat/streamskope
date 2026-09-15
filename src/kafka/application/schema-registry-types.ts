import type {
  SchemaCompatibilityCheckInput,
  SchemaDeletionInput,
  SchemaRegistrationInput,
  SchemaSubjectVersionIdentity,
  SchemaVersionDetail,
} from "../contracts";

import type { KafkaClusterServiceContext } from "./types";

export interface SchemaRegistrySubjectInventory {
  readonly omittedSubjects: number;
  readonly subjects: readonly string[];
}

export interface SchemaRegistrySubjectDetail {
  readonly compatibilityLevel: string | null;
  readonly schema: SchemaVersionDetail;
  readonly versions: readonly number[];
}

export interface SchemaRegistryCompatibilityResult {
  readonly compatible: boolean;
  readonly messages: readonly string[];
}

export interface SchemaRegistryPort {
  checkCompatibility(
    context: KafkaClusterServiceContext,
    input: SchemaCompatibilityCheckInput,
    signal: AbortSignal,
  ): Promise<SchemaRegistryCompatibilityResult>;
  delete(
    context: KafkaClusterServiceContext,
    input: SchemaDeletionInput,
    signal: AbortSignal,
  ): Promise<readonly number[]>;
  listSubjects(
    context: KafkaClusterServiceContext,
    signal: AbortSignal,
  ): Promise<SchemaRegistrySubjectInventory>;
  loadSubject(
    context: KafkaClusterServiceContext,
    identity: SchemaSubjectVersionIdentity,
    signal: AbortSignal,
  ): Promise<SchemaRegistrySubjectDetail>;
  loadLatestSubject(
    context: KafkaClusterServiceContext,
    subject: string,
    signal: AbortSignal,
  ): Promise<SchemaRegistrySubjectDetail>;
  register(
    context: KafkaClusterServiceContext,
    input: SchemaRegistrationInput,
    signal: AbortSignal,
  ): Promise<{ readonly id: number }>;
}
