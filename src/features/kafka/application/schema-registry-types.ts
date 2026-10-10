import type {
  SchemaCompatibilityCheckInput,
  SchemaDeletionInput,
  SchemaRegistrationInput,
  SchemaSubjectVersionIdentity,
  SchemaVersionDetail,
} from "../contracts";
import type {
  SchemaCompatibilityPolicy,
  SchemaCompatibilityLevel,
} from "../contracts/schema-changes";
import type { SchemaPolicyChange } from "../contracts/schema-policy";

import type { KafkaClusterServiceContext } from "./types";

/** Additional capabilities required for reviewed registration; legacy browsing remains independent. */
export interface SchemaRegistryReviewPort {
  loadReviewSchema(
    context: KafkaClusterServiceContext,
    identity: SchemaSubjectVersionIdentity,
    signal: AbortSignal,
  ): Promise<SchemaVersionDetail | null>;
  loadCompatibilityPolicy(
    context: KafkaClusterServiceContext,
    subject: string,
    signal: AbortSignal,
  ): Promise<SchemaCompatibilityPolicy>;
  checkProposedCompatibility(
    context: KafkaClusterServiceContext,
    input: SchemaRegistrationInput,
    versions: readonly number[],
    signal: AbortSignal,
  ): Promise<SchemaRegistryCompatibilityResult>;
}

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

export interface SchemaRegistryPolicyPort {
  changeSubjectCompatibility(
    context: KafkaClusterServiceContext,
    subject: string,
    change: SchemaPolicyChange,
    signal: AbortSignal,
  ): Promise<{ readonly level: SchemaCompatibilityLevel }>;
}
export type ReviewedSchemaPolicyPort = Pick<
  SchemaRegistryReviewPort,
  "loadReviewSchema" | "loadCompatibilityPolicy"
> &
  SchemaRegistryPolicyPort;
