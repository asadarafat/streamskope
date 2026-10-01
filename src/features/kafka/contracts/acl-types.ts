import type { HostError } from "./types";

export const KAFKA_ACL_RESOURCE_TYPES = [
  "TOPIC",
  "GROUP",
  "CLUSTER",
  "TRANSACTIONAL_ID",
  "DELEGATION_TOKEN",
  "USER",
] as const;
export const KAFKA_ACL_PATTERN_TYPES = ["LITERAL", "PREFIXED"] as const;
export const KAFKA_ACL_OPERATIONS = [
  "ALL",
  "READ",
  "WRITE",
  "CREATE",
  "DELETE",
  "ALTER",
  "DESCRIBE",
  "CLUSTER_ACTION",
  "DESCRIBE_CONFIGS",
  "ALTER_CONFIGS",
  "IDEMPOTENT_WRITE",
  "CREATE_TOKENS",
  "DESCRIBE_TOKENS",
  "TWO_PHASE_COMMIT",
] as const;
export const KAFKA_ACL_PERMISSIONS = ["ALLOW", "DENY"] as const;
export const KAFKA_ACL_STATES = [
  "unavailable",
  "loading",
  "ready",
  "empty",
  "stale",
  "denied",
  "unsupported",
  "failed",
] as const;
export const KAFKA_ACL_LIMITS = { acls: 10_000, fieldCharacters: 1_024 } as const;

export type KafkaAclResourceType = (typeof KAFKA_ACL_RESOURCE_TYPES)[number];
export type KafkaAclPatternType = (typeof KAFKA_ACL_PATTERN_TYPES)[number];
export type KafkaAclOperation = (typeof KAFKA_ACL_OPERATIONS)[number];
export type KafkaAclPermission = (typeof KAFKA_ACL_PERMISSIONS)[number];
export type KafkaAclState = (typeof KAFKA_ACL_STATES)[number];

export interface KafkaAclBinding {
  readonly host: string;
  readonly operation: KafkaAclOperation;
  readonly patternType: KafkaAclPatternType;
  readonly permission: KafkaAclPermission;
  readonly principal: string;
  readonly resourceName: string;
  readonly resourceType: KafkaAclResourceType;
}

export interface KafkaAclDeletionInput {
  readonly acl: KafkaAclBinding;
  readonly confirmation: string;
}

export interface KafkaAclSnapshot {
  readonly acls: readonly KafkaAclBinding[];
  readonly connectionName: string | null;
  readonly error?: HostError;
  readonly omittedAcls: number;
  readonly refreshedAt: string | null;
  readonly state: KafkaAclState;
}

export function kafkaAclIdentity(acl: KafkaAclBinding): string {
  return [
    acl.resourceType,
    acl.patternType,
    acl.resourceName,
    acl.principal,
    acl.host,
    acl.operation,
    acl.permission,
  ].join(" | ");
}
