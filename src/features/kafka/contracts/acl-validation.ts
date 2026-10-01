import {
  KAFKA_ACL_LIMITS,
  KAFKA_ACL_OPERATIONS,
  KAFKA_ACL_PATTERN_TYPES,
  KAFKA_ACL_PERMISSIONS,
  KAFKA_ACL_RESOURCE_TYPES,
  KAFKA_ACL_STATES,
  kafkaAclIdentity,
  type KafkaAclBinding,
  type KafkaAclDeletionInput,
  type KafkaAclSnapshot,
} from "./acl-types";
import type { HostError } from "./types";
import { HostContractValidationError } from "./validation-error";
import {
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  nullableText,
  record,
  text,
} from "./validation-primitives";

export function parseKafkaAclBinding(value: unknown, path: string): KafkaAclBinding {
  const acl = record(value, path);
  exactKeys(
    acl,
    ["host", "operation", "patternType", "permission", "principal", "resourceName", "resourceType"],
    path,
  );
  return {
    host: text(acl.host, `${path}.host`, KAFKA_ACL_LIMITS.fieldCharacters),
    operation: declaredValue(acl.operation, KAFKA_ACL_OPERATIONS, `${path}.operation`),
    patternType: declaredValue(acl.patternType, KAFKA_ACL_PATTERN_TYPES, `${path}.patternType`),
    permission: declaredValue(acl.permission, KAFKA_ACL_PERMISSIONS, `${path}.permission`),
    principal: text(acl.principal, `${path}.principal`, KAFKA_ACL_LIMITS.fieldCharacters),
    resourceName: text(acl.resourceName, `${path}.resourceName`, KAFKA_ACL_LIMITS.fieldCharacters),
    resourceType: declaredValue(acl.resourceType, KAFKA_ACL_RESOURCE_TYPES, `${path}.resourceType`),
  };
}

export function parseKafkaAclDeletionInput(value: unknown, path: string): KafkaAclDeletionInput {
  const input = record(value, path);
  exactKeys(input, ["acl", "confirmation"], path);
  const acl = parseKafkaAclBinding(input.acl, `${path}.acl`);
  const confirmation = text(input.confirmation, `${path}.confirmation`, 8_192);
  if (confirmation !== kafkaAclIdentity(acl)) {
    throw new HostContractValidationError(
      `${path}.confirmation`,
      "must exactly match the selected ACL",
    );
  }
  return { acl, confirmation };
}

export function parseKafkaAclSnapshot(
  value: unknown,
  path: string,
  parseError: (value: unknown, path: string) => HostError,
): KafkaAclSnapshot {
  const snapshot = record(value, path);
  exactKeys(
    snapshot,
    ["acls", "connectionName", "error", "omittedAcls", "refreshedAt", "state"],
    path,
  );
  if (!Array.isArray(snapshot.acls) || snapshot.acls.length > KAFKA_ACL_LIMITS.acls) {
    throw new HostContractValidationError(`${path}.acls`, "exceeds the ACL bound");
  }
  const acls = snapshot.acls.map((acl, index) =>
    parseKafkaAclBinding(acl, `${path}.acls[${String(index)}]`),
  );
  const state = declaredValue(snapshot.state, KAFKA_ACL_STATES, `${path}.state`);
  if ((state === "ready" && acls.length === 0) || (state === "empty" && acls.length !== 0)) {
    throw new HostContractValidationError(`${path}.state`, "does not match the ACL inventory");
  }
  const error = Object.hasOwn(snapshot, "error")
    ? parseError(snapshot.error, `${path}.error`)
    : undefined;
  return {
    acls,
    connectionName: nullableText(snapshot.connectionName, `${path}.connectionName`, 256),
    ...(error === undefined ? {} : { error }),
    omittedAcls: nonNegativeInteger(snapshot.omittedAcls, `${path}.omittedAcls`),
    refreshedAt: nullableText(snapshot.refreshedAt, `${path}.refreshedAt`, 128),
    state,
  };
}
