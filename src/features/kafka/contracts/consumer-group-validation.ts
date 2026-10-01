import {
  KAFKA_CONSUMER_GROUP_BROKER_STATES,
  KAFKA_CONSUMER_GROUP_DETAIL_STATES,
  KAFKA_CONSUMER_GROUP_INVENTORY_STATES,
  KAFKA_CONSUMER_GROUP_LIMITS,
  type KafkaConsumerGroupAssignment,
  type KafkaConsumerGroupDetailSnapshot,
  type KafkaConsumerGroupDetails,
  type KafkaConsumerGroupInventorySnapshot,
  type KafkaConsumerGroupMember,
  type KafkaConsumerGroupOffset,
  type KafkaConsumerGroupSummary,
} from "./consumer-group-types";
import { HostContractValidationError } from "./validation-error";
import {
  boundedText,
  canonicalIsoTimestamp,
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  record,
  text,
} from "./validation-primitives";
import type { HostError } from "./types";

function array(value: unknown, path: string, maximum: number): readonly unknown[] {
  if (!Array.isArray(value) || value.length > maximum) {
    throw new HostContractValidationError(path, `must be an array of at most ${maximum}`);
  }
  return value;
}

function nullableText(value: unknown, path: string, maximum: number): string | null {
  return value === null ? null : text(value, path, maximum);
}

function canonicalOffset(value: unknown, path: string): string {
  const parsed = text(value, path, 128);
  if (!/^(?:0|[1-9][0-9]*)$/.test(parsed)) {
    throw new HostContractValidationError(path, "must be a canonical non-negative integer string");
  }
  return parsed;
}

function nullableOffset(value: unknown, path: string): string | null {
  return value === null ? null : canonicalOffset(value, path);
}

function parseSummary(value: unknown, path: string): KafkaConsumerGroupSummary {
  const summary = record(value, path);
  exactKeys(summary, ["groupType", "id", "protocolType", "state"], path);
  return {
    groupType: boundedText(
      summary.groupType,
      `${path}.groupType`,
      KAFKA_CONSUMER_GROUP_LIMITS.protocolCharacters,
    ),
    id: text(summary.id, `${path}.id`, KAFKA_CONSUMER_GROUP_LIMITS.groupIdCharacters),
    protocolType: boundedText(
      summary.protocolType,
      `${path}.protocolType`,
      KAFKA_CONSUMER_GROUP_LIMITS.protocolCharacters,
    ),
    state: declaredValue(summary.state, KAFKA_CONSUMER_GROUP_BROKER_STATES, `${path}.state`),
  };
}

function parseAssignment(value: unknown, path: string): KafkaConsumerGroupAssignment {
  const assignment = record(value, path);
  exactKeys(assignment, ["partitions", "topic"], path);
  const partitions = array(
    assignment.partitions,
    `${path}.partitions`,
    KAFKA_CONSUMER_GROUP_LIMITS.partitionsPerAssignment,
  ).map((partition, index) =>
    nonNegativeInteger(partition, `${path}.partitions[${String(index)}]`),
  );
  if (
    new Set(partitions).size !== partitions.length ||
    partitions.some((partition, index) => index > 0 && partitions[index - 1]! > partition)
  ) {
    throw new HostContractValidationError(
      `${path}.partitions`,
      "must contain unique sorted partition indexes",
    );
  }
  return {
    partitions,
    topic: text(assignment.topic, `${path}.topic`, KAFKA_CONSUMER_GROUP_LIMITS.topicCharacters),
  };
}

function parseMember(value: unknown, path: string): KafkaConsumerGroupMember {
  const member = record(value, path);
  exactKeys(member, ["assignments", "clientHost", "clientId", "groupInstanceId", "id"], path);
  const assignments = array(
    member.assignments,
    `${path}.assignments`,
    KAFKA_CONSUMER_GROUP_LIMITS.assignments,
  ).map((assignment, index) =>
    parseAssignment(assignment, `${path}.assignments[${String(index)}]`),
  );
  if (
    assignments.some(
      (assignment, index) =>
        index > 0 && assignments[index - 1]!.topic.localeCompare(assignment.topic, "en-US") >= 0,
    )
  ) {
    throw new HostContractValidationError(
      `${path}.assignments`,
      "must be uniquely sorted by topic",
    );
  }
  return {
    assignments,
    clientHost: boundedText(
      member.clientHost,
      `${path}.clientHost`,
      KAFKA_CONSUMER_GROUP_LIMITS.clientIdentityCharacters,
    ),
    clientId: boundedText(
      member.clientId,
      `${path}.clientId`,
      KAFKA_CONSUMER_GROUP_LIMITS.clientIdentityCharacters,
    ),
    groupInstanceId: nullableText(
      member.groupInstanceId,
      `${path}.groupInstanceId`,
      KAFKA_CONSUMER_GROUP_LIMITS.clientIdentityCharacters,
    ),
    id: text(member.id, `${path}.id`, KAFKA_CONSUMER_GROUP_LIMITS.clientIdentityCharacters),
  };
}

function parseOffset(value: unknown, path: string): KafkaConsumerGroupOffset {
  const offset = record(value, path);
  exactKeys(offset, ["committedOffset", "endOffset", "lag", "partition", "topic"], path);
  const committedOffset = nullableOffset(offset.committedOffset, `${path}.committedOffset`);
  const endOffset = nullableOffset(offset.endOffset, `${path}.endOffset`);
  const lag = nullableOffset(offset.lag, `${path}.lag`);
  if (committedOffset === null || endOffset === null) {
    if (lag !== null) {
      throw new HostContractValidationError(`${path}.lag`, "must be null without both offsets");
    }
  } else {
    const expected = ((): string => {
      const difference = BigInt(endOffset) - BigInt(committedOffset);
      return (difference > 0n ? difference : 0n).toString();
    })();
    if (lag !== expected) {
      throw new HostContractValidationError(
        `${path}.lag`,
        "must equal the non-negative difference between endOffset and committedOffset",
      );
    }
  }
  return {
    committedOffset,
    endOffset,
    lag,
    partition: nonNegativeInteger(offset.partition, `${path}.partition`),
    topic: text(offset.topic, `${path}.topic`, KAFKA_CONSUMER_GROUP_LIMITS.topicCharacters),
  };
}

function parseDetails(value: unknown, path: string): KafkaConsumerGroupDetails {
  const details = record(value, path);
  exactKeys(
    details,
    [
      "id",
      "members",
      "offsets",
      "omittedAssignments",
      "omittedMembers",
      "omittedOffsets",
      "protocol",
      "protocolType",
      "state",
    ],
    path,
  );
  const members = array(
    details.members,
    `${path}.members`,
    KAFKA_CONSUMER_GROUP_LIMITS.members,
  ).map((member, index) => parseMember(member, `${path}.members[${String(index)}]`));
  const offsets = array(
    details.offsets,
    `${path}.offsets`,
    KAFKA_CONSUMER_GROUP_LIMITS.offsets,
  ).map((offset, index) => parseOffset(offset, `${path}.offsets[${String(index)}]`));
  if (
    members.some(
      (member, index) => index > 0 && members[index - 1]!.id.localeCompare(member.id, "en-US") >= 0,
    ) ||
    offsets.some((offset, index) => {
      if (index === 0) return false;
      const previous = offsets[index - 1]!;
      const topicOrder = previous.topic.localeCompare(offset.topic, "en-US");
      return topicOrder > 0 || (topicOrder === 0 && previous.partition >= offset.partition);
    })
  ) {
    throw new HostContractValidationError(path, "must contain uniquely sorted members and offsets");
  }
  return {
    id: text(details.id, `${path}.id`, KAFKA_CONSUMER_GROUP_LIMITS.groupIdCharacters),
    members,
    offsets,
    omittedAssignments: nonNegativeInteger(
      details.omittedAssignments,
      `${path}.omittedAssignments`,
    ),
    omittedMembers: nonNegativeInteger(details.omittedMembers, `${path}.omittedMembers`),
    omittedOffsets: nonNegativeInteger(details.omittedOffsets, `${path}.omittedOffsets`),
    protocol: boundedText(
      details.protocol,
      `${path}.protocol`,
      KAFKA_CONSUMER_GROUP_LIMITS.protocolCharacters,
    ),
    protocolType: boundedText(
      details.protocolType,
      `${path}.protocolType`,
      KAFKA_CONSUMER_GROUP_LIMITS.protocolCharacters,
    ),
    state: declaredValue(details.state, KAFKA_CONSUMER_GROUP_BROKER_STATES, `${path}.state`),
  };
}

export function parseKafkaConsumerGroupIdentity(
  value: unknown,
  path: string,
): { readonly groupId: string } {
  const identity = record(value, path);
  exactKeys(identity, ["groupId"], path);
  return {
    groupId: text(
      identity.groupId,
      `${path}.groupId`,
      KAFKA_CONSUMER_GROUP_LIMITS.groupIdCharacters,
    ),
  };
}

export function parseKafkaConsumerGroupInventorySnapshot(
  value: unknown,
  path: string,
  parseError: (value: unknown, path: string) => HostError,
): KafkaConsumerGroupInventorySnapshot {
  const snapshot = record(value, path);
  exactKeys(
    snapshot,
    ["connectionName", "error", "groups", "omittedGroups", "refreshedAt", "state"],
    path,
  );
  const state = declaredValue(
    snapshot.state,
    KAFKA_CONSUMER_GROUP_INVENTORY_STATES,
    `${path}.state`,
  );
  const connectionName = nullableText(snapshot.connectionName, `${path}.connectionName`, 512);
  const groups = array(snapshot.groups, `${path}.groups`, KAFKA_CONSUMER_GROUP_LIMITS.groups).map(
    (group, index) => parseSummary(group, `${path}.groups[${String(index)}]`),
  );
  const omittedGroups = nonNegativeInteger(snapshot.omittedGroups, `${path}.omittedGroups`);
  const refreshedAt =
    snapshot.refreshedAt === null
      ? null
      : canonicalIsoTimestamp(snapshot.refreshedAt, `${path}.refreshedAt`);
  const error = Object.hasOwn(snapshot, "error")
    ? parseError(snapshot.error, `${path}.error`)
    : undefined;
  if (
    groups.some(
      (group, index) => index > 0 && groups[index - 1]!.id.localeCompare(group.id, "en-US") >= 0,
    )
  ) {
    throw new HostContractValidationError(`${path}.groups`, "must be uniquely sorted by ID");
  }
  if (state === "unavailable") {
    if (
      connectionName !== null ||
      groups.length > 0 ||
      omittedGroups !== 0 ||
      refreshedAt !== null ||
      error !== undefined
    ) {
      throw new HostContractValidationError(path, "contains inconsistent unavailable state");
    }
  } else if (connectionName === null) {
    throw new HostContractValidationError(path, "must identify the owning connection");
  }
  if (
    state === "loading" &&
    (groups.length > 0 || omittedGroups !== 0 || refreshedAt !== null || error !== undefined)
  ) {
    throw new HostContractValidationError(path, "contains inconsistent loading state");
  }
  if (state === "ready" && (groups.length === 0 || refreshedAt === null || error !== undefined)) {
    throw new HostContractValidationError(path, "contains inconsistent ready state");
  }
  if (
    state === "empty" &&
    (groups.length > 0 || omittedGroups !== 0 || refreshedAt === null || error !== undefined)
  ) {
    throw new HostContractValidationError(path, "contains inconsistent empty state");
  }
  if (
    (state === "denied" || state === "failed") &&
    (groups.length > 0 || omittedGroups !== 0 || refreshedAt !== null || error === undefined)
  ) {
    throw new HostContractValidationError(path, "contains inconsistent failure state");
  }
  if (state === "stale" && (refreshedAt === null || error === undefined)) {
    throw new HostContractValidationError(path, "contains inconsistent stale state");
  }
  return {
    connectionName,
    ...(error === undefined ? {} : { error }),
    groups,
    omittedGroups,
    refreshedAt,
    state,
  };
}

export function parseKafkaConsumerGroupDetailSnapshot(
  value: unknown,
  path: string,
  parseError: (value: unknown, path: string) => HostError,
): KafkaConsumerGroupDetailSnapshot {
  const snapshot = record(value, path);
  exactKeys(
    snapshot,
    ["connectionName", "error", "group", "groupId", "refreshedAt", "state"],
    path,
  );
  const state = declaredValue(snapshot.state, KAFKA_CONSUMER_GROUP_DETAIL_STATES, `${path}.state`);
  const connectionName = nullableText(snapshot.connectionName, `${path}.connectionName`, 512);
  const groupId = nullableText(
    snapshot.groupId,
    `${path}.groupId`,
    KAFKA_CONSUMER_GROUP_LIMITS.groupIdCharacters,
  );
  const group = snapshot.group === null ? null : parseDetails(snapshot.group, `${path}.group`);
  const refreshedAt =
    snapshot.refreshedAt === null
      ? null
      : canonicalIsoTimestamp(snapshot.refreshedAt, `${path}.refreshedAt`);
  const error = Object.hasOwn(snapshot, "error")
    ? parseError(snapshot.error, `${path}.error`)
    : undefined;
  if (state === "unavailable") {
    if (
      connectionName !== null ||
      groupId !== null ||
      group !== null ||
      refreshedAt !== null ||
      error !== undefined
    ) {
      throw new HostContractValidationError(path, "contains inconsistent unavailable state");
    }
  } else if (connectionName === null || groupId === null) {
    throw new HostContractValidationError(path, "must identify connection and group");
  }
  if (group !== null && groupId !== group.id) {
    throw new HostContractValidationError(`${path}.group`, "must match groupId");
  }
  if (state === "loading" && (group !== null || refreshedAt !== null || error !== undefined)) {
    throw new HostContractValidationError(path, "contains inconsistent loading state");
  }
  if (state === "ready" && (group === null || refreshedAt === null || error !== undefined)) {
    throw new HostContractValidationError(path, "contains inconsistent ready state");
  }
  if (
    (state === "denied" || state === "not-found" || state === "failed") &&
    (group !== null || refreshedAt !== null || error === undefined)
  ) {
    throw new HostContractValidationError(path, "contains inconsistent failure state");
  }
  if (state === "stale" && (group === null || refreshedAt === null || error === undefined)) {
    throw new HostContractValidationError(path, "contains inconsistent stale state");
  }
  return {
    connectionName,
    ...(error === undefined ? {} : { error }),
    group,
    groupId,
    refreshedAt,
    state,
  };
}
