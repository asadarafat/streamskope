import type { HostError } from "./types";

export const KAFKA_CONSUMER_GROUP_BROKER_STATES = [
  "preparing-rebalance",
  "completing-rebalance",
  "stable",
  "dead",
  "empty",
] as const;

export const KAFKA_CONSUMER_GROUP_INVENTORY_STATES = [
  "unavailable",
  "loading",
  "ready",
  "empty",
  "denied",
  "failed",
  "stale",
] as const;

export const KAFKA_CONSUMER_GROUP_DETAIL_STATES = [
  "unavailable",
  "loading",
  "ready",
  "denied",
  "not-found",
  "failed",
  "stale",
] as const;

export const KAFKA_CONSUMER_GROUP_LIMITS = {
  assignments: 5_000,
  clientIdentityCharacters: 4_096,
  groupIdCharacters: 512,
  groups: 500,
  members: 1_000,
  offsets: 10_000,
  partitionsPerAssignment: 10_000,
  protocolCharacters: 512,
  topicCharacters: 512,
} as const;

export type KafkaConsumerGroupBrokerState = (typeof KAFKA_CONSUMER_GROUP_BROKER_STATES)[number];
export type KafkaConsumerGroupInventoryState =
  (typeof KAFKA_CONSUMER_GROUP_INVENTORY_STATES)[number];
export type KafkaConsumerGroupDetailState = (typeof KAFKA_CONSUMER_GROUP_DETAIL_STATES)[number];

export interface KafkaConsumerGroupSummary {
  readonly groupType: string;
  readonly id: string;
  readonly protocolType: string;
  readonly state: KafkaConsumerGroupBrokerState;
}

export interface KafkaConsumerGroupAssignment {
  readonly partitions: readonly number[];
  readonly topic: string;
}

export interface KafkaConsumerGroupMember {
  readonly assignments: readonly KafkaConsumerGroupAssignment[];
  readonly clientHost: string;
  readonly clientId: string;
  readonly groupInstanceId: string | null;
  readonly id: string;
}

export interface KafkaConsumerGroupOffset {
  readonly committedOffset: string | null;
  readonly endOffset: string | null;
  readonly lag: string | null;
  readonly partition: number;
  readonly topic: string;
}

export interface KafkaConsumerGroupDetails {
  readonly id: string;
  readonly members: readonly KafkaConsumerGroupMember[];
  readonly offsets: readonly KafkaConsumerGroupOffset[];
  readonly omittedAssignments: number;
  readonly omittedMembers: number;
  readonly omittedOffsets: number;
  readonly protocol: string;
  readonly protocolType: string;
  readonly state: KafkaConsumerGroupBrokerState;
}

export interface KafkaConsumerGroupInventorySnapshot {
  readonly connectionName: string | null;
  readonly error?: HostError;
  readonly groups: readonly KafkaConsumerGroupSummary[];
  readonly omittedGroups: number;
  readonly refreshedAt: string | null;
  readonly state: KafkaConsumerGroupInventoryState;
}

export interface KafkaConsumerGroupDetailSnapshot {
  readonly connectionName: string | null;
  readonly error?: HostError;
  readonly group: KafkaConsumerGroupDetails | null;
  readonly groupId: string | null;
  readonly refreshedAt: string | null;
  readonly state: KafkaConsumerGroupDetailState;
}
