import {
  AclOperations,
  AclPermissionTypes,
  Admin,
  ConfigResourceTypes,
  ConfigSources,
  ConfigTypes,
  IncrementalAlterConfigOperationTypes,
  ListOffsetTimestamps,
  ResourcePatternTypes,
  ResourceTypes,
  type Acl,
  type AclFilter,
  type ClusterMetadata,
  type ConfigDescription,
  type ConfigSourceValue,
  type ConfigTypeValue,
  type DescribeConfigsOptions,
  type IncrementalAlterConfigsOptions,
  type Group,
  type GroupBase,
  type ListConsumerGroupOffsetsGroup,
  type ListedOffsetsTopic,
} from "@platformatic/kafka";

import type {
  KafkaConfigurationEntry,
  KafkaAclBinding,
  KafkaConfigurationSource,
  KafkaConfigurationType,
  KafkaConsumerGroupBrokerState,
  KafkaConsumerGroupDetails,
  KafkaConsumerGroupMember,
  KafkaConsumerGroupOffset,
  KafkaConsumerGroupSummary,
  KafkaTopicConfigurationChange,
  KafkaTopicConfigurationEntry,
} from "../contracts";
import { KAFKA_CONSUMER_GROUP_LIMITS, kafkaAclIdentity } from "../contracts";
import type { KafkaClusterMetadata, KafkaConsumerGroupInventory } from "../application";

import { platformaticClientOptions } from "./platformatic-options";
import type { KafkaAdminFactory, KafkaAdminInput, KafkaAdminPort } from "./types";

export interface PlatformaticAdminClient {
  close(): Promise<void>;
  createAcls?(options: { readonly creations: Acl[] }): Promise<void>;
  deleteAcls?(options: { readonly filters: AclFilter[] }): Promise<readonly Acl[]>;
  describeAcls?(options: { readonly filter: AclFilter }): Promise<
    Array<{
      readonly acls: Array<Pick<Acl, "host" | "operation" | "permissionType" | "principal">>;
      readonly resourceName: string;
      readonly resourcePatternType: Acl["resourcePatternType"];
      readonly resourceType: Acl["resourceType"];
    }>
  >;
  describeConfigs(options: DescribeConfigsOptions): Promise<ConfigDescription[]>;
  describeGroups(options: {
    readonly groups: string[];
    readonly includeAuthorizedOperations: false;
  }): Promise<Map<string, Group>>;
  incrementalAlterConfigs(options: IncrementalAlterConfigsOptions): Promise<void>;
  listConsumerGroupOffsets(options: {
    readonly groups: string[];
    readonly requireStable: false;
  }): Promise<ListConsumerGroupOffsetsGroup[]>;
  listGroups(): Promise<Map<string, GroupBase>>;
  listOffsets(options: {
    readonly topics: Array<{
      readonly name: string;
      readonly partitions: Array<{
        readonly partitionIndex: number;
        readonly timestamp: typeof ListOffsetTimestamps.LATEST;
      }>;
    }>;
  }): Promise<ListedOffsetsTopic[]>;
  listTopics(): Promise<readonly string[]>;
  metadata(options: { readonly forceUpdate: true }): Promise<ClusterMetadata>;
}

function kafkaAclValue(acl: KafkaAclBinding): Acl {
  return {
    host: acl.host,
    operation: AclOperations[acl.operation],
    permissionType: AclPermissionTypes[acl.permission],
    principal: acl.principal,
    resourceName: acl.resourceName,
    resourcePatternType: ResourcePatternTypes[acl.patternType],
    resourceType: ResourceTypes[acl.resourceType],
  };
}

function aclResourceType(value: Acl["resourceType"]): KafkaAclBinding["resourceType"] {
  switch (value) {
    case ResourceTypes.TOPIC:
      return "TOPIC";
    case ResourceTypes.GROUP:
      return "GROUP";
    case ResourceTypes.CLUSTER:
      return "CLUSTER";
    case ResourceTypes.TRANSACTIONAL_ID:
      return "TRANSACTIONAL_ID";
    case ResourceTypes.DELEGATION_TOKEN:
      return "DELEGATION_TOKEN";
    case ResourceTypes.USER:
      return "USER";
    default:
      throw new Error(`Kafka returned unsupported ACL resource type ${String(value)}.`);
  }
}

function aclPatternType(value: Acl["resourcePatternType"]): KafkaAclBinding["patternType"] {
  switch (value) {
    case ResourcePatternTypes.LITERAL:
      return "LITERAL";
    case ResourcePatternTypes.PREFIXED:
      return "PREFIXED";
    default:
      throw new Error(`Kafka returned unsupported ACL pattern type ${String(value)}.`);
  }
}

function aclOperation(value: Acl["operation"]): KafkaAclBinding["operation"] {
  for (const operation of [
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
  ] as const) {
    if (AclOperations[operation] === value) return operation;
  }
  throw new Error(`Kafka returned unsupported ACL operation ${String(value)}.`);
}

function aclPermission(value: Acl["permissionType"]): KafkaAclBinding["permission"] {
  if (value === AclPermissionTypes.ALLOW) return "ALLOW";
  if (value === AclPermissionTypes.DENY) return "DENY";
  throw new Error(`Kafka returned unsupported ACL permission ${String(value)}.`);
}

function kafkaAclBinding(
  target: Omit<Acl, "host" | "operation" | "permissionType" | "principal">,
  permission: Pick<Acl, "host" | "operation" | "permissionType" | "principal">,
): KafkaAclBinding {
  return {
    host: permission.host,
    operation: aclOperation(permission.operation),
    patternType: aclPatternType(target.resourcePatternType),
    permission: aclPermission(permission.permissionType),
    principal: permission.principal,
    resourceName: target.resourceName,
    resourceType: aclResourceType(target.resourceType),
  };
}

function consumerGroupState(value: GroupBase["state"]): KafkaConsumerGroupBrokerState {
  switch (value) {
    case "PREPARING_REBALANCE":
      return "preparing-rebalance";
    case "COMPLETING_REBALANCE":
      return "completing-rebalance";
    case "STABLE":
      return "stable";
    case "DEAD":
      return "dead";
    case "EMPTY":
      return "empty";
  }
}

function consumerGroupSummary(group: GroupBase): KafkaConsumerGroupSummary {
  return {
    groupType: group.groupType,
    id: group.id,
    protocolType: group.protocolType,
    state: consumerGroupState(group.state),
  };
}

function consumerGroupMembers(group: Group): {
  readonly members: readonly KafkaConsumerGroupMember[];
  readonly omittedAssignments: number;
  readonly omittedMembers: number;
} {
  const allMembers = [...group.members.values()].sort((left, right) =>
    left.id.localeCompare(right.id, "en-US"),
  );
  const retainedMembers = allMembers.slice(0, KAFKA_CONSUMER_GROUP_LIMITS.members);
  let remainingAssignments = KAFKA_CONSUMER_GROUP_LIMITS.assignments;
  let omittedAssignments = 0;
  const members = retainedMembers.map((member) => {
    const assignments = [...(member.assignments?.values() ?? [])]
      .map((assignment) => ({
        partitions: [...assignment.partitions].sort((left, right) => left - right),
        topic: assignment.topic,
      }))
      .sort((left, right) => left.topic.localeCompare(right.topic, "en-US"));
    const retainedAssignments = assignments.slice(0, remainingAssignments);
    remainingAssignments -= retainedAssignments.length;
    omittedAssignments += assignments.length - retainedAssignments.length;
    return {
      assignments: retainedAssignments,
      clientHost: member.clientHost,
      clientId: member.clientId,
      groupInstanceId: member.groupInstanceId ?? null,
      id: member.id,
    };
  });
  omittedAssignments += allMembers
    .slice(KAFKA_CONSUMER_GROUP_LIMITS.members)
    .reduce((count, member) => count + (member.assignments?.size ?? 0), 0);
  return {
    members,
    omittedAssignments,
    omittedMembers: allMembers.length - retainedMembers.length,
  };
}

function consumerGroupOffsets(groups: readonly ListConsumerGroupOffsetsGroup[]): {
  readonly offsets: Array<{
    readonly committedOffset: bigint;
    readonly partition: number;
    readonly topic: string;
  }>;
  readonly omittedOffsets: number;
} {
  const offsets = groups
    .flatMap((group) =>
      group.topics.flatMap((topic) =>
        topic.partitions.map((partition) => ({
          committedOffset: partition.committedOffset,
          partition: partition.partitionIndex,
          topic: topic.name,
        })),
      ),
    )
    .sort((left, right) => {
      const topic = left.topic.localeCompare(right.topic, "en-US");
      return topic === 0 ? left.partition - right.partition : topic;
    });
  return {
    offsets: offsets.slice(0, KAFKA_CONSUMER_GROUP_LIMITS.offsets),
    omittedOffsets: Math.max(0, offsets.length - KAFKA_CONSUMER_GROUP_LIMITS.offsets),
  };
}

function latestOffsetMap(topics: readonly ListedOffsetsTopic[]): ReadonlyMap<string, bigint> {
  return new Map(
    topics.flatMap((topic) =>
      topic.partitions.map(
        (partition) =>
          [`${topic.name}\u0000${String(partition.partitionIndex)}`, partition.offset] as const,
      ),
    ),
  );
}

function configurationSource(value: ConfigSourceValue): KafkaConfigurationSource {
  switch (value) {
    case ConfigSources.TOPIC_CONFIG:
      return "topic";
    case ConfigSources.DYNAMIC_BROKER_CONFIG:
      return "dynamic-broker";
    case ConfigSources.DYNAMIC_DEFAULT_BROKER_CONFIG:
      return "dynamic-default-broker";
    case ConfigSources.STATIC_BROKER_CONFIG:
      return "static-broker";
    case ConfigSources.DEFAULT_CONFIG:
      return "default";
    case ConfigSources.DYNAMIC_BROKER_LOGGER_CONFIG:
      return "dynamic-broker-logger";
    case ConfigSources.CLIENT_METRICS_CONFIG:
      return "client-metrics";
    case ConfigSources.GROUP_CONFIG:
      return "group";
    case ConfigSources.UNKNOWN:
      return "unknown";
  }
}

function configurationType(value: ConfigTypeValue): KafkaConfigurationType {
  switch (value) {
    case ConfigTypes.BOOLEAN:
      return "boolean";
    case ConfigTypes.STRING:
      return "string";
    case ConfigTypes.INT:
      return "int";
    case ConfigTypes.SHORT:
      return "short";
    case ConfigTypes.LONG:
      return "long";
    case ConfigTypes.DOUBLE:
      return "double";
    case ConfigTypes.LIST:
      return "list";
    case ConfigTypes.CLASS:
      return "class";
    case ConfigTypes.PASSWORD:
      return "password";
    case ConfigTypes.UNKNOWN:
      return "unknown";
  }
}

function configurationEntries(
  descriptions: readonly ConfigDescription[],
  resourceType: number,
  resourceName: string,
): readonly KafkaConfigurationEntry[] {
  const description = descriptions.find(
    (candidate) =>
      candidate.resourceType === resourceType && candidate.resourceName === resourceName,
  );
  if (description === undefined) {
    throw new Error(
      `Kafka did not return configuration metadata for resource ${resourceType}:${resourceName}.`,
    );
  }
  return description.configs.map((config) => ({
    documentation: config.documentation ?? null,
    isDefault: config.configSource === ConfigSources.DEFAULT_CONFIG,
    isSensitive: config.isSensitive,
    name: config.name,
    readOnly: config.readOnly,
    source: configurationSource(config.configSource),
    synonyms: config.synonyms.map((synonym) => ({
      name: synonym.name,
      source: configurationSource(synonym.source),
      value: config.isSensitive ? null : (synonym.value ?? null),
    })),
    type: configurationType(config.configType),
    value: config.isSensitive ? null : (config.value ?? null),
  }));
}

export class PlatformaticAdminPort implements KafkaAdminPort {
  constructor(private readonly admin: PlatformaticAdminClient) {}

  alterTopicConfiguration(
    topic: string,
    changes: readonly KafkaTopicConfigurationChange[],
    validateOnly: boolean,
  ): Promise<void> {
    return this.admin.incrementalAlterConfigs({
      resources: [
        {
          configs: changes.map((change) => ({
            configOperation: IncrementalAlterConfigOperationTypes.SET,
            name: change.name,
            value: change.value,
          })),
          resourceName: topic,
          resourceType: ConfigResourceTypes.TOPIC,
        },
      ],
      validateOnly,
    });
  }

  close(): Promise<void> {
    return this.admin.close();
  }

  createAcl(acl: KafkaAclBinding): Promise<void> {
    if (this.admin.createAcls === undefined)
      return Promise.reject(new Error("Kafka ACL creation is unavailable."));
    return this.admin.createAcls({ creations: [kafkaAclValue(acl)] });
  }

  async deleteAcl(acl: KafkaAclBinding): Promise<void> {
    if (this.admin.deleteAcls === undefined) throw new Error("Kafka ACL deletion is unavailable.");
    const deleted = await this.admin.deleteAcls({ filters: [kafkaAclValue(acl)] });
    const confirmed = deleted.map((candidate) => kafkaAclBinding(candidate, candidate));
    if (confirmed.length !== 1 || kafkaAclIdentity(confirmed[0]!) !== kafkaAclIdentity(acl)) {
      throw new Error(
        "Kafka returned an inconsistent ACL deletion result for the exact requested binding.",
      );
    }
  }

  async describeBrokerConfiguration(brokerId: number): Promise<readonly KafkaConfigurationEntry[]> {
    if (!Number.isSafeInteger(brokerId) || brokerId < 0) {
      throw new RangeError("Kafka broker ID must be a non-negative safe integer.");
    }
    const resourceName = String(brokerId);
    const descriptions = await this.admin.describeConfigs({
      includeDocumentation: true,
      includeSynonyms: true,
      resources: [
        {
          resourceName,
          resourceType: ConfigResourceTypes.BROKER,
        },
      ],
    });
    return configurationEntries(descriptions, ConfigResourceTypes.BROKER, resourceName);
  }

  async describeClusterMetadata(): Promise<KafkaClusterMetadata> {
    const metadata = await this.admin.metadata({ forceUpdate: true });
    return {
      brokers: [...metadata.brokers.entries()]
        .map(([nodeId, broker]) => ({
          host: broker.host,
          nodeId,
          port: broker.port,
          rack: broker.rack ?? null,
        }))
        .sort((left, right) => left.nodeId - right.nodeId),
      clusterId: metadata.id.length === 0 ? null : metadata.id,
      controllerId:
        Number.isSafeInteger(metadata.controllerId) && metadata.controllerId >= 0
          ? metadata.controllerId
          : null,
    };
  }

  async describeConsumerGroup(groupId: string): Promise<KafkaConsumerGroupDetails> {
    const [descriptions, committedGroups] = await Promise.all([
      this.admin.describeGroups({ groups: [groupId], includeAuthorizedOperations: false }),
      this.admin.listConsumerGroupOffsets({ groups: [groupId], requireStable: false }),
    ]);
    const group = descriptions.get(groupId);
    if (group === undefined) {
      throw new Error(`Kafka did not return consumer group ${groupId}.`);
    }
    const memberEvidence = consumerGroupMembers(group);
    const committedEvidence = consumerGroupOffsets(
      committedGroups.filter((candidate) => candidate.groupId === groupId),
    );
    const topics = new Map<string, number[]>();
    for (const offset of committedEvidence.offsets) {
      const partitions = topics.get(offset.topic) ?? [];
      partitions.push(offset.partition);
      topics.set(offset.topic, partitions);
    }
    const latest =
      topics.size === 0
        ? []
        : await this.admin.listOffsets({
            topics: [...topics.entries()]
              .sort(([left], [right]) => left.localeCompare(right, "en-US"))
              .map(([name, partitions]) => ({
                name,
                partitions: partitions
                  .sort((left, right) => left - right)
                  .map((partitionIndex) => ({
                    partitionIndex,
                    timestamp: ListOffsetTimestamps.LATEST,
                  })),
              })),
          });
    const latestOffsets = latestOffsetMap(latest);
    const offsets: readonly KafkaConsumerGroupOffset[] = committedEvidence.offsets.map((offset) => {
      const latestOffset = latestOffsets.get(`${offset.topic}\u0000${String(offset.partition)}`);
      const committedOffset = offset.committedOffset < 0n ? null : offset.committedOffset;
      const endOffset = latestOffset === undefined || latestOffset < 0n ? null : latestOffset;
      const lag =
        committedOffset === null || endOffset === null
          ? null
          : endOffset > committedOffset
            ? endOffset - committedOffset
            : 0n;
      return {
        committedOffset: committedOffset?.toString() ?? null,
        endOffset: endOffset?.toString() ?? null,
        lag: lag?.toString() ?? null,
        partition: offset.partition,
        topic: offset.topic,
      };
    });
    return {
      id: group.id,
      members: memberEvidence.members,
      offsets,
      omittedAssignments: memberEvidence.omittedAssignments,
      omittedMembers: memberEvidence.omittedMembers,
      omittedOffsets: committedEvidence.omittedOffsets,
      protocol: group.protocol,
      protocolType: group.protocolType,
      state: consumerGroupState(group.state),
    };
  }

  async describeTopicConfiguration(
    topic: string,
  ): Promise<readonly KafkaTopicConfigurationEntry[]> {
    const descriptions = await this.admin.describeConfigs({
      includeDocumentation: true,
      includeSynonyms: true,
      resources: [
        {
          resourceName: topic,
          resourceType: ConfigResourceTypes.TOPIC,
        },
      ],
    });
    return configurationEntries(descriptions, ConfigResourceTypes.TOPIC, topic);
  }

  listTopics(): Promise<readonly string[]> {
    return this.admin.listTopics();
  }

  async listAcls(): Promise<readonly KafkaAclBinding[]> {
    if (this.admin.describeAcls === undefined)
      throw new Error("Kafka ACL inventory is unavailable.");
    const resources = await this.admin.describeAcls({
      filter: {
        host: null,
        operation: AclOperations.ANY,
        permissionType: AclPermissionTypes.ANY,
        principal: null,
        resourceName: null,
        resourcePatternType: ResourcePatternTypes.ANY,
        resourceType: ResourceTypes.ANY,
      },
    });
    return resources
      .flatMap((resource) =>
        resource.acls.map((permission) => kafkaAclBinding(resource, permission)),
      )
      .sort((left, right) =>
        kafkaAclIdentity(left).localeCompare(kafkaAclIdentity(right), "en-US"),
      );
  }

  async listConsumerGroups(): Promise<KafkaConsumerGroupInventory> {
    const groups = [...(await this.admin.listGroups()).values()]
      .map(consumerGroupSummary)
      .sort((left, right) => left.id.localeCompare(right.id, "en-US"));
    return {
      groups: groups.slice(0, KAFKA_CONSUMER_GROUP_LIMITS.groups),
      omittedGroups: Math.max(0, groups.length - KAFKA_CONSUMER_GROUP_LIMITS.groups),
    };
  }
}

export class PlatformaticAdminFactory implements KafkaAdminFactory {
  create(input: KafkaAdminInput): KafkaAdminPort {
    return new PlatformaticAdminPort(
      new Admin(platformaticClientOptions(input, "streamskope-connection")),
    );
  }
}
