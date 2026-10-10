import type {
  JsonValue,
  PluginAcquisitionProgress,
  PluginEvent,
  PluginSnapshot,
  PluginPackageInspectInput,
  PluginNetworkUpdateInput,
} from "../../../plugins/contracts";
import type { ProviderHostPort } from "../../../platform/providers/host";

import { HOST_COMMANDS } from "./host-command-vocabulary";
import type { HostError } from "./host-errors";
import type { KafkaSavedQuery } from "./query-library";
import type { KafkaExploredMessage } from "./message-types";
export type { KafkaMessage, KafkaExploredMessage } from "./message-types";
import type { KafkaReadCoverage, KafkaSearchFilter, KafkaSearchProgress } from "./query-search";
import type { HostCommandResultMap, HostAcknowledgementCommandName } from "./host-command-results";
import type {
  ProfileCreateInput,
  ProfileStoreCapability,
  ProfileSummary,
  ProfileTestInput,
  ProfileUpdateInput,
} from "./profile-types";
import type {
  ConnectionClientIdentity,
  ConnectionSasl,
  ResolvedClusterServiceEndpoints,
} from "./connection-security";
import type {
  TrustAcquisitionRecipeInput,
  TrustAcquisitionRecipeSnapshot,
  LegacyTrustRecipeSelection,
} from "./trust-recipe-types";
import type { KafkaLiveRuleCapability, KafkaRuleNotification } from "./live-rule-types";
import type {
  KafkaRuleDefinition,
  KafkaRuleEvaluationInput,
  KafkaRuleEvaluationReport,
  KafkaRuleSnapshot,
} from "./rule-types";
import type {
  KafkaTopicConfigurationHistorySnapshot,
  KafkaTopicConfigurationOperationInput,
  KafkaTopicConfigurationSnapshot,
} from "./topic-configuration-types";
import type { KafkaClusterDiagnosticsSnapshot } from "./cluster-diagnostics-types";
import type {
  KafkaLatencyHistorySnapshot,
  KafkaLatencyProbeRequest,
  KafkaLatencySnapshot,
} from "./latency-types";
import type { KafkaStreamMonitorSnapshot } from "./stream-monitor-types";
import type {
  AcquiredTlsConnectionInput,
  RemoteTrustHostKeyDiscoveryInput,
  RemoteTrustAcquisitionIdentity,
  RemoteTrustMaterialFetchInput,
} from "./remote-trust-types";
import type {
  KafkaOperationalPreferenceSnapshot,
  KafkaOperationalPreferenceUpdateInput,
} from "./operational-preference-types";
import type { ExternalUrlOpenResult } from "./external-url-types";
import type {
  KafkaConsumerGroupDetailSnapshot,
  KafkaConsumerGroupInventorySnapshot,
} from "./consumer-group-types";
import type {
  SchemaCompatibilityCheckInput,
  SchemaCompatibilitySnapshot,
  SchemaDeletionInput,
  SchemaRegistrationInput,
  SchemaRegistryDetailSnapshot,
  SchemaRegistryInventorySnapshot,
} from "./schema-registry-types";
import type { KafkaAclBinding, KafkaAclDeletionInput, KafkaAclSnapshot } from "./acl-types";
import type {
  RedpandaTransformDeletionInput,
  RedpandaTransformDetailSnapshot,
  RedpandaTransformInventorySnapshot,
  RedpandaTransformLogsSnapshot,
} from "./transform-types";

export { HOST_ERROR_CODES, HOST_ERROR_STAGES } from "./host-errors";
export type { HostError, HostErrorCode, HostErrorStage } from "./host-errors";

export const HOST_PROTOCOL_VERSION = 75 as const;

export { HOST_COMMANDS } from "./host-command-vocabulary";

export const HOST_EVENTS = [
  "records.analysis.changed",
  "records.export.changed",
  "observations.watch.changed",
  "backend.availability",
  "plugin.event",
  "plugins.changed",
  "plugins.network.progress",
  "connection.state",
  "topics.changed",
  "consumerGroups.changed",
  "consumerGroup.changed",
  "schemas.changed",
  "schema.changed",
  "schemaCompatibility.changed",
  "acls.changed",
  "transforms.changed",
  "transform.changed",
  "transformLogs.changed",
  "consumption.state",
  "messages.batch",
  "activity.recorded",
  "profiles.changed",
  "recipes.changed",
  "preferences.changed",
  "rules.changed",
  "rules.evaluation",
  "rules.notification",
  "topicConfiguration.changed",
  "topicConfiguration.history",
  "clusterDetails.changed",
  "latency.changed",
  "latency.history.changed",
  "streamMetrics.changed",
] as const;

export const SECURE_CONNECTION_LIMITS = {
  brokers: 32,
  brokerCharacters: 512,
  caPemCharacters: 1_500_000,
  clientIdCharacters: 512,
  clientSecretCharacters: 4_096,
  nameCharacters: 256,
  scopeCharacters: 1_024,
  tokenEndpointCharacters: 2_048,
} as const;

export const HOST_ACTIVITY_DETAIL_CHARACTER_LIMIT = 4_096 as const;
export const HOST_ACTIVITY_HISTORY_LIMIT = 100 as const;
export const KAFKA_MESSAGE_LIMITS = {
  batchBytes: 1_060_864,
  batchMessages: 200,
  emptyObservationMs: 1_000,
  headerCount: 128,
  headerKeyBytes: 512,
  headerValueBytes: 8_192,
  messageBytes: 1_048_576,
  previewBytes: 8_192,
  queuedBytes: 16 * 1_048_576,
  queuedMessages: 1_000,
  retainedBytes: 64 * 1_048_576,
  retainedMessages: 1_000,
} as const;
export const KAFKA_FETCH_MODES = ["tail", "newest", "earliest", "time-window"] as const;
export const KAFKA_FETCH_MODE_LABELS = {
  earliest: "First N",
  newest: "Newest N",
  tail: "Tail",
  "time-window": "Time window",
} as const satisfies Record<(typeof KAFKA_FETCH_MODES)[number], string>;
export const KAFKA_FETCH_LIMITS = {
  defaultMaxMessages: KAFKA_MESSAGE_LIMITS.retainedMessages,
  defaultTimeWindowMs: 120_000,
  maxMessages: KAFKA_MESSAGE_LIMITS.retainedMessages,
} as const;

export type HostCommandName = (typeof HOST_COMMANDS)[number];
export type HostEventName = (typeof HOST_EVENTS)[number];
export type KafkaFetchMode = (typeof KAFKA_FETCH_MODES)[number];

interface KafkaFetchRequestBase {
  readonly search?: KafkaSearchFilter;
  readonly maxMessages: number;
  readonly topic: string;
}

export type KafkaFetchRequest =
  | (KafkaFetchRequestBase & {
      readonly mode: "tail" | "newest" | "earliest";
    })
  | (KafkaFetchRequestBase & {
      readonly endTimeMs: number;
      readonly mode: "time-window";
      readonly startTimeMs: number;
    });

export interface OAuthConnectionInput {
  readonly clientId: string;
  readonly clientSecret: string;
  readonly scope: string;
  readonly tokenEndpoint: string;
}

export type TlsConnectionInput =
  | {
      readonly enabled: false;
    }
  | {
      readonly caPem: string;
      readonly clientIdentity?: ConnectionClientIdentity;
      readonly enabled: true;
    };

export interface SecureConnectionInput {
  readonly brokers: readonly string[];
  readonly name: string;
  readonly oauth?: OAuthConnectionInput;
  readonly sasl?: ConnectionSasl;
  readonly services?: ResolvedClusterServiceEndpoints;
  readonly tls: TlsConnectionInput;
}

export interface HostSecureConnectionInput {
  readonly brokers: readonly string[];
  readonly name: string;
  readonly oauth?: OAuthConnectionInput;
  readonly sasl?: ConnectionSasl;
  readonly services?: ResolvedClusterServiceEndpoints;
  readonly tls: AcquiredTlsConnectionInput | TlsConnectionInput;
}

export type SecureConnectionField =
  | "brokers"
  | "name"
  | "oauth.clientId"
  | "oauth.clientSecret"
  | "oauth.scope"
  | "oauth.tokenEndpoint"
  | "sasl"
  | "sasl.mechanism"
  | "sasl.username"
  | "sasl.password"
  | "tls.clientIdentity"
  | `services.${"connect" | "schemaRegistry" | "redpandaAdmin"}.${"basic" | "bearer" | "oauth" | "tls"}`
  | "services.connect.authentication"
  | "services.connect.baseUrl"
  | "services.redpandaAdmin.authentication"
  | "services.redpandaAdmin.baseUrl"
  | "services.schemaRegistry.authentication"
  | "services.schemaRegistry.baseUrl"
  | "tls.acquisitionId"
  | "tls.caPem"
  | "tls.enabled";

export interface SecureConnectionIssue {
  readonly field: SecureConnectionField;
  readonly message: string;
}

export interface HostCommandBase {
  readonly id: string;
  readonly version: typeof HOST_PROTOCOL_VERSION;
}

type HostCommandDefinition =
  | import("./schema-change-protocol").SchemaChangeCommand
  | import("./schema-policy-protocol").SchemaPolicyCommand
  | (HostCommandBase & {
      readonly command: "schemas.author";
      readonly payload: import("./schema-authoring").SchemaAuthoringInput;
    })
  | import("./topic-catalog-protocol").TopicCatalogCommand
  | import("./record-locator-protocol").RecordLocatorCommand
  | import("./record-analysis-protocol").RecordAnalysisCommand
  | import("./record-export-protocol").RecordExportCommand
  | import("./connect").ConnectHostCommand
  | import("./connect-offsets").ConnectOffsetsCommand
  | import("./environment-protocol").EnvironmentHostCommand
  | import("./relationship-protocol").RelationshipCommand
  | import("./observation-protocol").ObservationCommand
  | (HostCommandBase & {
      readonly command: "schemas.client";
      readonly payload: import("./schema-inspection").SchemaInspectionInput;
    })
  | import("./recovery-command-types").RecoveryHostCommand
  | (HostCommandBase & {
      readonly command: "records.trace";
      readonly payload: import("./correlation-trace").CorrelationTraceInput;
    })
  | (HostCommandBase & {
      readonly command: "records.trace.cancel";
      readonly payload: { readonly traceId: string };
    })
  | (HostCommandBase & {
      readonly command: "schemas.samples";
      readonly payload: import("./schema-samples").SchemaSampleInput;
    })
  | (HostCommandBase & {
      readonly command: "records.batch.review";
      readonly payload: import("./schema-samples").RecordBatchInput;
    })
  | (HostCommandBase & {
      readonly command: "records.batch.apply" | "records.batch.cancel";
      readonly payload: { readonly planId: string };
    })
  | (HostCommandBase & {
      readonly command: "schemas.inspect";
      readonly payload: import("./schema-inspection").SchemaInspectionInput;
    })
  | (HostCommandBase & {
      readonly command: "records.decode";
      readonly payload: import("./record-codec").RecordDecodeInput;
    })
  | (HostCommandBase & {
      readonly command: "queries.list";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "queries.put";
      readonly payload: {
        readonly query: KafkaSavedQuery;
        readonly expected?: KafkaSavedQuery | null;
      };
    })
  | (HostCommandBase & {
      readonly command: "queries.delete";
      readonly payload: { readonly id: string };
    })
  | (HostCommandBase & {
      readonly command:
        | "plugins.list"
        | "plugins.delivery"
        | "plugins.network.get"
        | "plugins.network.test"
        | "plugins.restart"
        | "plugins.exit.prepare";
      readonly payload: Record<string, never>;
    })
  | (HostCommandBase & {
      readonly command: "plugins.network.update";
      readonly payload: PluginNetworkUpdateInput;
    })
  | (HostCommandBase & {
      readonly command: "plugins.network.cancel";
      readonly payload: { readonly requestId: string };
    })
  | (HostCommandBase & {
      readonly command: "plugins.catalog";
      readonly payload: { readonly refresh?: boolean };
    })
  | (HostCommandBase & {
      readonly command: "plugins.package.inspect";
      readonly payload: PluginPackageInspectInput;
    })
  | (HostCommandBase & {
      readonly command: "plugins.package.change.prepare" | "plugins.package.discard";
      readonly payload: { readonly candidateId: string };
    })
  | (HostCommandBase & {
      readonly command: "plugins.package.install";
      readonly payload: { readonly candidateId: string; readonly confirmationToken?: string };
    })
  | (HostCommandBase & {
      readonly command: "plugins.install" | "plugins.remove" | "plugins.retry";
      readonly payload: { readonly pluginId: string; readonly confirmationToken?: string };
    })
  | (HostCommandBase & {
      readonly command: "plugins.change.prepare";
      readonly payload: {
        readonly pluginId: string;
        readonly operation: import("../../../plugins/contracts").PluginChangeOperation;
      };
    })
  | (HostCommandBase & {
      readonly command: "plugins.renderer.failed";
      readonly payload: {
        readonly pluginId: string;
        readonly activationId: string;
        readonly error: string;
      };
    })
  | (HostCommandBase & {
      readonly command: "plugins.exit.resolve";
      readonly payload: { readonly pluginId: string; readonly action: string };
    })
  | (HostCommandBase & {
      readonly command: "plugin.execute";
      readonly payload: {
        readonly pluginId: string;
        readonly method: string;
        readonly activationId?: string;
        readonly input: JsonValue;
      };
    })
  | (HostCommandBase & {
      readonly command: "connection.test";
      readonly payload: HostSecureConnectionInput;
    })
  | (HostCommandBase & {
      readonly command: "connection.connect";
      readonly payload: HostSecureConnectionInput;
    })
  | (HostCommandBase & {
      readonly command: "connection.disconnect";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "profiles.list";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "profiles.binding.get";
      readonly payload: { readonly profileId: string };
    })
  | (HostCommandBase & {
      readonly command: "profiles.create";
      readonly payload: {
        readonly profile: ProfileCreateInput;
      };
    })
  | (HostCommandBase & {
      readonly command: "profiles.update";
      readonly payload: {
        readonly profile: ProfileUpdateInput;
        readonly profileId: string;
      };
    })
  | (HostCommandBase & {
      readonly command: "profiles.test";
      readonly payload: ProfileTestInput;
    })
  | (HostCommandBase & {
      readonly command: "profiles.delete";
      readonly payload: {
        readonly profileId: string;
      };
    })
  | (HostCommandBase & {
      readonly command: "profiles.connect";
      readonly payload: {
        readonly profileId: string;
      };
    })
  | (HostCommandBase & {
      readonly command: "recipes.list" | "recipes.legacy.preview";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "recipes.create";
      readonly payload: TrustAcquisitionRecipeInput;
    })
  | (HostCommandBase & {
      readonly command: "recipes.update";
      readonly payload: {
        readonly id: string;
        readonly revision: number;
        readonly recipe: TrustAcquisitionRecipeInput;
      };
    })
  | (HostCommandBase & {
      readonly command: "recipes.export" | "recipes.usage";
      readonly payload: { readonly id: string; readonly revision: number };
    })
  | (HostCommandBase & {
      readonly command: "recipes.delete";
      readonly payload: {
        readonly id: string;
        readonly revision: number;
        readonly confirmedProfileIds?: readonly string[];
      };
    })
  | (HostCommandBase & {
      readonly command: "recipes.duplicate";
      readonly payload: { readonly id: string; readonly revision: number; readonly name: string };
    })
  | (HostCommandBase & {
      readonly command: "recipes.import.preview";
      readonly payload: { readonly contents: string };
    })
  | (HostCommandBase & {
      readonly command: "recipes.legacy.convert";
      readonly payload: LegacyTrustRecipeSelection & { readonly expectedSourceRevision: string };
    })
  | (HostCommandBase & {
      readonly command: "preferences.get" | "preferences.reset";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "preferences.update";
      readonly payload: KafkaOperationalPreferenceUpdateInput;
    })
  | (HostCommandBase & {
      readonly command: "rules.list";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "rules.create" | "rules.validate";
      readonly payload: {
        readonly rule: KafkaRuleDefinition;
      };
    })
  | (HostCommandBase & {
      readonly command: "rules.update";
      readonly payload: {
        readonly originalName: string;
        readonly rule: KafkaRuleDefinition;
      };
    })
  | (HostCommandBase & {
      readonly command: "rules.delete";
      readonly payload: {
        readonly name: string;
      };
    })
  | (HostCommandBase & {
      readonly command: "rules.evaluate";
      readonly payload: KafkaRuleEvaluationInput;
    })
  | (HostCommandBase & {
      readonly command: "topics.list";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "consumerGroups.list";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "consumerGroups.load";
      readonly payload: {
        readonly groupId: string;
      };
    })
  | (HostCommandBase & {
      readonly command: "schemas.list";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "schemas.load";
      readonly payload: { readonly subject: string; readonly version: number | "latest" };
    })
  | (HostCommandBase & {
      readonly command: "schemas.compatibility.check";
      readonly payload: SchemaCompatibilityCheckInput;
    })
  | (HostCommandBase & {
      readonly command: "schemas.register";
      readonly payload: SchemaRegistrationInput;
    })
  | (HostCommandBase & {
      readonly command: "schemas.delete";
      readonly payload: SchemaDeletionInput;
    })
  | (HostCommandBase & {
      readonly command: "acls.list";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "writes.review";
      readonly payload: import("./reviewed-writes").KafkaWriteInput;
    })
  | (HostCommandBase & {
      readonly command: "writes.apply";
      readonly payload: { readonly planId: string };
    })
  | (HostCommandBase & {
      readonly command: "acls.create";
      readonly payload: KafkaAclBinding;
    })
  | (HostCommandBase & {
      readonly command: "acls.delete";
      readonly payload: KafkaAclDeletionInput;
    })
  | (HostCommandBase & {
      readonly command: "transforms.list";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "transforms.load" | "transforms.logs.load";
      readonly payload: { readonly name: string };
    })
  | (HostCommandBase & {
      readonly command: "transforms.delete";
      readonly payload: RedpandaTransformDeletionInput;
    })
  | (HostCommandBase & {
      readonly command: "topicConfiguration.load" | "topicConfiguration.history";
      readonly payload: {
        readonly topic: string;
      };
    })
  | (HostCommandBase & {
      readonly command: "topicConfiguration.apply" | "topicConfiguration.validate";
      readonly payload: KafkaTopicConfigurationOperationInput;
    })
  | (HostCommandBase & {
      readonly command: "clusterDetails.load" | "clusterDetails.export";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "latency.start";
      readonly payload: KafkaLatencyProbeRequest;
    })
  | (HostCommandBase & {
      readonly command: "latency.export" | "latency.stop";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "messages.stop";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "messages.start";
      readonly payload: KafkaFetchRequest;
    })
  | (HostCommandBase & {
      readonly command: "messages.continue";
      readonly payload: { readonly continuationId: string };
    })
  | (HostCommandBase & {
      readonly command: "trustAcquisition.hostKey.discover";
      readonly payload: RemoteTrustHostKeyDiscoveryInput;
    })
  | (HostCommandBase & {
      readonly command: "trustAcquisition.capabilities";
      readonly payload: Readonly<Record<string, never>>;
    })
  | (HostCommandBase & {
      readonly command: "trustAcquisition.editor.open";
      readonly payload: { readonly profile?: { readonly id: string; readonly revision: number } };
    })
  | (HostCommandBase & {
      readonly command: "trustAcquisition.editor.advance";
      readonly payload: import("./remote-trust-types").TrustAcquisitionEditor;
    })
  | (HostCommandBase & {
      readonly command: "trustAcquisition.editor.close";
      readonly payload: { readonly editorId: string };
    })
  | (HostCommandBase & {
      readonly command: "trustAcquisition.apply";
      readonly payload: { readonly acquisitionId: string; readonly editorId: string };
    })
  | (HostCommandBase & {
      readonly command: "trustAcquisition.https.fetch";
      readonly payload: import("./remote-trust-types").HttpsTrustMaterialFetchInput;
    })
  | (HostCommandBase & {
      readonly command: "trustAcquisition.material.fetch";
      readonly payload: RemoteTrustMaterialFetchInput;
    })
  | (HostCommandBase & {
      readonly command: "trustAcquisition.cancel";
      readonly payload: { readonly requestId: string; readonly editorId?: string };
    })
  | (HostCommandBase & {
      readonly command: "trustAcquisition.discard";
      readonly payload: RemoteTrustAcquisitionIdentity;
    });

/** Split grouped names into individual discriminants for payload and result inference. */
type DistributeHostCommand<Command> = Command extends { readonly command: HostCommandName }
  ? {
      [Name in Command["command"]]: Omit<Command, "command"> & { readonly command: Name };
    }[Command["command"]]
  : never;
export type HostCommand = DistributeHostCommand<HostCommandDefinition>;

export interface HostCommandAccepted {
  readonly correlationId: string;
  readonly profileId?: string;
}

type AcknowledgementSuccess<Name extends HostCommandName> = [
  Extract<Name, HostAcknowledgementCommandName>,
] extends [never]
  ? never
  : {
      readonly command: Extract<Name, HostAcknowledgementCommandName>;
      readonly id: string;
      readonly ok: true;
      readonly result: { readonly correlationId: string };
      readonly version: typeof HOST_PROTOCOL_VERSION;
    };

export type HostCommandSuccess<Name extends HostCommandName = HostCommandName> =
  | AcknowledgementSuccess<Name>
  | {
      [Command in Exclude<Name, HostAcknowledgementCommandName>]: {
        readonly command: Command;
        readonly id: string;
        readonly ok: true;
        readonly result: HostCommandResultMap[Command];
        readonly version: typeof HOST_PROTOCOL_VERSION;
      };
    }[Exclude<Name, HostAcknowledgementCommandName>];

export type HostCommandFailure<Name extends HostCommandName = HostCommandName> = {
  readonly command: Name;
  readonly error: HostError;
  readonly id: string;
  readonly ok: false;
  readonly version: typeof HOST_PROTOCOL_VERSION;
};

export type HostCommandResponse<Name extends HostCommandName = HostCommandName> =
  HostCommandSuccess<Name> | HostCommandFailure<Name>;

export type BackendAvailability = "ready" | "unavailable";
export type ConnectionState =
  "disconnected" | "connecting" | "connected" | "disconnecting" | "failed";
export type TopicListState = "loading" | "ready" | "denied" | "failed";
export type ConsumptionState =
  | "unavailable"
  | "loading"
  | "fetching"
  | "streaming"
  | "complete"
  | "stopped"
  | "empty"
  | "failed";
export type ActivitySeverity = "info" | "warning" | "error";
export type ActivityOutcome = "started" | "succeeded" | "cancelled" | "failed";

export interface ActivityEntry {
  readonly correlationId: string;
  readonly detail: string;
  readonly id: string;
  readonly object: string;
  readonly operation: string;
  readonly outcome: ActivityOutcome;
  readonly severity: ActivitySeverity;
  readonly timestamp: string;
}

export interface HostEventBase {
  readonly sequence: number;
  readonly version: typeof HOST_PROTOCOL_VERSION;
}

export type HostEvent =
  | (HostEventBase & {
      readonly event: "observations.watch.changed";
      readonly payload: import("./observation-watch").ObservationWatchSnapshot;
    })
  | import("./record-analysis-protocol").RecordAnalysisEvent
  | import("./record-export-protocol").RecordExportEvent
  | (HostEventBase & {
      readonly event: "plugins.network.progress";
      readonly payload: PluginAcquisitionProgress;
    })
  | (HostEventBase & {
      readonly event: "plugins.changed";
      readonly payload: PluginSnapshot;
    })
  | (HostEventBase & {
      readonly event: "backend.availability";
      readonly payload: {
        readonly recovery?: string;
        readonly state: BackendAvailability;
      };
    })
  | (HostEventBase & {
      readonly event: "plugin.event";
      readonly payload: PluginEvent;
    })
  | (HostEventBase & {
      readonly event: "connection.state";
      readonly payload: {
        readonly connectionName: string | null;
        readonly error?: HostError;
        readonly state: ConnectionState;
      };
    })
  | (HostEventBase & {
      readonly event: "topics.changed";
      readonly payload: {
        readonly error?: HostError;
        readonly refreshedAt: string | null;
        readonly state: TopicListState;
        readonly topics: readonly string[];
      };
    })
  | (HostEventBase & {
      readonly event: "consumerGroups.changed";
      readonly payload: KafkaConsumerGroupInventorySnapshot;
    })
  | (HostEventBase & {
      readonly event: "consumerGroup.changed";
      readonly payload: KafkaConsumerGroupDetailSnapshot;
    })
  | (HostEventBase & {
      readonly event: "schemas.changed";
      readonly payload: SchemaRegistryInventorySnapshot;
    })
  | (HostEventBase & {
      readonly event: "schema.changed";
      readonly payload: SchemaRegistryDetailSnapshot;
    })
  | (HostEventBase & {
      readonly event: "schemaCompatibility.changed";
      readonly payload: SchemaCompatibilitySnapshot;
    })
  | (HostEventBase & {
      readonly event: "acls.changed";
      readonly payload: KafkaAclSnapshot;
    })
  | (HostEventBase & {
      readonly event: "transforms.changed";
      readonly payload: RedpandaTransformInventorySnapshot;
    })
  | (HostEventBase & {
      readonly event: "transform.changed";
      readonly payload: RedpandaTransformDetailSnapshot;
    })
  | (HostEventBase & {
      readonly event: "transformLogs.changed";
      readonly payload: RedpandaTransformLogsSnapshot;
    })
  | (HostEventBase & {
      readonly event: "consumption.state";
      readonly payload: {
        readonly coverage?: KafkaReadCoverage;
        readonly searchProgress?: KafkaSearchProgress;
        readonly droppedMessages: number;
        readonly error?: HostError;
        readonly receivedMessages: number;
        readonly request: KafkaFetchRequest | null;
        readonly ruleEvaluation: KafkaLiveRuleCapability;
        readonly state: ConsumptionState;
      };
    })
  | (HostEventBase & {
      readonly event: "messages.batch";
      readonly payload: {
        readonly droppedMessages: number;
        readonly messages: readonly KafkaExploredMessage[];
        readonly topic: string;
      };
    })
  | (HostEventBase & {
      readonly event: "activity.recorded";
      readonly payload: ActivityEntry;
    })
  | (HostEventBase & {
      readonly event: "profiles.changed";
      readonly payload: {
        readonly profiles: readonly ProfileSummary[];
        readonly store: ProfileStoreCapability;
      };
    })
  | (HostEventBase & {
      readonly event: "recipes.changed";
      readonly payload: TrustAcquisitionRecipeSnapshot;
    })
  | (HostEventBase & {
      readonly event: "preferences.changed";
      readonly payload: KafkaOperationalPreferenceSnapshot;
    })
  | (HostEventBase & {
      readonly event: "rules.changed";
      readonly payload: KafkaRuleSnapshot;
    })
  | (HostEventBase & {
      readonly event: "rules.evaluation";
      readonly payload: KafkaRuleEvaluationReport;
    })
  | (HostEventBase & {
      readonly event: "rules.notification";
      readonly payload: KafkaRuleNotification;
    })
  | (HostEventBase & {
      readonly event: "topicConfiguration.changed";
      readonly payload: KafkaTopicConfigurationSnapshot;
    })
  | (HostEventBase & {
      readonly event: "topicConfiguration.history";
      readonly payload: KafkaTopicConfigurationHistorySnapshot;
    })
  | (HostEventBase & {
      readonly event: "clusterDetails.changed";
      readonly payload: KafkaClusterDiagnosticsSnapshot;
    })
  | (HostEventBase & {
      readonly event: "latency.changed";
      readonly payload: KafkaLatencySnapshot;
    })
  | (HostEventBase & {
      readonly event: "latency.history.changed";
      readonly payload: KafkaLatencyHistorySnapshot;
    })
  | (HostEventBase & {
      readonly event: "streamMetrics.changed";
      readonly payload: KafkaStreamMonitorSnapshot;
    });

export type HostEventListener = (event: HostEvent) => void;

export type KafkaHostExecute = <Command extends HostCommand>(
  command: Command,
) => Promise<HostCommandResponse<Command["command"]>>;

export type StreamSkopeBackend = ProviderHostPort<KafkaHostExecute, HostEvent>;

export interface StreamSkopeHost extends StreamSkopeBackend {
  openExternalUrl(url: string): Promise<ExternalUrlOpenResult>;
}
