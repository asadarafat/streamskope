import type {
  ClusterServiceEndpointsInput,
  ProfileCreateInput,
  ProfileStoreCapability,
  ProfileSummary,
  ProfileTestInput,
  ProfileUpdateInput,
} from "./profile-types";
import type { ProfileBindingDetailResult } from "./profile-binding";
import type {
  TrustAcquisitionRecipeInput,
  TrustAcquisitionRecipeSnapshot,
  TrustRecipeReviewResult,
  LegacyTrustRecipeSelection,
} from "./trust-recipe-types";
import type {
  ConnectionTemplateCatalog,
  ConnectionTemplateSnapshot,
} from "./connection-template-types";
import type {
  KafkaLiveRuleCapability,
  KafkaLiveRuleEvaluation,
  KafkaRuleNotification,
} from "./live-rule-types";
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
import type {
  KafkaClusterDetailsExportResult,
  KafkaClusterDiagnosticsSnapshot,
} from "./cluster-diagnostics-types";
import type {
  KafkaLatencyExportResult,
  KafkaLatencyHistorySnapshot,
  KafkaLatencyProbeRequest,
  KafkaLatencySnapshot,
} from "./latency-types";
import type { KafkaStreamMonitorSnapshot } from "./stream-monitor-types";
import type {
  AcquiredTlsConnectionInput,
  RemoteTrustHostKeyDiscoveryInput,
  RemoteTrustHostKeyDiscoveryResult,
  RemoteTrustAcquisitionIdentity,
  RemoteTrustAcquisitionResult,
  RemoteTrustMaterialFetchInput,
  RemoteTrustPasswordFetchInput,
} from "./remote-trust-types";
import type {
  KafkaOperationalPreferenceResult,
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

export const HOST_PROTOCOL_VERSION = 17 as const;

export const HOST_COMMANDS = [
  "connection.test",
  "connection.connect",
  "connection.disconnect",
  "profiles.list",
  "profiles.binding.get",
  "profiles.create",
  "profiles.update",
  "profiles.test",
  "profiles.delete",
  "profiles.connect",
  "templates.list",
  "templates.create",
  "templates.update",
  "templates.delete",
  "templates.select",
  "recipes.list",
  "recipes.create",
  "recipes.update",
  "recipes.delete",
  "recipes.usage",
  "recipes.duplicate",
  "recipes.import.preview",
  "recipes.export",
  "recipes.legacy.preview",
  "recipes.legacy.convert",
  "preferences.get",
  "preferences.update",
  "preferences.reset",
  "rules.list",
  "rules.create",
  "rules.update",
  "rules.delete",
  "rules.validate",
  "rules.evaluate",
  "topics.list",
  "consumerGroups.list",
  "consumerGroups.load",
  "schemas.list",
  "schemas.load",
  "schemas.compatibility.check",
  "schemas.register",
  "schemas.delete",
  "acls.list",
  "acls.create",
  "acls.delete",
  "transforms.list",
  "transforms.load",
  "transforms.logs.load",
  "transforms.delete",
  "topicConfiguration.load",
  "topicConfiguration.validate",
  "topicConfiguration.apply",
  "topicConfiguration.history",
  "clusterDetails.load",
  "clusterDetails.export",
  "latency.start",
  "latency.stop",
  "latency.export",
  "messages.start",
  "messages.stop",
  "trustAcquisition.hostKey.discover",
  "trustAcquisition.capabilities",
  "trustAcquisition.editor.open",
  "trustAcquisition.editor.advance",
  "trustAcquisition.editor.close",
  "trustAcquisition.apply",
  "trustAcquisition.password.fetch",
  "trustAcquisition.material.fetch",
  "trustAcquisition.https.fetch",
  "trustAcquisition.discard",
  "trustAcquisition.cancel",
] as const;

export const HOST_EVENTS = [
  "backend.availability",
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
  "templates.changed",
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

export const HOST_ERROR_CODES = [
  "VALIDATION",
  "CANCELLED",
  "TIMEOUT",
  "OAUTH_REJECTED",
  "OAUTH_UNREACHABLE",
  "TLS_TRUST",
  "HTTPS_AUTHENTICATION",
  "HTTPS_AUTHORIZATION",
  "HTTPS_REDIRECT",
  "HTTPS_RESPONSE",
  "BROKER_UNREACHABLE",
  "KAFKA_AUTHENTICATION",
  "AUTHORIZATION_DENIED",
  "UNSUPPORTED_OPERATION",
  "BACKEND_UNAVAILABLE",
  "PROFILE_STORE_UNAVAILABLE",
  "PROFILE_DUPLICATE",
  "PROFILE_NOT_FOUND",
  "PROFILE_ACTIVE",
  "PROFILE_CORRUPT",
  "PROFILE_DECRYPTION",
  "TEMPLATE_STORE_UNAVAILABLE",
  "TEMPLATE_DUPLICATE",
  "TEMPLATE_NOT_FOUND",
  "TEMPLATE_CORRUPT",
  "PREFERENCE_STORE_UNAVAILABLE",
  "PREFERENCE_CORRUPT",
  "RULE_VALIDATION",
  "RULE_DUPLICATE",
  "RULE_NOT_FOUND",
  "RULE_CAPACITY",
  "RULE_SAMPLE",
  "RULE_STORE_UNAVAILABLE",
  "RULE_CORRUPT",
  "TRUST_MATERIAL",
  "TRUSTSTORE_PASSWORD",
  "SSH_IDENTITY",
  "SSH_AUTHENTICATION",
  "SSH_UNREACHABLE",
  "REMOTE_COMMAND",
  "REMOTE_TRANSFER",
  "REMOTE_CLEANUP",
  "ACQUISITION_NOT_FOUND",
  "ACQUISITION_EXPIRED",
  "ACQUISITION_INCOMPLETE",
  "ACQUISITION_CAPACITY",
  "TOPIC_NOT_FOUND",
  "CONSUMER_GROUP_NOT_FOUND",
  "INVALID_TOPIC_CONFIG",
  "TOPIC_CONFIG_HISTORY_UNAVAILABLE",
  "TOPIC_CONFIG_HISTORY_CORRUPT",
  "INTERNAL",
] as const;

export const HOST_ERROR_STAGES = [
  "validation",
  "oauth",
  "tls",
  "broker",
  "kafka",
  "authorization",
  "backend",
  "profile",
  "template",
  "preference",
  "rule",
  "storage",
  "trust",
  "ssh",
  "remote-command",
  "remote-transfer",
  "acquisition",
  "internal",
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
export type HostErrorCode = (typeof HOST_ERROR_CODES)[number];
export type HostErrorStage = (typeof HOST_ERROR_STAGES)[number];
export type KafkaFetchMode = (typeof KAFKA_FETCH_MODES)[number];

interface KafkaFetchRequestBase {
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

export interface TlsConnectionInput {
  readonly caPem: string;
  readonly enabled: true;
}

export interface SecureConnectionInput {
  readonly brokers: readonly string[];
  readonly name: string;
  readonly oauth?: OAuthConnectionInput;
  readonly services?: ClusterServiceEndpointsInput;
  readonly tls: TlsConnectionInput;
}

export interface HostSecureConnectionInput {
  readonly brokers: readonly string[];
  readonly name: string;
  readonly oauth?: OAuthConnectionInput;
  readonly services?: ClusterServiceEndpointsInput;
  readonly tls: AcquiredTlsConnectionInput | TlsConnectionInput;
}

export type SecureConnectionField =
  | "brokers"
  | "name"
  | "oauth.clientId"
  | "oauth.clientSecret"
  | "oauth.scope"
  | "oauth.tokenEndpoint"
  | "services.redpandaAdmin.authentication"
  | "services.redpandaAdmin.baseUrl"
  | "services.schemaRegistry.authentication"
  | "services.schemaRegistry.baseUrl"
  | "tls.acquisitionId"
  | "tls.caPem";

export interface SecureConnectionIssue {
  readonly field: SecureConnectionField;
  readonly message: string;
}

interface HostCommandBase {
  readonly id: string;
  readonly version: typeof HOST_PROTOCOL_VERSION;
}

export type HostCommand =
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
      readonly command: "templates.list";
      readonly payload: Readonly<Record<string, never>>;
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
      readonly command: "templates.create";
      readonly payload: {
        readonly catalog: ConnectionTemplateCatalog;
        readonly name: string;
        readonly template: string;
      };
    })
  | (HostCommandBase & {
      readonly command: "templates.update";
      readonly payload: {
        readonly catalog: ConnectionTemplateCatalog;
        readonly name: string;
        readonly originalName: string;
        readonly template: string;
      };
    })
  | (HostCommandBase & {
      readonly command: "templates.delete" | "templates.select";
      readonly payload: {
        readonly catalog: ConnectionTemplateCatalog;
        readonly name: string;
      };
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
      readonly command: "trustAcquisition.password.fetch";
      readonly payload: RemoteTrustPasswordFetchInput;
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

export interface HostError {
  readonly activeStateChanged: boolean;
  readonly code: HostErrorCode;
  readonly correlationId: string;
  readonly recovery: string;
  readonly retryable: boolean;
  readonly stage: HostErrorStage;
  readonly summary: string;
  readonly target?: string;
}

export interface HostCommandAccepted {
  readonly correlationId: string;
}

export type HostCommandResponse =
  | {
      readonly command: HostCommandName;
      readonly id: string;
      readonly ok: true;
      readonly result:
        | HostCommandAccepted
        | {
            readonly correlationId: string;
            readonly editor: import("./remote-trust-types").TrustAcquisitionEditor;
          }
        | ({
            readonly correlationId: string;
          } & import("./remote-trust-types").TrustAcquisitionCapabilities)
        | ProfileBindingDetailResult
        | TrustRecipeReviewResult
        | KafkaClusterDetailsExportResult
        | KafkaLatencyExportResult
        | KafkaOperationalPreferenceResult
        | RemoteTrustHostKeyDiscoveryResult
        | RemoteTrustAcquisitionResult;
      readonly version: typeof HOST_PROTOCOL_VERSION;
    }
  | {
      readonly command: HostCommandName;
      readonly error: HostError;
      readonly id: string;
      readonly ok: false;
      readonly version: typeof HOST_PROTOCOL_VERSION;
    };

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

export interface KafkaMessage {
  readonly headers: Readonly<Record<string, string>>;
  readonly id: string;
  readonly key: string | null;
  readonly offset: string;
  readonly originalByteSize: number;
  readonly partition: number;
  readonly payload: string | null;
  readonly preview: string;
  readonly timestamp: string;
  readonly topic: string;
  readonly truncated: boolean;
}

export interface KafkaExploredMessage extends KafkaMessage {
  readonly ruleEvaluation: KafkaLiveRuleEvaluation;
}

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

interface HostEventBase {
  readonly sequence: number;
  readonly version: typeof HOST_PROTOCOL_VERSION;
}

export type HostEvent =
  | (HostEventBase & {
      readonly event: "backend.availability";
      readonly payload: {
        readonly recovery?: string;
        readonly state: BackendAvailability;
      };
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
      readonly event: "templates.changed";
      readonly payload: ConnectionTemplateSnapshot;
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

export interface StreamSkopeBackend {
  execute(command: HostCommand): Promise<HostCommandResponse>;
  subscribe(listener: HostEventListener): () => void;
}

export interface StreamSkopeHost extends StreamSkopeBackend {
  openExternalUrl(url: string): Promise<ExternalUrlOpenResult>;
}
