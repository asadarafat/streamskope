import type {
  HostErrorCode,
  HostErrorStage,
  KafkaConfigurationEntry,
  KafkaAclBinding,
  KafkaConsumerGroupDetails,
  KafkaFetchRequest,
  KafkaLatencyProbeRequest,
  KafkaTopicConfigurationChange,
  KafkaTopicConfigurationEntry,
} from "../contracts";
import type {
  KafkaActiveConnection,
  KafkaClusterMetadata,
  KafkaConsumerGroupInventory,
  KafkaLatencyFetchSample,
  KafkaLatencyProbeMeasurement,
  KafkaMessageStream,
} from "../application";

export type { ConnectionCheck, KafkaConnectionTestResult } from "../application";

export interface OAuthToken {
  readonly expiresAt?: number;
  readonly value: string;
}

export interface OAuthTokenRequest {
  readonly caPem: string;
  readonly clientId: string;
  readonly clientSecret: string;
  readonly scope: string;
  readonly signal: AbortSignal;
  readonly tokenEndpoint: string;
}

export type OAuthTokenRequester = (request: OAuthTokenRequest) => Promise<OAuthToken>;
export type OAuthTokenProvider = () => Promise<OAuthToken>;

export interface KafkaClientInput {
  readonly brokers: readonly string[];
  readonly caPem: string;
  readonly oauthTokenProvider?: OAuthTokenProvider;
  readonly operationTimeoutMs: number;
}

export type KafkaAdminInput = KafkaClientInput;

export interface KafkaAdminPort {
  alterTopicConfiguration(
    topic: string,
    changes: readonly KafkaTopicConfigurationChange[],
    validateOnly: boolean,
  ): Promise<void>;
  close(): Promise<void>;
  createAcl?(acl: KafkaAclBinding): Promise<void>;
  deleteAcl?(acl: KafkaAclBinding): Promise<void>;
  describeBrokerConfiguration(brokerId: number): Promise<readonly KafkaConfigurationEntry[]>;
  describeClusterMetadata(): Promise<KafkaClusterMetadata>;
  describeConsumerGroup(groupId: string): Promise<KafkaConsumerGroupDetails>;
  describeTopicConfiguration(topic: string): Promise<readonly KafkaTopicConfigurationEntry[]>;
  listTopics(): Promise<readonly string[]>;
  listConsumerGroups(): Promise<KafkaConsumerGroupInventory>;
  listAcls?(): Promise<readonly KafkaAclBinding[]>;
}

export interface KafkaAdminFactory {
  create(input: KafkaAdminInput): KafkaAdminPort;
}

export interface KafkaRawMessage {
  readonly headers: ReadonlyMap<Buffer, Buffer>;
  readonly key?: Buffer;
  readonly offset: bigint;
  readonly partition: number;
  readonly timestamp: bigint;
  readonly topic: string;
  readonly value?: Buffer;
}

export interface KafkaRawMessageStream extends AsyncIterable<KafkaRawMessage> {
  close(): Promise<void>;
}

export interface KafkaConsumerInput extends KafkaClientInput {
  readonly groupId: string;
  readonly onFetchSample?: (sample: KafkaLatencyFetchSample) => void;
  readonly request: KafkaFetchRequest;
}

export interface KafkaConsumerFactory {
  open(input: KafkaConsumerInput): Promise<KafkaRawMessageStream>;
}

export interface KafkaLatencyProbeInput extends KafkaClientInput {
  readonly request: KafkaLatencyProbeRequest;
  readonly runId: string;
}

export interface KafkaLatencyProbePort {
  run(input: KafkaLatencyProbeInput, signal: AbortSignal): Promise<KafkaLatencyProbeMeasurement>;
}

export interface KafkaEngineConnection extends KafkaActiveConnection {
  listTopics(signal?: AbortSignal): Promise<readonly string[]>;
  openMessageStream(request: KafkaFetchRequest, signal: AbortSignal): Promise<KafkaMessageStream>;
}

export interface KafkaEngineFailureOptions {
  readonly cause?: unknown;
  readonly cleanupCause?: unknown;
  readonly code: HostErrorCode;
  readonly recovery: string;
  readonly retryable: boolean;
  readonly stage: HostErrorStage;
  readonly summary: string;
  readonly target?: string;
}

export interface StreamSkopeKafkaEngineOptions {
  readonly adminFactory?: KafkaAdminFactory;
  readonly consumerFactory?: KafkaConsumerFactory;
  readonly latencyProbe?: KafkaLatencyProbePort;
  readonly operationTimeoutMs?: number;
  readonly requestOAuthToken?: OAuthTokenRequester;
}
