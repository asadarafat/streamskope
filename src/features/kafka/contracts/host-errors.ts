export const HOST_ERROR_CODES = [
  "QUERY_UNAVAILABLE",
  "VALIDATION",
  "OBSERVATION_BUSY",
  "OBSERVATION_COOLDOWN",
  "OBSERVATION_DISCONNECTED",
  "OBSERVATION_HISTORY_UNAVAILABLE",
  "OBSERVATION_INCOMPLETE",
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
  "query",
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

export type HostErrorCode = (typeof HOST_ERROR_CODES)[number];
export type HostErrorStage = (typeof HOST_ERROR_STAGES)[number];

export interface HostError {
  readonly retryAfterMs?: number;
  readonly activeStateChanged: boolean;
  readonly code: HostErrorCode;
  readonly correlationId: string;
  readonly recovery: string;
  readonly retryable: boolean;
  readonly stage: HostErrorStage;
  readonly summary: string;
  readonly target?: string;
}
