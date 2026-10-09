import type { HostErrorCode, HostErrorStage } from "../contracts";
import {
  classifyConnectionFailure,
  connectionErrorChain,
} from "../application/connection-diagnostics";

import type { KafkaEngineFailureOptions } from "./types";

export class KafkaEngineFailure extends Error {
  readonly cleanupCause: unknown;
  readonly code: HostErrorCode;
  readonly recovery: string;
  readonly retryable: boolean;
  readonly stage: HostErrorStage;
  readonly target: string | undefined;

  constructor(options: KafkaEngineFailureOptions) {
    super(options.summary, { cause: options.cause });
    this.name = "KafkaEngineFailure";
    this.cleanupCause = options.cleanupCause;
    this.code = options.code;
    this.recovery = options.recovery;
    this.retryable = options.retryable;
    this.stage = options.stage;
    this.target = options.target;
  }
}

export function normalizeKafkaError(error: unknown): Error {
  return error instanceof Error
    ? error
    : new Error("An external Kafka operation rejected with a non-error value.", {
        cause: error,
      });
}

function errorCodes(error: unknown): readonly string[] {
  return connectionErrorChain(error).flatMap((entry) => {
    if (entry === null || typeof entry !== "object") return [];
    return ["code", "apiId"].flatMap((key) => {
      const value = (entry as Record<string, unknown>)[key];
      return typeof value === "string" ? [value.toUpperCase()] : [];
    });
  });
}

function errorText(error: unknown): string {
  return connectionErrorChain(error)
    .map((entry) =>
      entry instanceof Error
        ? `${entry.name} ${entry.message}`
        : typeof entry === "string"
          ? entry
          : "",
    )
    .join(" ")
    .toUpperCase();
}

function includesAny(values: readonly string[], candidates: readonly string[]): boolean {
  return candidates.some((candidate) => values.includes(candidate));
}

export function mapKafkaAdminFailure(
  error: unknown,
  target: string,
  cleanupCause?: unknown,
): KafkaEngineFailure {
  if (error instanceof KafkaEngineFailure) {
    return error;
  }
  const codes = errorCodes(error);
  const text = errorText(error);

  const diagnostic = classifyConnectionFailure(error);
  if (diagnostic === "tls-client" || diagnostic === "tls-trust") {
    return new KafkaEngineFailure({
      cause: error,
      cleanupCause,
      code: "TLS_TRUST",
      recovery:
        diagnostic === "tls-client"
          ? "Check the client certificate, matching private key, key passphrase and broker acceptance of its issuing CA."
          : "Select the CA that issued the broker certificate and verify its validity and broker hostname.",
      retryable: false,
      stage: "tls",
      summary:
        diagnostic === "tls-client"
          ? "Kafka client certificate authentication failed."
          : "Kafka broker certificate validation failed.",
      target,
    });
  }

  if (diagnostic === "authentication") {
    return new KafkaEngineFailure({
      cause: error,
      cleanupCause,
      code: "KAFKA_AUTHENTICATION",
      recovery:
        "Verify the selected SASL mechanism and its username/password or OAuth credentials, and the broker listener authentication configuration.",
      retryable: false,
      stage: "kafka",
      summary: "Kafka rejected the authenticated client.",
      target,
    });
  }

  if (
    includesAny(codes, ["UNSUPPORTED_OPERATION", "UNSUPPORTED_VERSION"]) ||
    /NOT SUPPORTED|UNSUPPORTED (?:OPERATION|VERSION)|DOES NOT SUPPORT/u.test(text)
  ) {
    return new KafkaEngineFailure({
      cause: error,
      cleanupCause,
      code: "UNSUPPORTED_OPERATION",
      recovery:
        "Verify that the connected Kafka-compatible broker supports this administration API.",
      retryable: false,
      stage: "kafka",
      summary: "The Kafka broker does not support this operation.",
      target,
    });
  }

  if (diagnostic === "authorization") {
    return new KafkaEngineFailure({
      cause: error,
      cleanupCause,
      code: "AUTHORIZATION_DENIED",
      recovery: "Request the Kafka permissions required by this operation and retry.",
      retryable: false,
      stage: "authorization",
      summary: "Kafka denied this operation.",
      target,
    });
  }

  if (diagnostic === "unreachable") {
    return new KafkaEngineFailure({
      cause: error,
      cleanupCause,
      code: "BROKER_UNREACHABLE",
      recovery: "Verify the bootstrap broker endpoints, listener address and local network path.",
      retryable: true,
      stage: "broker",
      summary: "No configured Kafka broker could be reached.",
      target,
    });
  }

  return new KafkaEngineFailure({
    cause: error,
    cleanupCause,
    code: "INTERNAL",
    recovery: "Open activity for the correlation ID and inspect the host diagnostics.",
    retryable: false,
    stage: "internal",
    summary: "The Kafka operation failed unexpectedly.",
    target,
  });
}

export function mapKafkaTopicConfigurationFailure(
  error: unknown,
  target: string,
): KafkaEngineFailure {
  if (error instanceof KafkaEngineFailure) {
    return error;
  }
  const codes = errorCodes(error);
  const text = errorText(error);

  if (
    includesAny(codes, ["TOPIC_AUTHORIZATION_FAILED"]) ||
    /AUTHORIZATION|NOT AUTHORIZED|ACL/u.test(text)
  ) {
    return new KafkaEngineFailure({
      cause: error,
      code: "AUTHORIZATION_DENIED",
      recovery:
        "Request DESCRIBE_CONFIGS or ALTER_CONFIGS permission for the selected topic and retry.",
      retryable: false,
      stage: "authorization",
      summary: "Kafka denied topic configuration access.",
      target,
    });
  }

  if (
    includesAny(codes, ["UNKNOWN_TOPIC_ID", "UNKNOWN_TOPIC_OR_PARTITION"]) ||
    /TOPIC NOT FOUND|UNKNOWN TOPIC|DID NOT RETURN CONFIGURATION METADATA/u.test(text)
  ) {
    return new KafkaEngineFailure({
      cause: error,
      code: "TOPIC_NOT_FOUND",
      recovery: "Refresh topics, select an existing topic, and retry.",
      retryable: false,
      stage: "kafka",
      summary: "The selected Kafka topic was not found.",
      target,
    });
  }

  if (
    includesAny(codes, ["INVALID_CONFIG", "INVALID_CONFIGURATION"]) ||
    /INVALID CONFIG|CONFIGURATION VALUE|CONFIG VALUE/u.test(text)
  ) {
    return new KafkaEngineFailure({
      cause: error,
      code: "INVALID_TOPIC_CONFIG",
      recovery: "Correct the proposed configuration values, dry-run them, and retry.",
      retryable: false,
      stage: "kafka",
      summary: "Kafka rejected the topic configuration.",
      target,
    });
  }

  return mapKafkaAdminFailure(error, target);
}

export function mapKafkaConsumerGroupFailure(error: unknown, target: string): KafkaEngineFailure {
  if (error instanceof KafkaEngineFailure) {
    return error;
  }
  const codes = errorCodes(error);
  const text = errorText(error);
  if (
    includesAny(codes, ["GROUP_AUTHORIZATION_FAILED"]) ||
    /GROUP.*AUTHORIZATION|AUTHORIZATION.*GROUP|NOT AUTHORIZED/u.test(text)
  ) {
    return new KafkaEngineFailure({
      cause: error,
      code: "AUTHORIZATION_DENIED",
      recovery: "Request DESCRIBE permission for consumer groups and retry.",
      retryable: false,
      stage: "authorization",
      summary: "Kafka denied consumer-group access.",
      target,
    });
  }
  if (
    includesAny(codes, ["GROUP_ID_NOT_FOUND"]) ||
    /DID NOT RETURN CONSUMER GROUP|GROUP (?:ID )?NOT FOUND|UNKNOWN CONSUMER GROUP/u.test(text)
  ) {
    return new KafkaEngineFailure({
      cause: error,
      code: "CONSUMER_GROUP_NOT_FOUND",
      recovery: "Refresh consumer groups, select an existing group, and retry.",
      retryable: false,
      stage: "kafka",
      summary: "The selected Kafka consumer group was not found.",
      target,
    });
  }
  return mapKafkaAdminFailure(error, target);
}
