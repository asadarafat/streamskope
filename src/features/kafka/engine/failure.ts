import type { HostErrorCode, HostErrorStage } from "../contracts";

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

function errorChain(error: unknown): readonly unknown[] {
  const chain: unknown[] = [];
  let current = error;
  for (let depth = 0; depth < 8 && current !== undefined; depth += 1) {
    chain.push(current);
    if (current === null || typeof current !== "object" || !("cause" in current)) {
      break;
    }
    current = (current as { readonly cause?: unknown }).cause;
  }
  return chain;
}

function errorCodes(error: unknown): readonly string[] {
  return errorChain(error).flatMap((entry) => {
    if (
      entry !== null &&
      typeof entry === "object" &&
      "code" in entry &&
      typeof (entry as { readonly code?: unknown }).code === "string"
    ) {
      return [(entry as { readonly code: string }).code.toUpperCase()];
    }
    return [];
  });
}

function errorText(error: unknown): string {
  return errorChain(error)
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

  if (
    includesAny(codes, [
      "CERT_HAS_EXPIRED",
      "DEPTH_ZERO_SELF_SIGNED_CERT",
      "ERR_TLS_CERT_ALTNAME_INVALID",
      "SELF_SIGNED_CERT_IN_CHAIN",
      "UNABLE_TO_GET_ISSUER_CERT_LOCALLY",
      "UNABLE_TO_VERIFY_LEAF_SIGNATURE",
    ]) ||
    /CERTIFICATE|SELF.SIGNED|TLS|SSL/u.test(text)
  ) {
    return new KafkaEngineFailure({
      cause: error,
      cleanupCause,
      code: "TLS_TRUST",
      recovery: "Select the CA that issued the broker certificate and verify the broker hostname.",
      retryable: false,
      stage: "tls",
      summary: "Kafka broker certificate validation failed.",
      target,
    });
  }

  if (
    includesAny(codes, ["SASL_AUTHENTICATION_FAILED", "SASL_AUTHENTICATION_ERROR"]) ||
    /SASL.*AUTHENTICATION|AUTHENTICATION.*FAILED/u.test(text)
  ) {
    return new KafkaEngineFailure({
      cause: error,
      cleanupCause,
      code: "KAFKA_AUTHENTICATION",
      recovery: "Verify the OAuth token claims, scope and broker authentication configuration.",
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

  if (/AUTHORIZATION|NOT AUTHORIZED/u.test(text)) {
    return new KafkaEngineFailure({
      cause: error,
      cleanupCause,
      code: "AUTHORIZATION_DENIED",
      recovery: "Request permission for broker metadata access and retry.",
      retryable: false,
      stage: "authorization",
      summary: "Kafka denied metadata access.",
      target,
    });
  }

  if (
    includesAny(codes, [
      "ECONNREFUSED",
      "ECONNRESET",
      "EHOSTUNREACH",
      "ENETUNREACH",
      "ENOTFOUND",
    ]) ||
    /CONNECTION REFUSED|ECONNREFUSED|ENOTFOUND|UNREACHABLE/u.test(text)
  ) {
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
