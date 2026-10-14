import type { HostErrorStage } from "../contracts";
import { classifyConnectionFailure } from "../application/connection-diagnostics";

import { KafkaEngineFailure } from "./failure";
import { OAuthEndpointResponseError } from "./oauth";
import type { KafkaReadOpeningStage } from "./types";

export function cancelledFailure(stage: HostErrorStage, target: string): KafkaEngineFailure {
  return new KafkaEngineFailure({
    code: "CANCELLED",
    recovery: "Retry the operation when the current connection change is complete.",
    retryable: true,
    stage,
    summary: "The Kafka operation was cancelled.",
    target,
  });
}

export function timeoutFailure(stage: HostErrorStage, target: string): KafkaEngineFailure {
  return new KafkaEngineFailure({
    code: "TIMEOUT",
    recovery:
      stage === "oauth"
        ? "Verify the OAuth endpoint and retry."
        : "Verify the broker endpoints and network path, then retry.",
    retryable: true,
    stage,
    summary:
      stage === "oauth"
        ? "OAuth token acquisition timed out."
        : "Kafka broker metadata access timed out.",
    target,
  });
}

export function readPreparationTimeoutFailure(
  stage: KafkaReadOpeningStage,
  target: string,
): KafkaEngineFailure {
  const action: Record<KafkaReadOpeningStage, string> = {
    "topic-metadata": "loading topic metadata",
    "starting-offsets": "finding starting offsets",
    "verify-topic": "verifying topic identity",
    "start-reader": "starting the message reader",
  };
  return new KafkaEngineFailure({
    code: "TIMEOUT",
    recovery:
      stage === "starting-offsets"
        ? "Retry with First N or a narrower Time window. If it still fails, check broker response times and the network path."
        : "Retry the read. If it still fails, check broker response times and the advertised broker endpoints from this host.",
    retryable: true,
    stage: "broker",
    summary: `Kafka reader preparation timed out while ${action[stage]}.`,
    target,
  });
}

export function oauthFailure(error: unknown, target: string): KafkaEngineFailure {
  if (error instanceof KafkaEngineFailure) {
    return error;
  }
  const category = classifyConnectionFailure(error);
  if (category === "tls-trust" || category === "tls-client") {
    return new KafkaEngineFailure({
      cause: error,
      code: "TLS_TRUST",
      recovery:
        "Verify the OAuth endpoint hostname, certificate validity and issuing CA in this profile's trust material.",
      retryable: false,
      stage: "tls",
      summary: "OAuth endpoint certificate validation failed.",
      target,
    });
  }
  if (error instanceof OAuthEndpointResponseError) {
    return new KafkaEngineFailure({
      cause: error,
      code:
        error.status === 400 || error.status === 401 || error.status === 403
          ? "OAUTH_REJECTED"
          : "OAUTH_UNREACHABLE",
      recovery: "Check the token endpoint, client identifier, client secret and required scope.",
      retryable: error.status >= 500,
      stage: "oauth",
      summary:
        error.status === 400 || error.status === 401 || error.status === 403
          ? "OAuth credentials were rejected."
          : "The OAuth token endpoint did not complete the request.",
      target,
    });
  }
  return new KafkaEngineFailure({
    cause: error,
    code: "OAUTH_UNREACHABLE",
    recovery: "Verify the OAuth endpoint, TLS trust and local network path, then retry.",
    retryable: true,
    stage: "oauth",
    summary: "The OAuth token endpoint could not be reached.",
    target,
  });
}

export function withCleanupFailure(
  failure: KafkaEngineFailure,
  cleanupCause: unknown,
): KafkaEngineFailure {
  return new KafkaEngineFailure({
    cause: failure.cause,
    cleanupCause,
    code: failure.code,
    recovery: failure.recovery,
    retryable: failure.retryable,
    stage: failure.stage,
    summary: failure.message,
    ...(failure.target === undefined ? {} : { target: failure.target }),
  });
}
