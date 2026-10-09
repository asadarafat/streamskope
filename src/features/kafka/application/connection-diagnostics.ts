import type { HostErrorCode, HostErrorStage } from "../contracts";

export type ConnectionFailureKind =
  | "authentication"
  | "authorization"
  | "cancelled"
  | "timeout"
  | "tls-client"
  | "tls-trust"
  | "unreachable";

/** Inspect bounded nested errors; never copy remote text into a public diagnostic. */
export function connectionErrorChain(error: unknown): readonly unknown[] {
  const chain: unknown[] = [];
  const pending: unknown[] = [error];
  const seen = new Set<unknown>();
  while (pending.length > 0 && chain.length < 32) {
    const current = pending.shift();
    if (current === undefined || seen.has(current)) continue;
    seen.add(current);
    chain.push(current);
    if (current !== null && typeof current === "object") {
      if ("cause" in current) pending.push(current.cause);
      if ("errors" in current && Array.isArray(current.errors))
        pending.push(...(current.errors as unknown[]).slice(0, 32));
    }
  }
  return chain;
}

export function classifyConnectionFailure(error: unknown): ConnectionFailureKind | undefined {
  const chain = connectionErrorChain(error);
  const codes = chain.flatMap((entry) => {
    if (entry === null || typeof entry !== "object") return [];
    return ["code", "apiId"].flatMap((key) => {
      const value = (entry as Record<string, unknown>)[key];
      return typeof value === "string" ? [value.toUpperCase()] : [];
    });
  });
  const names = chain.filter((entry) => entry instanceof Error).map((entry) => entry.name);
  const status = chain.flatMap((entry) =>
    entry !== null && typeof entry === "object" && "status" in entry ? [entry.status] : [],
  );
  const text = chain
    .map((entry) => (entry instanceof Error ? `${entry.name} ${entry.message}` : ""))
    .join(" ")
    .toUpperCase();
  const has = (...values: readonly string[]): boolean =>
    values.some((value) => codes.includes(value));
  if (names.includes("TimeoutError") || has("ETIMEDOUT")) return "timeout";
  if (
    names.some((name) =>
      ["AbortError", "OperationAborted", "ConnectionAttemptSupersededError"].includes(name),
    )
  )
    return "cancelled";
  if (
    has(
      "TLS_CLIENT_IDENTITY_INVALID",
      "ERR_SSL_TLSV13_ALERT_CERTIFICATE_REQUIRED",
      "ERR_SSL_SSLV3_ALERT_BAD_CERTIFICATE",
      "ERR_SSL_TLSV1_ALERT_UNKNOWN_CA",
      "ERR_SSL_SSLV3_ALERT_CERTIFICATE_EXPIRED",
      "ERR_SSL_SSLV3_ALERT_CERTIFICATE_UNKNOWN",
      "ERR_OSSL_X509_KEY_VALUES_MISMATCH",
      "ERR_OSSL_BAD_DECRYPT",
      "ERR_OSSL_EVP_BAD_DECRYPT",
      "ERR_OSSL_PEM_BAD_PASSWORD_READ",
    )
  )
    return "tls-client";
  if (
    has(
      "CERT_HAS_EXPIRED",
      "DEPTH_ZERO_SELF_SIGNED_CERT",
      "ERR_TLS_CERT_ALTNAME_INVALID",
      "SELF_SIGNED_CERT_IN_CHAIN",
      "UNABLE_TO_GET_ISSUER_CERT_LOCALLY",
      "UNABLE_TO_VERIFY_LEAF_SIGNATURE",
    )
  )
    return "tls-trust";
  if (
    status.includes(401) ||
    (status.includes(400) && names.includes("OAuthEndpointResponseError")) ||
    has("SASL_AUTHENTICATION_FAILED", "SASL_AUTHENTICATION_ERROR", "UNSUPPORTED_SASL_MECHANISM")
  )
    return "authentication";
  if (status.includes(403) || codes.some((code) => code.endsWith("AUTHORIZATION_FAILED")))
    return "authorization";
  if (has("ECONNREFUSED", "ECONNRESET", "EHOSTUNREACH", "ENETUNREACH", "ENOTFOUND", "EAI_AGAIN"))
    return "unreachable";
  // Structured status/code wins over untrusted message fragments (which may contain usernames).
  if (/CERTIFICATE REQUIRED|BAD CERTIFICATE|KEY VALUES MISMATCH|BAD DECRYPT/u.test(text))
    return "tls-client";
  if (/CERTIFICATE|SELF.SIGNED|TLS|SSL/u.test(text)) return "tls-trust";
  if (/SASL.*AUTHENTICATION|AUTHENTICATION.*FAILED/u.test(text)) return "authentication";
  if (/AUTHORIZATION|NOT AUTHORIZED/u.test(text)) return "authorization";
  if (/CONNECTION REFUSED|ECONNREFUSED|ENOTFOUND|UNREACHABLE/u.test(text)) return "unreachable";
  return undefined;
}

export interface ConnectionDiagnostic {
  readonly code: HostErrorCode;
  readonly recovery: string;
  readonly retryable: boolean;
  readonly stage: HostErrorStage;
  readonly summary: string;
}

export function serviceConnectionDiagnostic(
  error: unknown,
  service: "Schema Registry" | "Kafka Connect" | "Redpanda Admin",
): ConnectionDiagnostic | undefined {
  switch (classifyConnectionFailure(error)) {
    case "authentication":
      return {
        code: "HTTPS_AUTHENTICATION",
        stage: "authorization",
        retryable: false,
        summary: `${service} authentication was rejected.`,
        recovery: `Check the authentication mode and credentials configured for ${service} in this profile, then retry.`,
      };
    case "authorization":
      return {
        code: "AUTHORIZATION_DENIED",
        stage: "authorization",
        retryable: false,
        summary: `${service} authorization was denied.`,
        recovery: `Grant this service identity the required ${service} permissions, then retry.`,
      };
    case "tls-client":
      return {
        code: "TLS_TRUST",
        stage: "tls",
        retryable: false,
        summary: `${service} client certificate authentication failed.`,
        recovery: `Check the ${service} client certificate, matching private key, key passphrase and server acceptance of its issuing CA.`,
      };
    case "tls-trust":
      return {
        code: "TLS_TRUST",
        stage: "tls",
        retryable: false,
        summary: `${service} certificate verification failed.`,
        recovery: `Check this service's trust settings, certificate validity and endpoint hostname. Configure its issuing CA in the ${service} settings.`,
      };
    case "cancelled":
      return {
        code: "CANCELLED",
        stage: "backend",
        retryable: true,
        summary: `The ${service} request was cancelled.`,
        recovery: "Retry after the connection change is complete.",
      };
    case "timeout":
      return {
        code: "TIMEOUT",
        stage: "backend",
        retryable: true,
        summary: `${service} did not respond before the deadline.`,
        recovery: "Verify the service endpoint and network path, then retry.",
      };
    case "unreachable":
      return {
        code: "BACKEND_UNAVAILABLE",
        stage: "backend",
        retryable: true,
        summary: `${service} could not be reached.`,
        recovery: "Verify the service endpoint, listener, DNS and network path, then retry.",
      };
    default:
      return undefined;
  }
}
