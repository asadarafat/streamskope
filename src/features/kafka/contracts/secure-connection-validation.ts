import type { HostSecureConnectionInput, SecureConnectionIssue } from "./types";
import type { ConnectionClientIdentity } from "./connection-security";
import { CLUSTER_SERVICE_AUTHENTICATION_MODES, KAFKA_SASL_MECHANISMS } from "./profile-types";

function isValidServiceEndpoint(value: string): boolean {
  try {
    const url = new URL(value);
    return (
      (url.protocol === "http:" || url.protocol === "https:") &&
      url.hostname.length > 0 &&
      url.username.length === 0 &&
      url.password.length === 0 &&
      url.search.length === 0 &&
      url.hash.length === 0
    );
  } catch {
    return false;
  }
}

function isValidOAuthEndpoint(value: string): boolean {
  try {
    const url = new URL(value);
    return (
      ["http:", "https:"].includes(url.protocol) &&
      url.hostname.length > 0 &&
      !url.username &&
      !url.password &&
      !url.hash
    );
  } catch {
    return false;
  }
}

function isHttpsEndpoint(value: string): boolean {
  try {
    return new URL(value).protocol === "https:";
  } catch {
    return false;
  }
}

function completeIdentity(identity: ConnectionClientIdentity): boolean {
  return identity.certificatePem.trim().length > 0 && identity.privateKeyPem.trim().length > 0;
}

export function validateSecureConnectionInput(
  connection: HostSecureConnectionInput,
): readonly SecureConnectionIssue[] {
  const issues: SecureConnectionIssue[] = [];
  if (connection.name.trim().length === 0) {
    issues.push({ field: "name", message: "Connection name is required." });
  }
  if (connection.brokers.length === 0) {
    issues.push({
      field: "brokers",
      message: "Enter at least one bootstrap broker.",
    });
  }
  if (connection.tls.enabled !== true && connection.tls.enabled !== false) {
    issues.push({
      field: "tls.enabled",
      message: "Choose TLS or plaintext explicitly.",
    });
  } else if (
    connection.tls.enabled === false &&
    (Object.hasOwn(connection.tls, "caPem") ||
      Object.hasOwn(connection.tls, "acquisitionId") ||
      Object.hasOwn(connection.tls, "clientIdentity"))
  ) {
    issues.push({
      field: "tls.enabled",
      message: "Plaintext connections cannot contain TLS trust or a client certificate.",
    });
  } else if (
    connection.tls.enabled === true &&
    !Object.hasOwn(connection.tls, "caPem") &&
    !Object.hasOwn(connection.tls, "acquisitionId")
  ) {
    issues.push({
      field: "tls.caPem",
      message: "Select trusted CA material for TLS.",
    });
  }
  if (
    "caPem" in connection.tls &&
    (typeof connection.tls.caPem !== "string" || connection.tls.caPem.trim().length === 0)
  ) {
    issues.push({
      field: "tls.caPem",
      message: "Select a trusted PEM CA certificate.",
    });
  }
  if ("acquisitionId" in connection.tls && connection.tls.acquisitionId.trim().length === 0) {
    issues.push({
      field: "tls.acquisitionId",
      message: "Acquire remote trust before using it.",
    });
  }
  if (
    "clientIdentity" in connection.tls &&
    connection.tls.clientIdentity !== undefined &&
    !completeIdentity(connection.tls.clientIdentity)
  )
    issues.push({
      field: "tls.clientIdentity",
      message: "Supply both the client certificate and matching private key for mutual TLS.",
    });
  if (connection.sasl !== undefined) {
    if (connection.oauth !== undefined)
      issues.push({
        field: "sasl",
        message: "Choose either OAuth or a username/password SASL mechanism, not both.",
      });
    if (!KAFKA_SASL_MECHANISMS.includes(connection.sasl.mechanism))
      issues.push({
        field: "sasl.mechanism",
        message: "Choose PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512.",
      });
    if (!connection.sasl.username.trim() || connection.sasl.username.includes("\0"))
      issues.push({
        field: "sasl.username",
        message: "Enter a SASL username without NUL characters.",
      });
    if (!connection.sasl.password || connection.sasl.password.includes("\0"))
      issues.push({
        field: "sasl.password",
        message: "Enter a SASL password without NUL characters.",
      });
  }
  if (connection.oauth !== undefined) {
    if (connection.oauth.tokenEndpoint.trim().length === 0) {
      issues.push({
        field: "oauth.tokenEndpoint",
        message: "OAuth token endpoint is required.",
      });
    } else if (!isValidOAuthEndpoint(connection.oauth.tokenEndpoint)) {
      issues.push({
        field: "oauth.tokenEndpoint",
        message:
          "Enter an HTTP or HTTPS OAuth token endpoint without URL credentials or a fragment.",
      });
    }
    if (connection.oauth.clientId.trim().length === 0) {
      issues.push({
        field: "oauth.clientId",
        message: "OAuth client ID is required.",
      });
    }
    if (connection.oauth.clientSecret.length === 0) {
      issues.push({
        field: "oauth.clientSecret",
        message: "OAuth client secret is required.",
      });
    }
  }
  const services = [
    ["connect", connection.services?.connect],
    ["redpandaAdmin", connection.services?.redpandaAdmin],
    ["schemaRegistry", connection.services?.schemaRegistry],
  ] as const;
  for (const [serviceName, service] of services) {
    if (service === undefined) {
      continue;
    }
    if (!isValidServiceEndpoint(service.baseUrl)) {
      issues.push({
        field: `services.${serviceName}.baseUrl`,
        message: "Enter an HTTP or HTTPS service base URL without credentials, query, or fragment.",
      });
    }
    if (!CLUSTER_SERVICE_AUTHENTICATION_MODES.includes(service.authentication)) {
      issues.push({
        field: `services.${serviceName}.authentication`,
        message: "Choose a supported service authentication mode.",
      });
    }
    if (service.authentication === "oauth" && connection.oauth === undefined) {
      issues.push({
        field: `services.${serviceName}.authentication`,
        message: "OAuth service authentication requires complete profile OAuth settings.",
      });
    }
    for (const [mode, key] of [
      ["basic", "basic"],
      ["bearer", "bearer"],
      ["oauth-client", "oauth"],
    ] as const) {
      if ((service.authentication === mode) !== (service[key] !== undefined))
        issues.push({
          field: `services.${serviceName}.authentication`,
          message:
            "Supply only the credentials required by the selected service authentication mode.",
        });
    }
    if (
      service.basic !== undefined &&
      (!service.basic.username.trim() ||
        // eslint-disable-next-line no-control-regex -- Reject ASCII controls in HTTP credentials.
        /[:\u0000-\u001f\u007f]/u.test(service.basic.username) ||
        !service.basic.password)
    )
      issues.push({
        field: `services.${serviceName}.basic`,
        message: "Enter a Basic username without colon/control characters and a password.",
      });
    if (
      service.bearer !== undefined &&
      // eslint-disable-next-line no-control-regex -- Reject whitespace and ASCII controls in bearer credentials.
      (!service.bearer.trim() || /[\u0000-\u0020\u007f]/u.test(service.bearer))
    )
      issues.push({
        field: `services.${serviceName}.bearer`,
        message: "Enter a bearer token without whitespace or control characters.",
      });
    if (
      service.oauth !== undefined &&
      (!service.oauth.clientId.trim() ||
        !service.oauth.clientSecret ||
        !isValidOAuthEndpoint(service.oauth.tokenEndpoint))
    )
      issues.push({
        field: `services.${serviceName}.oauth`,
        message:
          "Complete this service's OAuth client ID, secret and HTTP/HTTPS token endpoint without URL credentials.",
      });
    if (service.tls !== undefined) {
      if (
        !isHttpsEndpoint(service.baseUrl) &&
        (service.tls.caPem !== undefined || service.tls.clientIdentity !== undefined)
      )
        issues.push({
          field: `services.${serviceName}.tls`,
          message: "Custom trust and client certificates require an HTTPS service endpoint.",
        });
      if (service.tls.caPem !== undefined && !service.tls.caPem.trim())
        issues.push({
          field: `services.${serviceName}.tls`,
          message: "Supply the service CA certificate or select system trust.",
        });
      if (service.tls.clientIdentity !== undefined && !completeIdentity(service.tls.clientIdentity))
        issues.push({
          field: `services.${serviceName}.tls`,
          message:
            "Supply both the service client certificate and matching private key for mutual TLS.",
        });
      if (
        service.tls.clientIdentity !== undefined &&
        service.oauth !== undefined &&
        !isHttpsEndpoint(service.oauth.tokenEndpoint)
      )
        issues.push({
          field: `services.${serviceName}.oauth`,
          message: "Service OAuth with a client certificate requires an HTTPS token endpoint.",
        });
    }
  }
  return issues;
}
