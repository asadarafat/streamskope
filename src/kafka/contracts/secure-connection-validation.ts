import type { HostSecureConnectionInput, SecureConnectionIssue } from "./types";

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
  if ("caPem" in connection.tls && connection.tls.caPem.trim().length === 0) {
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
  if (connection.oauth !== undefined) {
    if (connection.oauth.tokenEndpoint.trim().length === 0) {
      issues.push({
        field: "oauth.tokenEndpoint",
        message: "OAuth token endpoint is required.",
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
    if (connection.oauth.scope.trim().length === 0) {
      issues.push({
        field: "oauth.scope",
        message: "OAuth scope is required.",
      });
    }
  }
  const services = [
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
    if (service.authentication === "oauth" && connection.oauth === undefined) {
      issues.push({
        field: `services.${serviceName}.authentication`,
        message: "OAuth service authentication requires complete profile OAuth settings.",
      });
    }
  }
  return issues;
}
