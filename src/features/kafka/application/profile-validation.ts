import {
  PROFILE_LIMITS,
  type ProfileCreateInput,
  type ProfileUpdateInput,
  type ClusterServiceEndpointInput,
  type ClusterServiceEndpointsInput,
} from "../contracts";

import { KafkaProfileRevisionError } from "./profile-errors";
import {
  kafkaProfileValidationInput,
  type KafkaProfileIssue,
  type KafkaProfileRecord,
} from "./profile-types";

export function normalizedName(name: string): string {
  return name.normalize("NFKC").trim().toLocaleLowerCase("en-US");
}

export function trustLabel(label: string): string {
  const segments = label.replaceAll("\\", "/").split("/");
  return segments.at(-1)?.trim() ?? "";
}

function validBroker(value: string): boolean {
  try {
    const url = new URL(`tcp://${value}`);
    return (
      url.hostname.length > 0 &&
      url.port.length > 0 &&
      Number(url.port) > 0 &&
      Number(url.port) <= 65_535 &&
      url.pathname === "" &&
      url.username.length === 0 &&
      url.password.length === 0
    );
  } catch {
    return false;
  }
}

function validHttpEndpoint(value: string): boolean {
  try {
    const url = new URL(value);
    return (
      (url.protocol === "http:" || url.protocol === "https:") &&
      url.hostname.length > 0 &&
      url.username.length === 0 &&
      url.password.length === 0 &&
      url.hash.length === 0 &&
      url.search.length === 0
    );
  } catch {
    return false;
  }
}

function canonicalServiceEndpoint(
  endpoint: ClusterServiceEndpointInput,
): ClusterServiceEndpointInput {
  const url = new URL(endpoint.baseUrl.trim());
  url.pathname = url.pathname.replace(/\/+$/u, "") || "/";
  const serialized = url.toString();
  return {
    authentication: endpoint.authentication,
    baseUrl: url.pathname === "/" ? serialized.slice(0, -1) : serialized,
  };
}

export function canonicalServices(
  services: ClusterServiceEndpointsInput | undefined,
): ClusterServiceEndpointsInput | undefined {
  if (services === undefined) {
    return undefined;
  }
  return {
    ...(services.redpandaAdmin === undefined
      ? {}
      : { redpandaAdmin: canonicalServiceEndpoint(services.redpandaAdmin) }),
    ...(services.schemaRegistry === undefined
      ? {}
      : { schemaRegistry: canonicalServiceEndpoint(services.schemaRegistry) }),
  };
}

export function createIssues(
  input: ProfileCreateInput | ProfileUpdateInput,
  retainAllowed = false,
): readonly KafkaProfileIssue[] {
  const issues: KafkaProfileIssue[] = [];
  if (input.name.trim().length === 0 || input.name.length > PROFILE_LIMITS.nameCharacters) {
    issues.push({
      field: "name",
      message: `Enter a name no longer than ${PROFILE_LIMITS.nameCharacters} characters.`,
    });
  }
  if (input.brokers.length === 0 || input.brokers.length > PROFILE_LIMITS.brokers) {
    issues.push({
      field: "brokers",
      message: `Enter between 1 and ${PROFILE_LIMITS.brokers} bootstrap brokers.`,
    });
  }
  input.brokers.forEach((broker, index) => {
    if (broker.length > PROFILE_LIMITS.brokerCharacters || !validBroker(broker.trim())) {
      issues.push({
        field: `brokers[${index}]`,
        message: "Enter a host and port, for example broker.example.test:9093.",
      });
    }
  });
  if (input.transport === "plaintext") {
    for (const field of ["apiCa", "binding", "trust"] as const) {
      if (Object.hasOwn(input, field)) {
        issues.push({
          field,
          message: "Plaintext profiles cannot contain broker trust or trust-retrieval values.",
        });
      }
    }
  } else if (input.trust === undefined) {
    issues.push({
      field: "trust",
      message: "TLS profiles require trust material.",
    });
  } else {
    if (trustLabel(input.trust.label).length === 0) {
      issues.push({ field: "trust.label", message: "Trust material must have a file label." });
    }
    if (
      input.trust.material.mode === "clear" ||
      (input.trust.material.mode === "retain" && !retainAllowed)
    ) {
      issues.push({
        field: "trust.material",
        message: "New profiles require trust material.",
      });
    }
    if (
      input.trust.kind === "pem" &&
      input.trust.password.mode !== "clear" &&
      !(retainAllowed && input.trust.password.mode === "retain")
    ) {
      issues.push({
        field: "trust.password",
        message: "PEM CA files do not use a truststore password.",
      });
    }
    if (
      input.trust.kind !== "pem" &&
      input.trust.password.mode !== "replace" &&
      input.trust.password.mode !== "acquired" &&
      !(retainAllowed && input.trust.password.mode === "retain")
    ) {
      issues.push({
        field: "trust.password",
        message: "JKS and PKCS12 truststores require a password.",
      });
    }
  }
  if (input.oauth !== undefined) {
    if (
      input.oauth.clientId.trim().length === 0 ||
      input.oauth.clientId.length > PROFILE_LIMITS.clientIdCharacters
    ) {
      issues.push({ field: "oauth.clientId", message: "OAuth client ID is required." });
    }
    if (
      input.oauth.clientSecret.mode !== "replace" &&
      !(retainAllowed && input.oauth.clientSecret.mode === "retain")
    ) {
      issues.push({
        field: "oauth.clientSecret",
        message: "New OAuth profiles require a client secret.",
      });
    }
    if (input.oauth.scope.length > PROFILE_LIMITS.scopeCharacters) {
      issues.push({ field: "oauth.scope", message: "OAuth scope exceeds the supported length." });
    }
    if (
      input.oauth.tokenEndpoint.length > PROFILE_LIMITS.tokenEndpointCharacters ||
      !validHttpEndpoint(input.oauth.tokenEndpoint.trim())
    ) {
      issues.push({
        field: "oauth.tokenEndpoint",
        message: "Enter an HTTP or HTTPS OAuth token endpoint.",
      });
    }
  }
  const services = [
    ["redpandaAdmin", input.services?.redpandaAdmin],
    ["schemaRegistry", input.services?.schemaRegistry],
  ] as const;
  for (const [serviceName, service] of services) {
    if (service === undefined) {
      continue;
    }
    if (
      service.baseUrl.length > PROFILE_LIMITS.tokenEndpointCharacters ||
      !validHttpEndpoint(service.baseUrl.trim())
    ) {
      issues.push({
        field: `services.${serviceName}.baseUrl`,
        message: "Enter an HTTP or HTTPS service base URL without credentials, query, or fragment.",
      });
    }
    if (service.authentication === "oauth" && input.oauth === undefined) {
      issues.push({
        field: `services.${serviceName}.authentication`,
        message: "OAuth service authentication requires complete profile OAuth settings.",
      });
    }
  }
  return issues;
}

function canonicalTimestamp(value: string): boolean {
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) && new Date(timestamp).toISOString() === value;
}

export function validStoredRecords(records: readonly KafkaProfileRecord[]): boolean {
  const ids = new Set<string>();
  const names = new Set<string>();
  for (const record of records) {
    const name = normalizedName(record.name);
    const plaintext = record.transport === "plaintext";
    if (
      (record.revision !== undefined &&
        (!Number.isSafeInteger(record.revision) || record.revision < 1)) ||
      (Object.hasOwn(record, "transport") &&
        record.transport !== "tls" &&
        record.transport !== "plaintext") ||
      record.id.trim().length === 0 ||
      record.id !== record.id.trim() ||
      ids.has(record.id) ||
      names.has(name) ||
      record.name !== record.name.normalize("NFKC").trim() ||
      (plaintext &&
        (Object.hasOwn(record, "trust") ||
          Object.hasOwn(record, "binding") ||
          Object.hasOwn(record, "apiCaPem"))) ||
      (!plaintext &&
        (record.trust === undefined || record.trust.label !== trustLabel(record.trust.label))) ||
      record.brokers.some((broker) => broker !== broker.trim()) ||
      !canonicalTimestamp(record.createdAt) ||
      !canonicalTimestamp(record.updatedAt) ||
      record.createdAt > record.updatedAt ||
      createIssues(kafkaProfileValidationInput(record)).length > 0
    ) {
      return false;
    }
    ids.add(record.id);
    names.add(name);
  }
  return true;
}

export function assertProfileRevision(
  existing: KafkaProfileRecord,
  expected: number | undefined,
): void {
  const revision = existing.revision ?? 1;
  if (
    revision >= Number.MAX_SAFE_INTEGER ||
    (existing.revision !== undefined && expected === undefined) ||
    (expected !== undefined && expected !== revision)
  )
    throw new KafkaProfileRevisionError();
}
