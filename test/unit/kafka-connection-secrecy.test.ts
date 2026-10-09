import { describe, expect, it } from "vitest";

import type { SecureConnectionInput } from "../../src/features/kafka/contracts";
import { serviceConnectionDiagnostic } from "../../src/features/kafka/application/connection-diagnostics";
import { KafkaEngineFailure, mapKafkaAdminFailure } from "../../src/features/kafka/engine/failure";
import {
  sensitiveValues,
  translateFacadeFailure,
} from "../../src/features/kafka/facade/facade-support";
import { ActivityHistory } from "../../src/platform/activity";

const connection: SecureConnectionInput = {
  name: "Credentials fixture",
  brokers: ["broker.example:9093"],
  sasl: { mechanism: "PLAIN", username: "user", password: "broker-password-fixture" },
  tls: {
    enabled: true,
    caPem: "broker-ca-fixture",
    clientIdentity: {
      certificatePem: "client-certificate-fixture",
      privateKeyPem: "private-key-fixture",
      passphrase: "key-passphrase-fixture",
    },
  },
  services: {
    schemaRegistry: {
      baseUrl: "https://registry.example",
      authentication: "basic",
      basic: { username: "registry-user", password: "registry-password-fixture" },
    },
    connect: {
      baseUrl: "https://connect.example",
      authentication: "bearer",
      bearer: "bearer-fixture",
    },
  },
};

function publishedFailure(failure: Error): string {
  const translated = translateFacadeFailure(
    failure,
    { connection, activeStateChanged: false, correlationId: "fixture-correlation" },
    true,
  );
  const activity = new ActivityHistory(3);
  activity.record(
    {
      id: "failure",
      correlationId: "fixture-correlation",
      detail: translated.detail,
      object: connection.name,
      operation: "Connection test",
      outcome: "failed",
      severity: "error",
      timestamp: "2026-10-09T00:00:00.000Z",
    },
    sensitiveValues(connection),
  );
  return JSON.stringify({ response: translated.error, activity: activity.entries() });
}

describe("connection error publication", () => {
  it.each([
    "SASL_AUTHENTICATION_FAILED",
    "ERR_TLS_CERT_ALTNAME_INVALID",
    "ERR_OSSL_X509_KEY_VALUES_MISMATCH",
    "INTERNAL_FAILURE",
  ])("keeps arbitrary upstream secrets private for %s", (code) => {
    const upstream = Object.assign(
      new Error(
        "broker-password-fixture private-key-fixture key-passphrase-fixture untrusted-certificate-subject.example dynamic-oauth-token-without-a-label",
      ),
      { code },
    );
    const result = publishedFailure(mapKafkaAdminFailure(upstream, "broker.example:9093"));
    for (const secret of [
      "broker-password-fixture",
      "private-key-fixture",
      "key-passphrase-fixture",
      "untrusted-certificate-subject.example",
      "dynamic-oauth-token-without-a-label",
    ])
      expect(result).not.toContain(secret);
    expect(result).toContain("fixture-correlation");
    expect(result).toContain("broker.example:9093");
  });

  it.each([401, 403])("does not publish raw HTTP %s credentials or response text", (status) => {
    const upstream = Object.assign(
      new Error("registry-password-fixture bearer-fixture dynamic-oauth-token-without-a-label"),
      { status },
    );
    const diagnostic = serviceConnectionDiagnostic(upstream, "Schema Registry");
    if (diagnostic === undefined) throw new Error("The service rejection was not classified.");
    const result = publishedFailure(
      new KafkaEngineFailure({ ...diagnostic, cause: upstream, target: "schemaRegistry" }),
    );
    for (const secret of [
      "registry-password-fixture",
      "bearer-fixture",
      "dynamic-oauth-token-without-a-label",
    ])
      expect(result).not.toContain(secret);
  });

  it("also redacts configured credentials and encoded Basic authorization from unclassified host exceptions", () => {
    const encoded = Buffer.from("registry-user:registry-password-fixture").toString("base64");
    const result = publishedFailure(
      new Error(
        `broker-password-fixture private-key-fixture key-passphrase-fixture bearer-fixture ${encoded}`,
      ),
    );
    for (const secret of [
      "broker-password-fixture",
      "private-key-fixture",
      "key-passphrase-fixture",
      "bearer-fixture",
      encoded,
    ])
      expect(result).not.toContain(secret);
  });
});
