import { describe, expect, it } from "vitest";

import {
  classifyConnectionFailure,
  serviceConnectionDiagnostic,
} from "../../src/features/kafka/application/connection-diagnostics";
import { mapKafkaAdminFailure } from "../../src/features/kafka/engine/failure";

describe("connection diagnostics", () => {
  it.each([
    ["SASL_AUTHENTICATION_FAILED", "KAFKA_AUTHENTICATION", "SASL mechanism"],
    ["UNSUPPORTED_SASL_MECHANISM", "KAFKA_AUTHENTICATION", "SASL mechanism"],
    ["CERT_HAS_EXPIRED", "TLS_TRUST", "validity"],
    ["ERR_TLS_CERT_ALTNAME_INVALID", "TLS_TRUST", "hostname"],
    ["ERR_SSL_TLSV13_ALERT_CERTIFICATE_REQUIRED", "TLS_TRUST", "private key"],
    ["ERR_OSSL_X509_KEY_VALUES_MISMATCH", "TLS_TRUST", "private key"],
    ["ERR_OSSL_BAD_DECRYPT", "TLS_TRUST", "passphrase"],
    ["ENOTFOUND", "BROKER_UNREACHABLE", "network path"],
  ])("classifies nested %s without publishing upstream secrets", (code, expected, recovery) => {
    const upstream = Object.assign(new Error("remote-secret-fixture"), { code });
    const failure = mapKafkaAdminFailure(
      new AggregateError([new Error("wrapper", { cause: upstream })]),
      "broker",
    );
    expect(failure.code).toBe(expected);
    expect(failure.recovery).toContain(recovery);
    expect(JSON.stringify(failure)).not.toContain("remote-secret-fixture");
    expect(failure.message).not.toContain("remote-secret-fixture");
  });

  it.each(["Schema Registry", "Kafka Connect"] as const)(
    "distinguishes rejected credentials from missing permissions for %s",
    (service) => {
      const authentication = serviceConnectionDiagnostic(
        Object.assign(new Error("secret-password-fixture"), { status: 401 }),
        service,
      );
      const authorization = serviceConnectionDiagnostic(
        Object.assign(new Error("secret-token-fixture"), { status: 403 }),
        service,
      );
      expect(authentication).toMatchObject({ code: "HTTPS_AUTHENTICATION", retryable: false });
      expect(authentication?.recovery).toContain("credentials");
      expect(authorization).toMatchObject({ code: "AUTHORIZATION_DENIED", retryable: false });
      expect(authorization?.recovery).toContain("permissions");
      expect(JSON.stringify([authentication, authorization])).not.toContain("secret-");
    },
  );

  it("preserves TLS/cancellation evidence in service requests and bounds cyclic error inspection", () => {
    const error = Object.assign(new Error("private-key-fixture"), {
      code: "ERR_SSL_TLSV13_ALERT_CERTIFICATE_REQUIRED",
    });
    Object.assign(error, { cause: error });
    expect(serviceConnectionDiagnostic(error, "Schema Registry")).toMatchObject({
      code: "TLS_TRUST",
      stage: "tls",
      summary: "Schema Registry client certificate authentication failed.",
    });
    expect(classifyConnectionFailure(new DOMException("secret", "AbortError"))).toBe("cancelled");
    expect(classifyConnectionFailure(new DOMException("secret", "TimeoutError"))).toBe("timeout");
  });

  it("prefers structured authentication evidence over untrusted message fragments", () => {
    expect(
      classifyConnectionFailure(
        Object.assign(new Error("Rejected certificate-user"), { status: 401 }),
      ),
    ).toBe("authentication");
    expect(
      classifyConnectionFailure(
        Object.assign(new Error("TLS-user denied"), { code: "SASL_AUTHENTICATION_FAILED" }),
      ),
    ).toBe("authentication");
    expect(
      serviceConnectionDiagnostic(
        Object.assign(new Error("token-secret-fixture"), {
          name: "OAuthEndpointResponseError",
          status: 400,
        }),
        "Kafka Connect",
      ),
    ).toMatchObject({ code: "HTTPS_AUTHENTICATION", retryable: false });
  });
});
