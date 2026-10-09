import { describe, expect, it } from "vitest";

import { platformaticClientOptions } from "../../src/features/kafka/engine/platformatic-options";

describe("Platformatic Kafka client options", () => {
  it.each(["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] as const)(
    "uses %s with explicit mTLS identity while preserving strict verification",
    (mechanism) => {
      const result = platformaticClientOptions(
        {
          brokers: ["broker.example:9093"],
          operationTimeoutMs: 5000,
          caPem: "broker-ca",
          sasl: { mechanism, username: "fixture-user", password: "fixture-password" },
          clientIdentity: {
            certificatePem: "client-cert",
            privateKeyPem: "client-key",
            passphrase: "key-password",
          },
        },
        "test-client",
      );
      expect(result.sasl).toEqual({
        mechanism,
        username: "fixture-user",
        password: "fixture-password",
      });
      expect(result.tls).toEqual({
        ca: ["broker-ca"],
        cert: "client-cert",
        key: "client-key",
        passphrase: "key-password",
        rejectUnauthorized: true,
      });
    },
  );

  it("rejects ambiguous authentication instead of silently ignoring a credential", () => {
    expect(() =>
      platformaticClientOptions(
        {
          brokers: ["broker.example:9092"],
          operationTimeoutMs: 5000,
          tlsEnabled: false,
          sasl: { mechanism: "PLAIN", username: "fixture", password: "fixture" },
          oauthTokenProvider: () => Promise.resolve({ value: "token" }),
        },
        "test-client",
      ),
    ).toThrow("Choose one Kafka SASL authentication mechanism.");
  });
  it("omits TLS for plaintext and keeps strict CA verification for TLS", () => {
    const common = {
      brokers: ["127.0.0.1:19092"],
      operationTimeoutMs: 5_000,
    };

    expect(
      platformaticClientOptions(
        {
          ...common,
          tlsEnabled: false,
        },
        "streamskope-plaintext-test",
      ),
    ).not.toHaveProperty("tls");
    expect(
      platformaticClientOptions(
        {
          ...common,
          caPem: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
        },
        "streamskope-tls-test",
      ),
    ).toMatchObject({
      tls: {
        ca: ["-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----"],
        rejectUnauthorized: true,
      },
    });
  });
});
