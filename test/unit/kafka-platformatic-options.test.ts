import { describe, expect, it } from "vitest";

import { platformaticClientOptions } from "../../src/features/kafka/engine/platformatic-options";

describe("Platformatic Kafka client options", () => {
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
