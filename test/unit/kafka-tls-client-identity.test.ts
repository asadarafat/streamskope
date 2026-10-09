import { createPrivateKey } from "node:crypto";

import { describe, expect, it } from "vitest";

import {
  classifyConnectionFailure,
  serviceConnectionDiagnostic,
} from "../../src/features/kafka/application/connection-diagnostics";
import { mapKafkaAdminFailure } from "../../src/features/kafka/engine/failure";
import { tlsClientIdentityOptions } from "../../src/features/kafka/engine/tls-client-identity";
import { encryptedClientKey, undecodableClientKey } from "../support/tls-client-identity-fixture";

describe("TLS client identity diagnostics", () => {
  it("preserves a valid encrypted identity and does not invent an absent identity", () => {
    const identity = {
      certificatePem: "certificate-fixture",
      privateKeyPem: encryptedClientKey("private-passphrase-fixture"),
      passphrase: "private-passphrase-fixture",
    };
    expect(tlsClientIdentityOptions(undefined)).toEqual({});
    expect(tlsClientIdentityOptions(identity)).toEqual({
      cert: identity.certificatePem,
      key: identity.privateKeyPem,
      passphrase: identity.passphrase,
    });
  });

  it("reports an actual OpenSSL decoder failure as an identity error only in identity context", () => {
    const passphrase = "private-passphrase-fixture";
    const privateKeyPem = undecodableClientKey(passphrase);
    let nativeError: unknown;
    try {
      createPrivateKey({ key: privateKeyPem, passphrase });
    } catch (error) {
      nativeError = error;
    }
    expect(nativeError).toMatchObject({ code: "ERR_OSSL_UNSUPPORTED" });
    expect(classifyConnectionFailure(nativeError)).toBeUndefined();

    let identityError: unknown;
    try {
      tlsClientIdentityOptions({
        certificatePem: "certificate-fixture",
        privateKeyPem,
        passphrase,
      });
    } catch (error) {
      identityError = error;
    }
    expect(identityError).toMatchObject({
      code: "TLS_CLIENT_IDENTITY_INVALID",
      cause: { code: "ERR_OSSL_UNSUPPORTED" },
    });
    const nested = new AggregateError([new Error("Transport wrapper", { cause: identityError })]);
    const broker = mapKafkaAdminFailure(nested, "broker");
    expect(broker).toMatchObject({ code: "TLS_TRUST", stage: "tls", retryable: false });
    expect(broker.recovery).toContain("key passphrase");
    const diagnostics = [
      broker,
      ...(["Schema Registry", "Kafka Connect"] as const).map((service) => {
        const diagnostic = serviceConnectionDiagnostic(nested, service);
        expect(diagnostic).toMatchObject({ code: "TLS_TRUST", stage: "tls", retryable: false });
        expect(diagnostic?.recovery).toContain("key passphrase");
        return diagnostic;
      }),
    ];
    const visible = JSON.stringify(diagnostics);
    expect(visible).not.toContain(passphrase);
    expect(visible).not.toContain(JSON.stringify(privateKeyPem).slice(1, -1));
    expect(visible).not.toContain("DECODER routines");
  });
});
