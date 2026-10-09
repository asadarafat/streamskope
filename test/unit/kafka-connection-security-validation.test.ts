import { describe, expect, it } from "vitest";

import type {
  HostSecureConnectionInput,
  ResolvedClusterServiceEndpoint,
} from "../../src/features/kafka/contracts";
import { validateSecureConnectionInput } from "../../src/features/kafka/contracts/secure-connection-validation";

const broker = {
  name: "Fixture",
  brokers: ["broker.example:9093"],
  tls: { enabled: true, caPem: "ca" },
} as const;
const oauth = {
  clientId: "client",
  clientSecret: "secret-value-fixture",
  scope: "",
  tokenEndpoint: "https://identity.example/token",
};
const identity = { certificatePem: "certificate", privateKeyPem: "private-key" };
const withService = (service: ResolvedClusterServiceEndpoint): HostSecureConnectionInput => ({
  ...broker,
  services: { schemaRegistry: service },
});

describe("resolved production connection admission", () => {
  it.each(["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] as const)(
    "accepts %s with mutual TLS and rejects an ambiguous OAuth credential",
    (mechanism) => {
      const connection = {
        ...broker,
        sasl: { mechanism, username: "user", password: "password" },
        tls: { ...broker.tls, clientIdentity: identity },
      };
      expect(validateSecureConnectionInput(connection)).toEqual([]);
      expect(validateSecureConnectionInput({ ...connection, oauth })).toContainEqual(
        expect.objectContaining({ field: "sasl" }),
      );
      expect(
        validateSecureConnectionInput({
          ...connection,
          sasl: { ...connection.sasl, password: "" },
        }),
      ).toContainEqual(expect.objectContaining({ field: "sasl.password" }));
    },
  );

  it("preserves legacy inherited service OAuth and permits independent service authentication with no broker OAuth", () => {
    expect(
      validateSecureConnectionInput({
        ...broker,
        oauth,
        services: {
          schemaRegistry: { baseUrl: "https://registry.example", authentication: "oauth" },
        },
      }),
    ).toEqual([]);
    expect(
      validateSecureConnectionInput(
        withService({
          baseUrl: "https://registry.example",
          authentication: "oauth-client",
          oauth,
          tls: { caPem: "registry-ca", clientIdentity: identity },
        }),
      ),
    ).toEqual([]);
    expect(
      validateSecureConnectionInput(
        withService({ baseUrl: "https://registry.example", authentication: "oauth" }),
      ),
    ).toContainEqual(expect.objectContaining({ field: "services.schemaRegistry.authentication" }));
  });

  it.each([
    {
      authentication: "basic",
      basic: { username: "colon:user", password: "secret-value-fixture" },
    },
    { authentication: "bearer", bearer: "token\r\nX-Injected: secret-value-fixture" },
    { authentication: "none", bearer: "secret-value-fixture" },
    {
      authentication: "oauth-client",
      oauth: {
        ...oauth,
        tokenEndpoint: "https://user:secret-value-fixture@identity.example/token",
      },
    },
  ] as const)(
    "rejects malformed or contradictory service credentials: $authentication",
    (service) => {
      const issues = validateSecureConnectionInput(
        withService({ ...service, baseUrl: "https://registry.example" }),
      );
      expect(issues.length).toBeGreaterThan(0);
      expect(JSON.stringify(issues)).not.toContain("secret-value-fixture");
    },
  );

  it("refuses silently ignored TLS credentials on plaintext and incomplete client identities", () => {
    expect(
      validateSecureConnectionInput({
        ...broker,
        tls: { enabled: false, clientIdentity: identity },
      } as unknown as HostSecureConnectionInput),
    ).toContainEqual(expect.objectContaining({ field: "tls.enabled" }));
    expect(
      validateSecureConnectionInput({
        ...broker,
        tls: { ...broker.tls, clientIdentity: { ...identity, privateKeyPem: "" } },
      }),
    ).toContainEqual(expect.objectContaining({ field: "tls.clientIdentity" }));
    expect(
      validateSecureConnectionInput(
        withService({
          baseUrl: "http://registry.example",
          authentication: "none",
          tls: { clientIdentity: identity },
        }),
      ),
    ).toContainEqual(expect.objectContaining({ field: "services.schemaRegistry.tls" }));
    expect(
      validateSecureConnectionInput(
        withService({
          baseUrl: "https://registry.example",
          authentication: "oauth-client",
          oauth: { ...oauth, tokenEndpoint: "http://identity.example/token" },
          tls: { clientIdentity: identity },
        }),
      ),
    ).toContainEqual(expect.objectContaining({ field: "services.schemaRegistry.oauth" }));
  });

  it.each(["\t", "\u0001", "\u001f", "\u007f"])(
    "rejects control characters in direct HTTP Basic usernames",
    (control) => {
      const issues = validateSecureConnectionInput(
        withService({
          baseUrl: "https://registry.example",
          authentication: "basic",
          basic: { username: `user${control}name`, password: "secret-value-fixture" },
        }),
      );
      expect(issues).toContainEqual(
        expect.objectContaining({ field: "services.schemaRegistry.basic" }),
      );
      expect(JSON.stringify(issues)).not.toContain("secret-value-fixture");
    },
  );

  it.each([" ", "\t", "\u0001", "\u001f", "\u007f"])(
    "rejects whitespace and control characters in direct bearer tokens",
    (invalid) => {
      const issues = validateSecureConnectionInput(
        withService({
          baseUrl: "https://registry.example",
          authentication: "bearer",
          bearer: `secret${invalid}value-fixture`,
        }),
      );
      expect(issues).toContainEqual({
        field: "services.schemaRegistry.bearer",
        message: "Enter a bearer token without whitespace or control characters.",
      });
      expect(JSON.stringify(issues)).not.toContain("value-fixture");
    },
  );
});
