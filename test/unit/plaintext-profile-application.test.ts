import { describe, expect, it } from "vitest";

import type {
  ProfileCreateInput,
  ProfileStoreCapability,
  ProfileUpdateInput,
} from "../../src/features/kafka/contracts";
import {
  ActiveKafkaProfileMutationError,
  KafkaProfileStoreUnavailableError,
  KafkaProfileValidationError,
} from "../../src/features/kafka/application/profile-errors";
import { InMemoryKafkaProfileStore } from "../../src/features/kafka/application/in-memory-profile-store";
import { KafkaProfileService } from "../../src/features/kafka/application/profile-service";
import type {
  KafkaProfileRecord,
  KafkaProfileTrustDecoder,
} from "../../src/features/kafka/application/profile-types";

type TlsProfileRecord = Extract<KafkaProfileRecord, { readonly transport?: "tls" }>;

const capability: ProfileStoreCapability = {
  durability: "session",
  protection: "memory",
  state: "ready",
};

class AcceptingTrustDecoder implements KafkaProfileTrustDecoder {
  readonly calls: Array<{
    readonly kind: "jks" | "pem" | "pkcs12";
    readonly material: string;
    readonly password?: string;
  }> = [];

  decode(input: {
    readonly kind: "jks" | "pem" | "pkcs12";
    readonly material: string;
    readonly password?: string;
  }): Promise<{ readonly caPem: string; readonly kind: "jks" | "pem" | "pkcs12" }> {
    this.calls.push(input);
    return Promise.resolve({
      caPem: "-----BEGIN CERTIFICATE-----\nvalidated\n-----END CERTIFICATE-----",
      kind: input.kind,
    });
  }
}

function tlsRecord(id: string, name: string): TlsProfileRecord {
  return {
    brokers: [`${id}.example.test:9093`],
    createdAt: "2026-09-17T18:00:00.000Z",
    id,
    name,
    trust: {
      kind: "pem",
      label: `${id}.pem`,
      material: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
    },
    updatedAt: "2026-09-17T18:00:00.000Z",
  };
}

describe("plaintext Kafka profile application", () => {
  it("resolves a plaintext draft without decoding broker trust", async () => {
    const store = new InMemoryKafkaProfileStore(capability);
    const decoder = new AcceptingTrustDecoder();
    const service = new KafkaProfileService(store, decoder);

    await expect(
      service
        .resolveTestContext({
          mode: "create",
          profile: {
            brokers: ["127.0.0.1:19092"],
            name: "Plaintext lab",
            transport: "plaintext",
          },
        })
        .then((context) => context.connection),
    ).resolves.toEqual({
      brokers: ["127.0.0.1:19092"],
      name: "Plaintext lab",
      tls: { enabled: false },
    });
    expect(decoder.calls).toEqual([]);
    expect(store.commitCount).toBe(0);
  });

  it("normalizes a legacy record to TLS without changing stored bytes", async () => {
    const legacy = tlsRecord("legacy-profile", "Legacy TLS");
    const store = new InMemoryKafkaProfileStore(capability, [legacy]);
    const service = new KafkaProfileService(store, new AcceptingTrustDecoder());

    await expect(service.list()).resolves.toMatchObject({
      profiles: [{ id: legacy.id, transport: "tls" }],
    });
    expect(store.records()[0]).not.toHaveProperty("transport");
    await expect(service.resolveConnection(legacy.id)).resolves.toMatchObject({
      tls: { enabled: true },
    });
  });

  it.each([
    ["unknown", "udp"],
    ["explicit undefined", undefined],
  ])("fails closed when a store adapter returns %s transport", async (_label, transport) => {
    const malformed = {
      ...tlsRecord("invalid-transport", "Invalid transport"),
      transport,
    } as unknown as KafkaProfileRecord;
    const decoder = new AcceptingTrustDecoder();
    const service = new KafkaProfileService(
      new InMemoryKafkaProfileStore(capability, [malformed]),
      decoder,
    );

    await expect(service.list()).rejects.toBeInstanceOf(KafkaProfileStoreUnavailableError);
    expect(decoder.calls).toEqual([]);
    expect(service.currentSnapshot()).toMatchObject({
      profiles: [],
      store: { state: "unavailable" },
    });
  });

  it("creates and resolves plaintext OAuth and services without broker trust", async () => {
    const store = new InMemoryKafkaProfileStore(capability);
    const decoder = new AcceptingTrustDecoder();
    const service = new KafkaProfileService(store, decoder, {
      createId: (): string => "plaintext-profile",
      now: (): Date => new Date("2026-09-17T18:00:00.000Z"),
    });
    const input: ProfileCreateInput = {
      brokers: ["127.0.0.1:19092"],
      name: "Plaintext OAuth",
      oauth: {
        clientId: "plain-client",
        clientSecret: { mode: "replace", value: "plain-secret" },
        scope: "kafka",
        tokenEndpoint: "https://identity.example.test/token",
      },
      services: {
        schemaRegistry: {
          authentication: "oauth",
          baseUrl: "https://schema.example.test:8081/",
        },
      },
      transport: "plaintext",
    };

    await expect(service.create(input)).resolves.toMatchObject({
      profiles: [
        {
          id: "plaintext-profile",
          oauth: { clientSecretPresent: true },
          transport: "plaintext",
        },
      ],
    });
    expect(store.records()[0]).not.toHaveProperty("trust");
    await expect(service.resolveConnection("plaintext-profile")).resolves.toMatchObject({
      oauth: { clientSecret: "plain-secret" },
      services: {
        schemaRegistry: {
          authentication: "oauth",
          baseUrl: "https://schema.example.test:8081",
        },
      },
      tls: { enabled: false },
    });
    expect(decoder.calls).toEqual([]);
  });

  it("atomically clears TLS-only values when transport changes to plaintext", async () => {
    const existing: TlsProfileRecord = {
      ...tlsRecord("transition-profile", "TLS destination"),
      apiCaPem: "api-ca",
      binding: {
        overrides: {},
        recipe: {
          id: "fixture-recipe",
          kind: "pem",
          method: "ssh",
          name: "Fixture recipe",
          parameters: [],
          revision: 1,
          ssh: {
            password: { source: "none" },
            source: "file",
            value: "/tmp/ca.pem",
          },
          syntax: "named-v1",
          timeoutSeconds: 30,
        },
      },
      oauth: {
        clientId: "client",
        clientSecret: "secret",
        scope: "kafka",
        tokenEndpoint: "https://identity.example.test/token",
      },
      revision: 1,
    };
    const store = new InMemoryKafkaProfileStore(capability, [existing]);
    const decoder = new AcceptingTrustDecoder();
    const service = new KafkaProfileService(store, decoder);
    const oauth = existing.oauth;
    if (oauth === undefined) throw new Error("Expected OAuth fixture values.");

    await service.update(existing.id, {
      brokers: existing.brokers,
      expectedRevision: 1,
      name: existing.name,
      oauth: {
        clientId: oauth.clientId,
        clientSecret: { mode: "retain" },
        scope: oauth.scope,
        tokenEndpoint: oauth.tokenEndpoint,
      },
      transport: "plaintext",
    });

    expect(store.records()[0]).toMatchObject({
      oauth: existing.oauth,
      revision: 2,
      transport: "plaintext",
    });
    expect(store.records()[0]).not.toHaveProperty("trust");
    expect(store.records()[0]).not.toHaveProperty("binding");
    expect(store.records()[0]).not.toHaveProperty("apiCaPem");
    expect(decoder.calls).toEqual([]);
  });

  it("requires fresh trust when transport changes from plaintext to TLS", async () => {
    const existing: KafkaProfileRecord = {
      brokers: ["127.0.0.1:19092"],
      createdAt: "2026-09-17T18:00:00.000Z",
      id: "plaintext-profile",
      name: "Plaintext destination",
      revision: 1,
      transport: "plaintext",
      updatedAt: "2026-09-17T18:00:00.000Z",
    };
    const store = new InMemoryKafkaProfileStore(capability, [existing]);
    const decoder = new AcceptingTrustDecoder();
    const service = new KafkaProfileService(store, decoder);
    const update = {
      brokers: existing.brokers,
      expectedRevision: 1,
      name: existing.name,
      transport: "tls" as const,
    };

    await expect(
      service.update(existing.id, {
        ...update,
        apiCa: { mode: "replace", value: "must-not-decode-before-fresh-trust" },
        trust: {
          kind: "pem",
          label: "ca.pem",
          material: { mode: "retain" },
          password: { mode: "clear" },
        },
      }),
    ).rejects.toBeInstanceOf(KafkaProfileValidationError);
    expect(store.commitCount).toBe(0);
    expect(decoder.calls).toEqual([]);

    await expect(
      service.update(existing.id, {
        ...update,
        trust: {
          kind: "pem",
          label: "ca.pem",
          material: {
            mode: "replace",
            value: "-----BEGIN CERTIFICATE-----\nfresh\n-----END CERTIFICATE-----",
          },
          password: { mode: "clear" },
        },
      }),
    ).resolves.toMatchObject({
      profiles: [{ id: existing.id, transport: "tls", trust: { materialPresent: true } }],
    });
  });

  it("preserves active and revision guards across transport changes", async () => {
    const existing = { ...tlsRecord("guarded-profile", "Guarded TLS"), revision: 2 };
    const store = new InMemoryKafkaProfileStore(capability, [existing]);
    const decoder = new AcceptingTrustDecoder();
    const service = new KafkaProfileService(store, decoder);
    const transition: ProfileUpdateInput = {
      brokers: existing.brokers,
      expectedRevision: 1,
      name: existing.name,
      transport: "plaintext",
    };

    await expect(service.update(existing.id, transition)).rejects.toMatchObject({
      code: "VALIDATION",
    });
    await service.markActive(existing.id);
    await expect(
      service.update(existing.id, { ...transition, expectedRevision: 2 }),
    ).rejects.toBeInstanceOf(ActiveKafkaProfileMutationError);
    expect(store.commitCount).toBe(0);
    expect(store.records()).toEqual([existing]);
    expect(decoder.calls).toEqual([]);
  });
});
