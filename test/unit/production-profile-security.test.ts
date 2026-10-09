import { describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostEvent,
  type ProfileCreateInput,
  type ProfileUpdateInput,
} from "../../src/features/kafka/contracts";
import { KafkaProfileService } from "../../src/features/kafka/application/profile-service";
import { InMemoryKafkaProfileStore } from "../../src/features/kafka/application/in-memory-profile-store";
import { retainedServiceEndpoints } from "../../src/features/kafka/contracts/profile-retain-input";
import type {
  KafkaProfileTrustDecoder,
  KafkaProfileTrustDecoderResult,
} from "../../src/features/kafka/application/profile-types";

const replace = (value: string): { readonly mode: "replace"; readonly value: string } => ({
  mode: "replace",
  value,
});
const clear = { mode: "clear" as const };
const retain = { mode: "retain" as const };
const input: Extract<ProfileCreateInput, { trust: unknown }> = {
  name: "Production profile",
  brokers: ["broker.test:9093"],
  transport: "tls",
  trust: { kind: "pem", label: "broker.pem", material: replace("broker-ca"), password: clear },
  sasl: {
    mechanism: "SCRAM-SHA-256",
    username: "broker-user",
    password: replace("broker-password"),
  },
  clientIdentity: {
    certificatePem: replace("client-cert"),
    privateKeyPem: replace("client-private-key"),
    passphrase: clear,
  },
  services: {
    schemaRegistry: {
      baseUrl: "https://registry.test",
      authentication: "basic",
      basic: { username: "registry-user", password: replace("registry-password") },
      trust: {
        mode: "custom",
        kind: "pem",
        label: "registry.pem",
        material: replace("registry-ca"),
        password: clear,
      },
    },
    connect: {
      baseUrl: "https://connect.test",
      authentication: "oauth-client",
      oauth: {
        clientId: "connect-user",
        clientSecret: replace("connect-secret"),
        scope: "",
        tokenEndpoint: "https://connect.test/token",
      },
      trust: { mode: "system" },
    },
  },
};
const decoder: KafkaProfileTrustDecoder = {
  decode(value): Promise<KafkaProfileTrustDecoderResult> {
    return Promise.resolve({ kind: value.kind, caPem: value.material });
  },
};

function setup(): { store: InMemoryKafkaProfileStore; service: KafkaProfileService } {
  const store = new InMemoryKafkaProfileStore({
    durability: "session",
    protection: "memory",
    state: "ready",
  });
  const service = new KafkaProfileService(store, decoder, { createId: (): string => "production" });
  return { store, service };
}

describe("production profile credential boundary", () => {
  it("keeps saved credentials out of events and restores them from retain-only edits", async () => {
    const { store, service } = setup();
    const first = await service.create(input);
    const summary = first.profiles[0]!;
    const serialized = JSON.stringify(first);
    for (const value of [
      "broker-password",
      "client-private-key",
      "registry-password",
      "registry-ca",
      "connect-secret",
    ])
      expect(serialized).not.toContain(value);
    expect(() =>
      parseHostEvent({
        event: "profiles.changed",
        version: HOST_PROTOCOL_VERSION,
        sequence: 1,
        payload: first,
      }),
    ).not.toThrow();
    const update: ProfileUpdateInput = {
      ...input,
      name: "Renamed",
      expectedRevision: 1,
      trust: { ...input.trust, material: retain, password: retain },
      sasl: { mechanism: "SCRAM-SHA-256", username: "broker-user", password: retain },
      clientIdentity: { certificatePem: retain, privateKeyPem: retain, passphrase: retain },
      services: retainedServiceEndpoints(summary.services!),
    };
    const command = parseHostCommand({
      command: "profiles.update",
      version: HOST_PROTOCOL_VERSION,
      id: "edit",
      payload: { profileId: "production", profile: update },
    });
    expect(command.command).toBe("profiles.update");
    await service.update("production", update);
    const connection = await new KafkaProfileService(store, decoder).resolveConnection(
      "production",
    );
    expect(connection.sasl?.password).toBe("broker-password");
    expect(connection.tls).toEqual({
      enabled: true,
      caPem: "broker-ca",
      clientIdentity: { certificatePem: "client-cert", privateKeyPem: "client-private-key" },
    });
    expect(connection.services?.schemaRegistry).toEqual({
      authentication: "basic",
      baseUrl: "https://registry.test",
      basic: { username: "registry-user", password: "registry-password" },
      tls: { caPem: "registry-ca" },
    });
    expect(connection.services?.connect?.tls).toEqual({});
    expect(connection.services?.connect?.oauth?.clientSecret).toBe("connect-secret");
    expect(
      parseHostCommand({
        command: "connection.test",
        version: HOST_PROTOCOL_VERSION,
        id: "test",
        payload: connection,
      }),
    ).toMatchObject({ payload: { sasl: { mechanism: "SCRAM-SHA-256" } } });
  });

  it("rejects an invalid retained secret without modifying the committed profile", async () => {
    const { store, service } = setup();
    await service.create({ ...input, services: {} });
    const before = await store.load();
    await expect(
      service.update("production", {
        ...input,
        expectedRevision: 1,
        services: {
          connect: {
            authentication: "basic",
            baseUrl: "https://connect.test",
            basic: { username: "new-user", password: retain },
          },
        },
      }),
    ).rejects.toThrow("Supply a value");
    expect(await store.load()).toEqual(before);
  });

  it("rejects secret-bearing summaries at the host event boundary", async () => {
    const { service } = setup();
    const snapshot = await service.create(input);
    const summary = snapshot.profiles[0]!;
    expect(() =>
      parseHostEvent({
        event: "profiles.changed",
        version: HOST_PROTOCOL_VERSION,
        sequence: 1,
        payload: {
          ...snapshot,
          profiles: [
            {
              ...summary,
              services: {
                schemaRegistry: {
                  ...summary.services!.schemaRegistry!,
                  basic: { username: "operator", passwordPresent: true, password: "leaked" },
                },
              },
            },
          ],
        },
      }),
    ).toThrow();
  });

  it.each(["basic", "bearer"] as const)(
    "rejects invalid %s service credentials before committing",
    async (mode) => {
      const { service, store } = setup();
      const endpoint =
        mode === "basic"
          ? {
              authentication: "basic" as const,
              baseUrl: "https://registry.test",
              basic: { username: "invalid:user", password: replace("secret") },
            }
          : {
              authentication: "bearer" as const,
              baseUrl: "https://registry.test",
              bearer: replace("token\r\ninjection"),
            };
      await expect(
        service.create({ ...input, services: { schemaRegistry: endpoint } }),
      ).rejects.toThrow();
      expect(await store.load()).toEqual([]);
    },
  );
});
