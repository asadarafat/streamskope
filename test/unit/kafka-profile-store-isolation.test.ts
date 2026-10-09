import { expect, it } from "vitest";

import { InMemoryKafkaProfileStore } from "../../src/features/kafka/application/in-memory-profile-store";
import type { KafkaProfileRecord } from "../../src/features/kafka/application/profile-types";

function fixture(): KafkaProfileRecord {
  return {
    id: "isolated",
    revision: 1,
    name: "Isolated",
    brokers: ["broker.example:9093"],
    createdAt: "2026-10-09T10:00:00.000Z",
    updatedAt: "2026-10-09T10:00:00.000Z",
    transport: "tls",
    trust: { kind: "pem", label: "ca.pem", material: "broker-ca" },
    sasl: { mechanism: "PLAIN", username: "broker-user", password: "broker-password" },
    clientIdentity: {
      certificatePem: "broker-cert",
      privateKeyPem: "broker-key",
      passphrase: "broker-passphrase",
    },
    services: {
      schemaRegistry: {
        baseUrl: "https://schema.example",
        authentication: "basic",
        basic: { username: "schema-user", password: "schema-password" },
        trust: {
          mode: "custom",
          kind: "pem",
          label: "schema.pem",
          material: "schema-ca",
          password: "",
        },
        clientIdentity: {
          certificatePem: "schema-cert",
          privateKeyPem: "schema-key",
          passphrase: "",
        },
      },
      connect: {
        baseUrl: "https://connect.example",
        authentication: "oauth-client",
        oauth: {
          clientId: "connect-client",
          tokenEndpoint: "https://identity.example/token",
          clientSecret: "connect-secret",
          scope: "connect",
        },
      },
    },
  };
}

function mutate(record: KafkaProfileRecord): void {
  Reflect.set(record.sasl!, "password", "changed");
  Reflect.set(record.clientIdentity!, "privateKeyPem", "changed");
  Reflect.set(record.services!.schemaRegistry!.basic!, "password", "changed");
  Reflect.set(record.services!.schemaRegistry!.trust!, "material", "changed");
  Reflect.set(record.services!.schemaRegistry!.clientIdentity!, "privateKeyPem", "changed");
  Reflect.set(record.services!.connect!.oauth!, "clientSecret", "changed");
}

it("isolates the complete credential graph from initialization and commit callers", async () => {
  const initial = fixture();
  const store = new InMemoryKafkaProfileStore(
    { state: "ready", durability: "session", protection: "memory" },
    [initial],
  );
  mutate(initial);
  expect(await store.load()).toEqual([fixture()]);
  const candidate = fixture();
  await store.commit([candidate]);
  mutate(candidate);
  expect(await store.load()).toEqual([fixture()]);
  expect(store.commitCount).toBe(1);
});

it("isolates loaded snapshots from retained credentials in committed state", async () => {
  const store = new InMemoryKafkaProfileStore(
    { state: "ready", durability: "session", protection: "memory" },
    [fixture()],
  );
  const [snapshot] = await store.load();
  mutate(snapshot!);
  mutate(store.records()[0]!);
  expect(await store.load()).toEqual([fixture()]);
  expect(store.commitCount).toBe(0);
});
