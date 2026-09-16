import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import type { ProfileStoreCapability } from "../../src/features/kafka/contracts";
import {
  InMemoryKafkaProfileStore,
  KafkaProfileService,
  type KafkaProfileRecord,
} from "../../src/features/kafka/application";
import { prepareLocalAioDevelopmentProfile } from "../../tools/kafka-fixture/development-profile";

const capability: ProfileStoreCapability = {
  durability: "session",
  protection: "memory",
  state: "ready",
};

const timestamp = "2026-08-05T18:00:00.000Z";
const certificate =
  "-----BEGIN CERTIFICATE-----\nfixture-ca-public-material\n-----END CERTIFICATE-----";
const secret = "fixture-demo-secret";
const temporaryDirectories: string[] = [];

async function createFixtureRoot(): Promise<string> {
  const repositoryRoot = await mkdtemp(join(tmpdir(), "streamskope-local-aio-"));
  temporaryDirectories.push(repositoryRoot);
  const fixtureRoot = join(repositoryRoot, "aio-kafka");
  const certificatePath = join(fixtureRoot, "ownership", "streamskope-kafka", "certs", "ca.pem");
  await mkdir(join(fixtureRoot, "ownership", "records"), { recursive: true });
  await mkdir(join(certificatePath, ".."), { recursive: true });
  await writeFile(
    join(fixtureRoot, "fixture.config.json"),
    JSON.stringify({
      oauthClientId: "admin",
      oauthClientSecret: secret,
      oauthImage: "fixture-oauth:1",
      oauthScope: "kafka",
      schemaDefinition: '{"type":"record","name":"Fixture","fields":[]}',
      schemaRegistryAudience: "streamskope-schema-registry",
      schemaRegistryImage: "fixture-schema-registry:1@sha256:fixture",
      schemaRegistryRole: "streamskope.schema:manage",
      schemaSubject: "test-value",
      seedPayload: '{"message":"ready"}',
      topic: "test",
    }),
  );
  await writeFile(certificatePath, certificate);
  await writeFile(
    join(fixtureRoot, "ownership", "records", "streamskope-kafka.json"),
    JSON.stringify({
      caPath: certificatePath,
      kafkaPort: 19093,
      name: "streamskope-kafka",
      oauthImage: "fixture-oauth:1",
      oauthPort: 15000,
      ownership: "owned",
      schemaRegistryImage: "fixture-schema-registry:1@sha256:fixture",
      schemaRegistryPort: 18081,
      topologyPath: join(fixtureRoot, "topology.clab.yml"),
    }),
  );
  return repositoryRoot;
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

describe("local AIO browser-development profile", () => {
  it("seeds one complete host-only profile from authoritative fixture evidence", async () => {
    const repositoryRoot = await createFixtureRoot();
    const store = new InMemoryKafkaProfileStore(capability);

    const result = await prepareLocalAioDevelopmentProfile(store, {
      now: () => new Date(timestamp),
      repositoryRoot,
    });

    expect(result).toEqual({ profileName: "Local AIO Kafka", status: "seeded" });
    expect(store.records()).toEqual([
      {
        brokers: ["127.0.0.1:19093"],
        createdAt: timestamp,
        id: "local-aio-kafka",
        name: "Local AIO Kafka",
        oauth: {
          clientId: "admin",
          clientSecret: secret,
          scope: "kafka",
          tokenEndpoint: "http://127.0.0.1:15000/rest-gateway/rest/api/v1/auth/token",
        },
        services: {
          schemaRegistry: {
            authentication: "oauth",
            baseUrl: "http://127.0.0.1:18081",
          },
        },
        trust: {
          kind: "pem",
          label: "Repository fixture CA",
          material: certificate,
        },
        updatedAt: timestamp,
      },
    ]);
    expect(store.commitCount).toBe(1);

    const snapshot = await new KafkaProfileService(store, {
      decode: (): Promise<never> =>
        Promise.reject(new Error("Profile resolution was not requested.")),
    }).list();
    expect(snapshot).toEqual({
      profiles: [
        {
          active: false,
          brokers: ["127.0.0.1:19093"],
          createdAt: timestamp,
          id: "local-aio-kafka",
          name: "Local AIO Kafka",
          oauth: {
            clientId: "admin",
            clientSecretPresent: true,
            scope: "kafka",
            tokenEndpoint: "http://127.0.0.1:15000/rest-gateway/rest/api/v1/auth/token",
          },
          services: {
            schemaRegistry: {
              authentication: "oauth",
              baseUrl: "http://127.0.0.1:18081",
            },
          },
          trust: {
            kind: "pem",
            label: "Repository fixture CA",
            materialPresent: true,
            passwordPresent: false,
          },
          updatedAt: timestamp,
        },
      ],
      store: capability,
    });
    expect(JSON.stringify(snapshot)).not.toContain(secret);
    expect(JSON.stringify(snapshot)).not.toContain(certificate);

    await expect(
      prepareLocalAioDevelopmentProfile(store, {
        now: () => new Date("2026-08-05T19:00:00.000Z"),
        repositoryRoot,
      }),
    ).resolves.toEqual({ status: "unchanged" });
    expect(store.commitCount).toBe(1);
  });

  it("does not inspect fixture data or rewrite a non-empty session store", async () => {
    const existing: KafkaProfileRecord = {
      brokers: ["broker.example:9093"],
      createdAt: timestamp,
      id: "existing-profile",
      name: "Existing profile",
      trust: { kind: "pem", label: "Existing CA", material: certificate },
      updatedAt: timestamp,
    };
    const store = new InMemoryKafkaProfileStore(capability, [existing]);

    await expect(
      prepareLocalAioDevelopmentProfile(store, {
        now: () => new Date(timestamp),
        repositoryRoot: "/fixture-path-must-not-be-read",
      }),
    ).resolves.toEqual({ status: "unchanged" });
    expect(store.records()).toEqual([existing]);
    expect(store.commitCount).toBe(0);
  });

  it("returns fixed recovery without publishing a partial profile", async () => {
    const repositoryRoot = await mkdtemp(join(tmpdir(), "streamskope-local-aio-missing-"));
    temporaryDirectories.push(repositoryRoot);
    const store = new InMemoryKafkaProfileStore(capability);

    await expect(
      prepareLocalAioDevelopmentProfile(store, {
        now: () => new Date(timestamp),
        repositoryRoot,
      }),
    ).resolves.toEqual({
      recovery: "Run npm run fixture:start before connecting to Local AIO Kafka.",
      status: "unavailable",
    });
    expect(store.records()).toEqual([]);
    expect(store.commitCount).toBe(0);
  });

  it("rejects legacy ownership without current Schema Registry evidence", async () => {
    const repositoryRoot = await createFixtureRoot();
    const fixtureRoot = join(repositoryRoot, "aio-kafka");
    const ownershipPath = join(fixtureRoot, "ownership", "records", "streamskope-kafka.json");
    const legacy = JSON.parse(await readFile(ownershipPath, "utf8")) as Record<string, unknown>;
    delete legacy.schemaRegistryImage;
    delete legacy.schemaRegistryPort;
    await writeFile(ownershipPath, JSON.stringify(legacy));
    const store = new InMemoryKafkaProfileStore(capability);

    await expect(
      prepareLocalAioDevelopmentProfile(store, { repositoryRoot }),
    ).resolves.toMatchObject({ status: "unavailable" });
    expect(store.records()).toEqual([]);
  });
});
