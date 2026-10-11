import { mkdtemp, readFile, writeFile, stat, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, expect, it } from "vitest";

import type { KafkaProfileRecord } from "../../src/features/kafka/application";
import {
  AtomicKafkaProfileFileStore,
  KafkaProfileFileCorruptError,
  type KafkaProfileProtector,
} from "../../src/platform/node/kafka-profile-file-store";

const roots: string[] = [];
afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});
const protector: KafkaProfileProtector = {
  protect: (value) => Promise.resolve(Buffer.from(value).reverse()),
  unprotect: (value) =>
    Promise.resolve({ plaintext: Buffer.from(value).reverse().toString(), shouldReEncrypt: false }),
};
const capability = { durability: "durable", protection: "os-protected", state: "ready" } as const;
const profile: KafkaProfileRecord = {
  id: "protected-profile",
  name: "Preserved profile",
  transport: "tls",
  brokers: ["broker.test:9093"],
  createdAt: "2026-10-10T00:00:00.000Z",
  updatedAt: "2026-10-10T00:00:00.000Z",
  revision: 1,
  oauth: {
    clientId: "broker",
    clientSecret: "broker-private",
    scope: "kafka",
    tokenEndpoint: "https://broker.test/token",
  },
  trust: { kind: "pem", label: "broker.pem", material: "broker-ca" },
  clientIdentity: {
    certificatePem: "client-cert",
    privateKeyPem: "client-private",
    passphrase: "passphrase-private",
  },
  source: {
    kind: "plugin",
    pluginId: "streamskope.eda",
    version: 1,
    data: { sessionId: "retained-source" },
  },
  services: {
    connect: {
      baseUrl: "https://connect.test",
      authentication: "oauth-client",
      oauth: {
        clientId: "connect",
        clientSecret: "connect-private",
        scope: "",
        tokenEndpoint: "https://connect.test/token",
      },
      trust: { mode: "system" },
    },
    schemaRegistry: {
      baseUrl: "https://registry.test",
      authentication: "basic",
      basic: { username: "registry", password: "registry-private" },
      trust: {
        mode: "custom",
        kind: "pem",
        label: "registry.pem",
        material: "registry-ca",
        password: "",
      },
    },
  },
};
const metrics = {
  baseUrl: "https://metrics.test",
  authentication: "bearer",
  bearer: "metrics-private",
  trust: { mode: "system" },
} as const;
async function path(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-metrics-profile-"));
  roots.push(root);
  return join(root, "kafka-profiles.json");
}
it("preserves complete format-4 protected and plugin values when adding metrics, retains private exact rollback bytes and never downgrades format 5", async () => {
  const file = await path(),
    store = new AtomicKafkaProfileFileStore(file, protector, capability);
  await store.commit([profile]);
  const original = await readFile(file);
  expect(JSON.parse(original.toString())).toMatchObject({ version: 4 });
  const extended = { ...profile, services: { ...profile.services, metrics } };
  await store.commit([extended]);
  const encoded = await readFile(file, "utf8");
  const document = JSON.parse(encoded) as {
    version: number;
    rollbackGeneration: string;
    profiles: { protectedValue: string }[];
  };
  expect(document.version).toBe(5);
  expect(document.rollbackGeneration).toBe("kafka-profiles.json.pre-metrics-v4");
  expect(await readFile(join(file, "..", document.rollbackGeneration))).toEqual(original);
  expect((await stat(join(file, "..", document.rollbackGeneration))).mode & 0o777).toBe(0o600);
  expect(
    JSON.parse(Buffer.from(document.profiles[0]!.protectedValue, "base64").reverse().toString()),
  ).toMatchObject({ version: 7 });
  for (const secret of [
    "broker-private",
    "client-private",
    "passphrase-private",
    "connect-private",
    "registry-private",
    "metrics-private",
  ])
    expect(encoded).not.toContain(secret);
  const restarted = new AtomicKafkaProfileFileStore(file, protector, capability);
  await expect(restarted.load()).resolves.toEqual([extended]);
  await restarted.commit([profile]);
  expect(JSON.parse(await readFile(file, "utf8"))).toMatchObject({ version: 5 });
  await expect(
    new AtomicKafkaProfileFileStore(file, protector, capability).load(),
  ).resolves.toEqual([profile]);
});
it("rejects metrics smuggled into format-4 metadata or protected values, and mismatched format-5 protected payloads without rewriting", async () => {
  const file = await path(),
    store = new AtomicKafkaProfileFileStore(file, protector, capability);
  await store.commit([{ ...profile, services: { ...profile.services, metrics } }]);
  const original = await readFile(file, "utf8");
  interface Mutable {
    version: number;
    profiles: { protectedValue: string; services: Record<string, unknown> }[];
  }
  for (const mode of ["metadata", "protected", "pairing"]) {
    const doc = JSON.parse(original) as Mutable;
    const record = doc.profiles[0]!;
    const secret = JSON.parse(
      Buffer.from(record.protectedValue, "base64").reverse().toString(),
    ) as { version: number };
    if (mode !== "pairing") doc.version = 4;
    secret.version = 6;
    if (mode === "protected") delete record.services.metrics;
    record.protectedValue = (await protector.protect(JSON.stringify(secret))).toString("base64");
    const changed = JSON.stringify(doc);
    await writeFile(file, changed, { mode: 0o600 });
    await expect(
      new AtomicKafkaProfileFileStore(file, protector, capability).load(),
    ).rejects.toBeInstanceOf(KafkaProfileFileCorruptError);
    expect(await readFile(file, "utf8")).toBe(changed);
  }
});
