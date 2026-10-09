import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  stat,
  symlink,
  writeFile,
} from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { afterEach, describe, expect, it } from "vitest";

import type { ProfileStoreCapability } from "../../src/features/kafka/contracts";
import type { KafkaProfileRecord } from "../../src/features/kafka/application";
import { trustRecipeInput } from "../support/trust-recipe";
import {
  AtomicKafkaProfileFileStore,
  KafkaProfileFileCorruptError,
  KafkaProfileFileWriteError,
  type KafkaProfileProtector,
} from "../../src/platform/node/kafka-profile-file-store";

const capability: ProfileStoreCapability = {
  durability: "durable",
  protection: "os-protected",
  state: "ready",
};

const profile: KafkaProfileRecord = {
  brokers: ["127.0.0.1:19093"],
  createdAt: "2026-07-25T18:00:00.000Z",
  id: "01983770-8f0c-77e2-8edc-51426a9e2450",
  name: "Local validation",
  oauth: {
    clientId: "admin",
    clientSecret: "fixture-secret-never-on-disk",
    scope: "kafka",
    tokenEndpoint: "http://127.0.0.1:15000/token",
  },
  services: {
    connect: { authentication: "oauth", baseUrl: "https://connect.example.test:8083" },
    redpandaAdmin: {
      authentication: "oauth",
      baseUrl: "https://redpanda.example.test:9644",
    },
    schemaRegistry: {
      authentication: "none",
      baseUrl: "https://schema.example.test:8081",
    },
  },
  trust: {
    kind: "pem",
    label: "ca.pem",
    material:
      "-----BEGIN CERTIFICATE-----\ntrust-material-never-on-disk\n-----END CERTIFICATE-----",
  },
  updatedAt: "2026-07-25T18:00:00.000Z",
};

class ReversibleProtector implements KafkaProfileProtector {
  protectedValues: string[] = [];
  unprotectedValues: Buffer[] = [];

  protect(plaintext: string): Promise<Buffer> {
    this.protectedValues.push(plaintext);
    return Promise.resolve(Buffer.from(plaintext, "utf8").reverse());
  }

  unprotect(
    protectedValue: Buffer,
  ): Promise<{ readonly plaintext: string; readonly shouldReEncrypt: boolean }> {
    this.unprotectedValues.push(protectedValue);
    return Promise.resolve({
      plaintext: Buffer.from(protectedValue).reverse().toString("utf8"),
      shouldReEncrypt: false,
    });
  }
}

const temporaryDirectories: string[] = [];

it("preserves managed capture identity across a durable store restart and profile rename", async () => {
  const path = await temporaryProfilePath();
  const protector = new ReversibleProtector();
  const legacySource = {
    kind: "eda-capture" as const,
    state: "ready" as const,
    broker: "127.0.0.1:19092",
    clusterBroker: "capture:9092",
    context: "explicit-host",
    edaApiUrl: "https://eda.example.test",
    sessionId: "session-before-restart",
    source: {
      apiVersion: "kafka.eda.nokia.com/v1" as const,
      kind: "Producer" as const,
      namespace: "eda-system",
      name: "interfaces",
    },
    topics: [],
    exporterName: "streamskope-capture",
    workloadName: "streamskope-redpanda",
  };
  const source = {
    kind: "plugin" as const,
    pluginId: "streamskope.eda",
    version: 1 as const,
    data: legacySource,
  };
  const record: KafkaProfileRecord = {
    id: "capture-profile",
    name: "Renamed without EDA prefix",
    transport: "plaintext",
    brokers: [legacySource.broker],
    source,
    createdAt: profile.createdAt,
    updatedAt: profile.updatedAt,
  };
  const store = new AtomicKafkaProfileFileStore(path, protector, capability);
  await store.commit([record]);
  const reopened = new AtomicKafkaProfileFileStore(path, protector, capability);
  expect((await reopened.load())[0]).toMatchObject({ name: record.name, source });
});

it("migrates legacy EDA profile metadata without requiring the plugin or rewriting on read", async () => {
  const path = await temporaryProfilePath();
  const protector = new ReversibleProtector();
  const legacySource = {
    kind: "eda-capture",
    state: "ready",
    broker: "127.0.0.1:19092",
    clusterBroker: "capture:9092",
    edaApiUrl: "https://eda.example.test",
    sessionId: "recoverable-session",
    source: {
      apiVersion: "kafka.eda.nokia.com/v1",
      kind: "Producer",
      namespace: "eda-system",
      name: "interfaces",
    },
    topics: ["interfaces"],
    exporterName: "capture-exporter",
    workloadName: "capture-broker",
  };
  const protectedValue = await protector.protect(
    JSON.stringify({ profileId: "legacy-capture", transport: "plaintext", version: 5 }),
  );
  const legacyRecord = {
    id: "legacy-capture",
    name: "Existing capture",
    brokers: [legacySource.broker],
    transport: "plaintext",
    createdAt: profile.createdAt,
    updatedAt: profile.updatedAt,
    source: legacySource,
  };
  const original = `${JSON.stringify({ version: 3, profiles: [{ ...legacyRecord, protectedValue: protectedValue.toString("base64") }] })}\n`;
  await mkdir(join(path, ".."), { recursive: true });
  await writeFile(path, original, { mode: 0o600 });
  const store = new AtomicKafkaProfileFileStore(path, protector, capability);
  const records = await store.load();
  const migrated = {
    ...legacyRecord,
    source: { kind: "plugin", pluginId: "streamskope.eda", version: 1, data: legacySource },
  };
  expect(records).toEqual([migrated]);
  expect(await readFile(path, "utf8")).toBe(original);
  await store.commit(records);
  expect(JSON.parse(await readFile(path, "utf8"))).toMatchObject({ profiles: [migrated] });
  expect(await new AtomicKafkaProfileFileStore(path, protector, capability).load()).toEqual(
    records,
  );
});

async function temporaryProfilePath(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-profile-store-"));
  temporaryDirectories.push(directory);
  return join(directory, "application-data", "kafka-profiles.json");
}

async function writeLegacyVersionTwo(
  path: string,
  protector: KafkaProfileProtector,
  value: KafkaProfileRecord = profile,
  protectedVersion?: 1 | 2 | 3 | 4,
): Promise<Buffer> {
  if (value.transport === "plaintext" || value.trust === undefined) {
    throw new Error("A version-two fixture must be a TLS profile.");
  }
  const protectedValue = await protector.protect(
    JSON.stringify({
      ...(value.apiCaPem === undefined ? {} : { apiCaPem: value.apiCaPem }),
      ...(value.binding === undefined ? {} : { binding: value.binding }),
      ...(value.oauth === undefined ? {} : { oauth: { clientSecret: value.oauth.clientSecret } }),
      ...(value.revision === undefined ? {} : { revision: value.revision }),
      profileId: value.id,
      trust: {
        material: value.trust.material,
        ...(value.trust.password === undefined ? {} : { password: value.trust.password }),
      },
      version:
        protectedVersion ??
        (value.binding !== undefined || value.apiCaPem !== undefined
          ? 4
          : value.revision === undefined
            ? 1
            : 2),
    }),
  );
  const document = {
    profiles: [
      {
        brokers: value.brokers,
        createdAt: value.createdAt,
        id: value.id,
        name: value.name,
        ...(value.oauth === undefined
          ? {}
          : {
              oauth: {
                clientId: value.oauth.clientId,
                clientSecretPresent: value.oauth.clientSecret.length > 0,
                scope: value.oauth.scope,
                tokenEndpoint: value.oauth.tokenEndpoint,
              },
            }),
        protectedValue: protectedValue.toString("base64"),
        ...(value.services === undefined ? {} : { services: value.services }),
        trust: {
          kind: value.trust.kind,
          label: value.trust.label,
          materialPresent: value.trust.material.length > 0,
          passwordPresent: value.trust.password !== undefined,
        },
        updatedAt: value.updatedAt,
      },
    ],
    version: 2,
  };
  const bytes = Buffer.from(`${JSON.stringify(document)}\n`, "utf8");
  await mkdir(join(path, ".."), { recursive: true });
  await writeFile(path, bytes, { mode: 0o600 });
  return bytes;
}

async function mutateFirstProtectedValue(
  path: string,
  mutate: (value: Record<string, unknown>) => void,
): Promise<void> {
  const document = JSON.parse(await readFile(path, "utf8")) as {
    profiles: Array<{ protectedValue: string }>;
  };
  const stored = document.profiles[0];
  if (stored === undefined) throw new Error("Expected one stored profile fixture.");
  const protectedValue = JSON.parse(
    Buffer.from(stored.protectedValue, "base64").reverse().toString("utf8"),
  ) as Record<string, unknown>;
  mutate(protectedValue);
  stored.protectedValue = Buffer.from(JSON.stringify(protectedValue), "utf8")
    .reverse()
    .toString("base64");
  await writeFile(path, `${JSON.stringify(document)}\n`, { mode: 0o600 });
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

describe("Kafka profile file store", () => {
  it("preserves an existing temporary file when a profile save collides with it", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    await new AtomicKafkaProfileFileStore(path, protector, capability).commit([]);
    const committed = await readFile(path);
    const temporary = join(path, "..", ".kafka-profiles.json.collision.tmp");
    await writeFile(temporary, "owned by another save");
    const store = new AtomicKafkaProfileFileStore(path, protector, capability, {
      createTempId: (): string => "collision",
    });
    await expect(store.commit([profile])).rejects.toBeInstanceOf(KafkaProfileFileWriteError);
    expect(await readFile(temporary, "utf8")).toBe("owned by another save");
    expect(await readFile(path)).toEqual(committed);
  });

  it("loads a version-two profile as explicit TLS without rewriting legacy bytes", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const original = await writeLegacyVersionTwo(path, protector);
    const store = new AtomicKafkaProfileFileStore(path, protector, capability);

    await expect(store.load()).resolves.toEqual([{ ...profile, transport: "tls" }]);
    await expect(readFile(path)).resolves.toEqual(original);
  });

  it("round-trips exact version-three TLS and plaintext protected variants", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const store = new AtomicKafkaProfileFileStore(path, protector, capability);
    const tls = { ...profile, revision: 1, transport: "tls" as const };
    const plaintext: KafkaProfileRecord = {
      brokers: ["127.0.0.1:19092"],
      createdAt: "2026-09-17T18:00:00.000Z",
      id: "plaintext-profile",
      name: "Plaintext profile",
      oauth: {
        clientId: "plain-client",
        clientSecret: "plain-secret",
        scope: "kafka",
        tokenEndpoint: "https://identity.example.test/token",
      },
      revision: 1,
      services: {
        schemaRegistry: {
          authentication: "oauth",
          baseUrl: "https://schema.example.test:8081",
        },
      },
      transport: "plaintext",
      updatedAt: "2026-09-17T18:00:00.000Z",
    };

    await store.commit([tls, plaintext]);

    await expect(store.load()).resolves.toEqual([tls, plaintext]);
    const document = JSON.parse(await readFile(path, "utf8")) as {
      profiles: Array<Record<string, unknown>>;
      version: number;
    };
    expect(document.version).toBe(3);
    expect(document.profiles).toMatchObject([
      { revision: 1, transport: "tls", trust: { materialPresent: true } },
      { revision: 1, transport: "plaintext" },
    ]);
    expect(document.profiles[1]).not.toHaveProperty("trust");
    const protectedTls = JSON.parse(protector.protectedValues.at(-2) ?? "") as Record<
      string,
      unknown
    >;
    const protectedPlaintext = JSON.parse(protector.protectedValues.at(-1) ?? "") as Record<
      string,
      unknown
    >;
    expect(protectedTls).toMatchObject({ transport: "tls", version: 5 });
    expect(protectedPlaintext).toMatchObject({ transport: "plaintext", version: 5 });
    expect(protectedPlaintext).not.toHaveProperty("trust");
    expect(protectedPlaintext).not.toHaveProperty("binding");
    expect(protectedPlaintext).not.toHaveProperty("apiCaPem");
  });

  it.each(["safe transport is unknown", "safe and protected transport differ"] as const)(
    "fails closed when version-three %s",
    async (failure) => {
      const path = await temporaryProfilePath();
      const protector = new ReversibleProtector();
      const store = new AtomicKafkaProfileFileStore(path, protector, capability);
      await store.commit([{ ...profile, revision: 1, transport: "tls" }]);
      const document = JSON.parse(await readFile(path, "utf8")) as {
        profiles: Array<Record<string, unknown>>;
      };
      const stored = document.profiles[0];
      if (stored === undefined) throw new Error("Expected one stored profile fixture.");
      if (failure === "safe transport is unknown") {
        stored.transport = "udp";
        await writeFile(path, `${JSON.stringify(document)}\n`, { mode: 0o600 });
      } else {
        await mutateFirstProtectedValue(path, (value) => {
          value.transport = "plaintext";
          delete value.trust;
        });
      }
      const original = await readFile(path);

      await expect(
        new AtomicKafkaProfileFileStore(path, protector, capability).load(),
      ).rejects.toBeInstanceOf(KafkaProfileFileCorruptError);
      await expect(readFile(path)).resolves.toEqual(original);
    },
  );

  it("rejects hidden trust in a version-three plaintext protected payload", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const plaintext: KafkaProfileRecord = {
      brokers: ["127.0.0.1:19092"],
      createdAt: "2026-09-17T18:00:00.000Z",
      id: "plaintext-profile",
      name: "Plaintext profile",
      revision: 1,
      transport: "plaintext",
      updatedAt: "2026-09-17T18:00:00.000Z",
    };
    await new AtomicKafkaProfileFileStore(path, protector, capability).commit([plaintext]);
    await mutateFirstProtectedValue(path, (value) => {
      value.trust = { material: "hidden-trust" };
    });
    const corrupt = await readFile(path);

    await expect(
      new AtomicKafkaProfileFileStore(path, protector, capability).load(),
    ).rejects.toBeInstanceOf(KafkaProfileFileCorruptError);
    await expect(readFile(path)).resolves.toEqual(corrupt);
  });

  it("creates the lowest unused non-overwriting transport rollback generation", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const original = await writeLegacyVersionTwo(path, protector);
    const generationZero = `${path}.pre-transport-v2`;
    const generationOne = `${generationZero}.1`;
    const generationTwo = `${generationZero}.2`;
    const sentinel = Buffer.from("existing-generation", "utf8");
    const symlinkTarget = join(path, "..", "must-not-be-read-or-written");
    await writeFile(generationZero, sentinel, { mode: 0o600 });
    await writeFile(symlinkTarget, "symlink-target", { mode: 0o600 });
    await symlink(symlinkTarget, generationOne);
    const store = new AtomicKafkaProfileFileStore(path, protector, capability);

    await store.commit([{ ...profile, revision: 1, transport: "tls" }]);

    await expect(readFile(generationZero)).resolves.toEqual(sentinel);
    await expect(readFile(symlinkTarget, "utf8")).resolves.toBe("symlink-target");
    await expect(readFile(generationTwo)).resolves.toEqual(original);
    expect((await stat(generationTwo)).mode & 0o777).toBe(0o600);
    expect(JSON.parse(await readFile(path, "utf8"))).toMatchObject({
      rollbackGeneration: "kafka-profiles.json.pre-transport-v2.2",
      version: 3,
    });
    await store.commit([{ ...profile, revision: 2, transport: "tls" }]);
    expect(JSON.parse(await readFile(path, "utf8"))).toMatchObject({
      rollbackGeneration: "kafka-profiles.json.pre-transport-v2.2",
      version: 3,
    });
    await expect(readFile(generationTwo)).resolves.toEqual(original);
  });

  it("does not create a transport rollback generation on read or a new install", async () => {
    const legacyPath = await temporaryProfilePath();
    const legacyProtector = new ReversibleProtector();
    await writeLegacyVersionTwo(legacyPath, legacyProtector);
    await new AtomicKafkaProfileFileStore(legacyPath, legacyProtector, capability).load();
    expect(await readdir(join(legacyPath, ".."))).toEqual(["kafka-profiles.json"]);

    const newPath = await temporaryProfilePath();
    await new AtomicKafkaProfileFileStore(newPath, new ReversibleProtector(), capability).commit([
      { ...profile, revision: 1, transport: "tls" },
    ]);
    expect(await readdir(join(newPath, ".."))).toEqual(["kafka-profiles.json"]);
    expect(JSON.parse(await readFile(newPath, "utf8"))).not.toHaveProperty("rollbackGeneration");
  });

  it("aborts before primary replacement when all transport generations are occupied", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const original = await writeLegacyVersionTwo(path, protector);
    const base = `${path}.pre-transport-v2`;
    await Promise.all(
      Array.from({ length: 100 }, (_, generation) =>
        writeFile(generation === 0 ? base : `${base}.${String(generation)}`, "occupied", {
          mode: 0o600,
        }),
      ),
    );
    const store = new AtomicKafkaProfileFileStore(path, protector, capability);

    let failure: unknown;
    try {
      await store.commit([{ ...profile, revision: 1, transport: "tls" }]);
    } catch (error) {
      failure = error;
    }
    expect(failure).toBeInstanceOf(KafkaProfileFileWriteError);
    if (!(failure instanceof KafkaProfileFileWriteError)) {
      throw new Error("Expected rollback generation exhaustion.");
    }
    expect(failure.recovery).not.toContain(".pre-transport-v2");
    expect(failure.recovery).toContain("100");
    await expect(readFile(path)).resolves.toEqual(original);
    expect((await stat(join(path, ".."))).mode & 0o777).toBe(0o700);
  });

  it("reports the exact rollback generation when replacement fails after preservation", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const original = await writeLegacyVersionTwo(path, protector);
    const temporaryPath = join(path, "..", ".kafka-profiles.json.blocked.tmp");
    await mkdir(temporaryPath);
    const store = new AtomicKafkaProfileFileStore(path, protector, capability, {
      createTempId: (): string => "blocked",
    });

    let failure: unknown;
    try {
      await store.commit([{ ...profile, revision: 1, transport: "tls" }]);
    } catch (error) {
      failure = error;
    }
    expect(failure).toBeInstanceOf(KafkaProfileFileWriteError);
    if (!(failure instanceof KafkaProfileFileWriteError)) {
      throw new Error("Expected profile storage to reject replacement.");
    }
    expect(failure.recovery).toContain("kafka-profiles.json.pre-transport-v2");
    await expect(readFile(path)).resolves.toEqual(original);
    await expect(readFile(`${path}.pre-transport-v2`)).resolves.toEqual(original);
  });

  it("reads the previous version-three binding payload without rewriting it", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const bound = {
      ...profile,
      revision: 1,
      binding: { recipe: { ...trustRecipeInput(), id: "legacy", revision: 1 }, overrides: {} },
    };
    await writeLegacyVersionTwo(path, protector, bound, 3);
    const before = await readFile(path);
    expect(await new AtomicKafkaProfileFileStore(path, protector, capability).load()).toEqual([
      { ...bound, transport: "tls" },
    ]);
    expect(await readFile(path)).toEqual(before);
  });
  it("keeps an independent API CA inside the protected record across restart", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const value = {
      ...profile,
      revision: 1,
      apiCaPem: "-----BEGIN CERTIFICATE-----\napi-ca-only\n-----END CERTIFICATE-----",
      binding: {
        recipe: { ...trustRecipeInput(), id: "api-access", revision: 1 },
        overrides: {},
        apiAccess: {
          host: "private-api.example.test",
          username: "api-reader",
          tls: "custom" as const,
        },
      },
    };
    await new AtomicKafkaProfileFileStore(path, protector, capability).commit([value]);
    expect(await new AtomicKafkaProfileFileStore(path, protector, capability).load()).toEqual([
      { ...value, transport: "tls" },
    ]);
    expect(await readFile(path, "utf8")).not.toContain("api-ca-only");
    expect(await readFile(path, "utf8")).not.toContain("private-api.example.test");
    expect(protector.protectedValues.at(-1)).toContain("api-ca-only");
  });
  it("round-trips the pinned recipe only inside the protected payload", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const bound = {
      ...profile,
      revision: 1,
      binding: {
        recipe: { ...trustRecipeInput(), id: "fixture-recipe", revision: 1 },
        identity: { host: "lab.example.test", port: 22, fingerprint: `SHA256:${"A".repeat(43)}` },
        overrides: { certificate_path: "/remote/private/ca.pem" },
      },
    };
    await new AtomicKafkaProfileFileStore(path, protector, capability).commit([bound]);
    await expect(
      new AtomicKafkaProfileFileStore(path, protector, capability).load(),
    ).resolves.toEqual([{ ...bound, transport: "tls" }]);
    const contents = await readFile(path, "utf8");
    expect(contents).not.toContain("certificate_path");
    expect(contents).not.toContain("/remote/private");
    expect(contents).not.toContain("fixture-recipe");
  });

  it("preserves an exclusive protected pre-upgrade backup before the first revisioned save", async () => {
    const path = await temporaryProfilePath();
    const store = new AtomicKafkaProfileFileStore(path, new ReversibleProtector(), capability);
    await store.commit([profile]);
    const original = await readFile(path);
    await store.commit([{ ...profile, revision: 1 }]);
    await expect(readFile(`${path}.pre-upgrade.bak`)).resolves.toEqual(original);
    expect((await stat(`${path}.pre-upgrade.bak`)).mode & 0o777).toBe(0o600);
    await expect(
      new AtomicKafkaProfileFileStore(
        `${path}.pre-upgrade.bak`,
        new ReversibleProtector(),
        capability,
      ).load(),
    ).resolves.toEqual([{ ...profile, transport: "tls" }]);
    await store.commit([{ ...profile, revision: 2 }]);
    await expect(readFile(`${path}.pre-upgrade.bak`)).resolves.toEqual(original);
    expect(await readFile(`${path}.pre-upgrade.bak`, "utf8")).not.toContain(
      "trust-material-never-on-disk",
    );
  });

  it("blocks a save when its existing recovery backup is corrupt and preserves both files", async () => {
    const path = await temporaryProfilePath();
    const store = new AtomicKafkaProfileFileStore(path, new ReversibleProtector(), capability);
    await store.commit([profile]);
    const original = await readFile(path);
    await writeFile(`${path}.pre-upgrade.bak`, "incomplete backup", { mode: 0o600 });
    await expect(store.commit([{ ...profile, revision: 1 }])).rejects.toBeInstanceOf(
      KafkaProfileFileWriteError,
    );
    await expect(readFile(path)).resolves.toEqual(original);
    await expect(readFile(`${path}.pre-upgrade.bak`, "utf8")).resolves.toBe("incomplete backup");
  });

  it("rejects an invalid protected binding without rewriting original bytes", async () => {
    const path = await temporaryProfilePath();
    const writer = new AtomicKafkaProfileFileStore(path, new ReversibleProtector(), capability);
    await writer.commit([profile]);
    const original = await readFile(path);
    const invalidProtector: KafkaProfileProtector = {
      protect: (value) => Promise.resolve(Buffer.from(value)),
      unprotect: () =>
        Promise.resolve({
          plaintext: JSON.stringify({
            version: 3,
            revision: 1,
            profileId: profile.id,
            trust: { material: profile.trust.material },
            oauth: { clientSecret: profile.oauth?.clientSecret },
            binding: {
              recipe: { ...trustRecipeInput(), id: "fixture-recipe", revision: 1 },
              overrides: { undeclared: "must not be accepted" },
            },
          }),
          shouldReEncrypt: true,
        }),
    };
    const reader = new AtomicKafkaProfileFileStore(path, invalidProtector, capability);
    await expect(reader.load()).rejects.toBeInstanceOf(KafkaProfileFileCorruptError);
    await expect(readFile(path)).resolves.toEqual(original);
  });

  it("preserves the revision inside protected storage across restart", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const store = new AtomicKafkaProfileFileStore(path, protector, capability);
    const versioned = { ...profile, revision: 7 };
    await store.commit([versioned]);
    const restarted = new AtomicKafkaProfileFileStore(path, protector, capability);
    await expect(restarted.load()).resolves.toEqual([{ ...versioned, transport: "tls" }]);
    expect(JSON.parse(await readFile(path, "utf8"))).toMatchObject({
      profiles: [{ revision: 7, transport: "tls" }],
    });
  });

  it.each([0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1])(
    "preserves the original when revision %s is invalid",
    async (revision) => {
      const path = await temporaryProfilePath();
      const store = new AtomicKafkaProfileFileStore(path, new ReversibleProtector(), capability);
      await store.commit([profile]);
      const original = await readFile(path);
      await expect(store.commit([{ ...profile, revision }])).rejects.toBeInstanceOf(
        KafkaProfileFileWriteError,
      );
      await expect(readFile(path)).resolves.toEqual(original);
    },
  );

  it("rejects a protected revision document missing its revision without rewriting it", async () => {
    const path = await temporaryProfilePath();
    const writer = new AtomicKafkaProfileFileStore(path, new ReversibleProtector(), capability);
    await writer.commit([{ ...profile, revision: 2 }]);
    const original = await readFile(path);
    const invalidProtector: KafkaProfileProtector = {
      protect: (value) => Promise.resolve(Buffer.from(value)),
      unprotect: () =>
        Promise.resolve({
          plaintext: JSON.stringify({
            version: 2,
            profileId: profile.id,
            trust: { material: profile.trust.material },
            oauth: { clientSecret: profile.oauth?.clientSecret },
          }),
          shouldReEncrypt: true,
        }),
    };
    const reader = new AtomicKafkaProfileFileStore(path, invalidProtector, capability);
    await expect(reader.load()).rejects.toBeInstanceOf(KafkaProfileFileCorruptError);
    await expect(readFile(path)).resolves.toEqual(original);
  });

  it.each([
    ["source mode", { mode: "producerCapture" }],
    ["capture provenance", { captureSource: "external" }],
  ])("fails closed when stored profile data contains %s", async (_label, sourceFields) => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    await new AtomicKafkaProfileFileStore(path, protector, capability).commit([profile]);
    const document = JSON.parse(await readFile(path, "utf8")) as {
      profiles: Array<Record<string, unknown>>;
    };
    const [stored] = document.profiles;
    if (stored === undefined) throw new Error("Expected one stored profile fixture.");
    Object.assign(stored, sourceFields);
    const corrupt = `${JSON.stringify(document)}\n`;
    await writeFile(path, corrupt, { mode: 0o600 });

    const reader = new AtomicKafkaProfileFileStore(path, protector, capability);
    const result = reader.load();

    await expect(result).rejects.toBeInstanceOf(KafkaProfileFileCorruptError);
    await expect(readFile(path, "utf8")).resolves.toBe(corrupt);
    expect(reader.capability()).toMatchObject({
      durability: "durable",
      protection: "unavailable",
      state: "unavailable",
    });
  });

  it("atomically persists and restores a complete protected record with restrictive modes", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const store = new AtomicKafkaProfileFileStore(path, protector, capability, {
      createTempId: (): string => "commit-1",
    });

    await store.commit([profile]);

    await expect(store.load()).resolves.toEqual([{ ...profile, transport: "tls" }]);
    const serialized = await readFile(path, "utf8");
    expect(serialized).not.toContain("fixture-secret-never-on-disk");
    expect(serialized).not.toContain("trust-material-never-on-disk");
    expect(serialized).not.toContain("BEGIN CERTIFICATE");
    expect(JSON.parse(serialized)).toMatchObject({
      profiles: [
        {
          id: profile.id,
          name: profile.name,
          oauth: { clientSecretPresent: true },
          services: profile.services,
          transport: "tls",
          trust: { materialPresent: true, passwordPresent: false },
        },
      ],
      version: 3,
    });
    expect((await stat(path)).mode & 0o777).toBe(0o600);
    expect((await stat(join(path, ".."))).mode & 0o777).toBe(0o700);
    expect(await readdir(join(path, ".."))).toEqual(["kafka-profiles.json"]);
  });

  it("leaves the previously committed document authoritative when protection fails", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const store = new AtomicKafkaProfileFileStore(path, protector, capability, {
      createTempId: (): string => "commit-1",
    });
    await store.commit([profile]);
    const committed = await readFile(path);
    const failingProtector: KafkaProfileProtector = {
      protect: (): Promise<Buffer> =>
        Promise.reject(new Error("credential service locked for fixture-secret-never-on-disk")),
      unprotect: (value) => protector.unprotect(value),
    };
    const failingStore = new AtomicKafkaProfileFileStore(path, failingProtector, capability, {
      createTempId: (): string => "commit-2",
    });

    const changedProfile = { ...profile, name: "Changed profile" };
    await expect(failingStore.commit([changedProfile])).rejects.toBeInstanceOf(
      KafkaProfileFileWriteError,
    );
    await expect(failingStore.commit([changedProfile])).rejects.not.toThrow(
      /fixture-secret-never-on-disk/,
    );
    await expect(readFile(path)).resolves.toEqual(committed);
    expect(await readdir(join(path, ".."))).toEqual(["kafka-profiles.json"]);
  });

  it("preserves a corrupt original and reports no decrypted or protected content", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const store = new AtomicKafkaProfileFileStore(path, protector, capability);
    await store.commit([profile]);
    await chmod(path, 0o600);
    const corrupt = Buffer.from('{"version":1,"profiles":[{"protected":"secret-ciphertext"}]}');
    await writeFile(path, corrupt, { mode: 0o600 });

    const result = store.load();

    await expect(result).rejects.toBeInstanceOf(KafkaProfileFileCorruptError);
    await expect(result).rejects.not.toThrow(/secret-ciphertext|fixture-secret/);
    await expect(readFile(path)).resolves.toEqual(corrupt);
    expect(store.capability()).toMatchObject({
      durability: "durable",
      protection: "unavailable",
      state: "unavailable",
    });
  });

  it("rejects a profile document beyond its file bound before JSON parsing", async () => {
    const path = await temporaryProfilePath();
    const store = new AtomicKafkaProfileFileStore(path, new ReversibleProtector(), capability, {
      maximumFileBytes: 32,
    });
    await mkdir(join(path, ".."), { recursive: true });
    await writeFile(path, Buffer.alloc(33, 0x78), { mode: 0o600 });

    await expect(store.load()).rejects.toBeInstanceOf(KafkaProfileFileCorruptError);
    await expect(stat(path)).resolves.toBeDefined();
  });
});

describe("production credential persistence", () => {
  function secured(): KafkaProfileRecord {
    const { oauth: _oauth, ...base } = profile;
    void _oauth;
    return {
      ...base,
      transport: "tls",
      revision: 1,
      sasl: {
        mechanism: "SCRAM-SHA-512",
        username: "operator",
        password: "broker-password-private",
      },
      clientIdentity: {
        certificatePem: "client-certificate",
        privateKeyPem: "client-key-private",
        passphrase: "client-key-passphrase-private",
      },
      services: {
        schemaRegistry: {
          baseUrl: "https://registry.example.test",
          authentication: "basic",
          basic: { username: "registry-user", password: "registry-password-private" },
          trust: {
            mode: "custom",
            kind: "pem",
            label: "registry.pem",
            material: "registry-ca",
            password: "",
          },
        },
        connect: {
          baseUrl: "https://connect.example.test",
          authentication: "oauth-client",
          oauth: {
            clientId: "connect-client",
            clientSecret: "connect-secret-private",
            scope: "",
            tokenEndpoint: "https://connect-auth.example.test/token",
          },
          trust: { mode: "system" },
        },
      },
    };
  }

  it("preserves a legacy plugin/OAuth profile and exact rollback bytes when adding independent protected credentials", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const source = {
      kind: "plugin" as const,
      pluginId: "streamskope.eda",
      version: 1 as const,
      data: { sessionId: "owned-session" },
    };
    const legacy = { ...profile, id: "legacy-plugin", source, transport: "tls" as const };
    const store = new AtomicKafkaProfileFileStore(path, protector, capability);
    await store.commit([legacy]);
    const original = await readFile(path);
    await store.commit([legacy, secured()]);
    const encoded = await readFile(path, "utf8");
    const document = JSON.parse(encoded) as { version: number; rollbackGeneration: string };
    expect(document.version).toBe(4);
    expect(document.rollbackGeneration).toBe(`${path.split("/").at(-1)}.pre-security-v3`);
    expect(await readFile(join(path, "..", document.rollbackGeneration))).toEqual(original);
    expect((await stat(path)).mode & 0o777).toBe(0o600);
    for (const secret of [
      "broker-password-private",
      "client-key-private",
      "client-key-passphrase-private",
      "registry-password-private",
      "connect-secret-private",
    ])
      expect(encoded).not.toContain(secret);
    const restarted = new AtomicKafkaProfileFileStore(path, protector, capability);
    await expect(restarted.load()).resolves.toEqual([legacy, secured()]);
    await restarted.commit([legacy]);
    expect(JSON.parse(await readFile(path, "utf8"))).toMatchObject({ version: 4 });
    expect(await readFile(join(path, "..", document.rollbackGeneration))).toEqual(original);
  });

  it("rejects changed service destinations or secret-presence metadata without rewriting the file", async () => {
    interface MutableSecuritySummary {
      services: { schemaRegistry: { baseUrl: string } };
      sasl: { passwordPresent: boolean };
      clientIdentity: { privateKeyPresent: boolean };
    }
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    await new AtomicKafkaProfileFileStore(path, protector, capability).commit([secured()]);
    const original = await readFile(path, "utf8");
    for (const change of [
      (p: MutableSecuritySummary): void => {
        p.services.schemaRegistry.baseUrl = "https://other.example.test";
      },
      (p: MutableSecuritySummary): void => {
        p.sasl.passwordPresent = false;
      },
      (p: MutableSecuritySummary): void => {
        p.clientIdentity.privateKeyPresent = false;
      },
    ]) {
      const document = JSON.parse(original) as { profiles: [MutableSecuritySummary] };
      change(document.profiles[0]);
      const changed = JSON.stringify(document);
      await writeFile(path, changed);
      await expect(
        new AtomicKafkaProfileFileStore(path, protector, capability).load(),
      ).rejects.toThrow(KafkaProfileFileCorruptError);
      expect(await readFile(path, "utf8")).toBe(changed);
    }
  });

  it("round trips an explicitly plaintext SASL profile with independently trusted HTTPS services", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const tls = secured();
    const record: KafkaProfileRecord = {
      id: tls.id,
      name: tls.name,
      brokers: tls.brokers,
      createdAt: tls.createdAt,
      updatedAt: tls.updatedAt,
      transport: "plaintext",
      sasl: tls.sasl!,
      services: tls.services!,
    };
    await new AtomicKafkaProfileFileStore(path, protector, capability).commit([record]);
    await expect(
      new AtomicKafkaProfileFileStore(path, protector, capability).load(),
    ).resolves.toEqual([record]);
  });

  it("does not replace an existing generation when new credential protection fails", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    await new AtomicKafkaProfileFileStore(path, protector, capability).commit([profile]);
    const before = await readFile(path);
    const failed: KafkaProfileProtector = {
      protect: () => Promise.reject(new Error("backend-private-secret")),
      unprotect: (value) => protector.unprotect(value),
    };
    await expect(
      new AtomicKafkaProfileFileStore(path, failed, capability).commit([secured()]),
    ).rejects.toThrow(KafkaProfileFileWriteError);
    expect(await readFile(path)).toEqual(before);
  });
});

it("rejects oversized protected envelopes before replacing a readable profile file", async () => {
  const path = await temporaryProfilePath();
  const protector = new ReversibleProtector();
  await new AtomicKafkaProfileFileStore(path, protector, capability).commit([profile]);
  const before = await readFile(path);
  const oversized: KafkaProfileProtector = {
    protect: () => Promise.resolve(Buffer.alloc(32 * 1_048_576 + 1)),
    unprotect: (value) => protector.unprotect(value),
  };
  await expect(
    new AtomicKafkaProfileFileStore(path, oversized, capability).commit([profile]),
  ).rejects.toThrow(KafkaProfileFileWriteError);
  expect(await readFile(path)).toEqual(before);
});

it("preserves empty OAuth scopes across an unchanged encrypted profile restart", async () => {
  const path = await temporaryProfilePath();
  const protector = new ReversibleProtector();
  const scoped = { ...profile, oauth: { ...profile.oauth!, scope: "" } };
  await new AtomicKafkaProfileFileStore(path, protector, capability).commit([scoped]);
  await expect(
    new AtomicKafkaProfileFileStore(path, protector, capability).load(),
  ).resolves.toEqual([{ ...scoped, transport: "tls" }]);
});
