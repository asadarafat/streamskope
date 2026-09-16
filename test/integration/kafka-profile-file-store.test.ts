import { chmod, mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
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
} from "../../src/platform/electron/main/kafka-profile-file-store";

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

async function temporaryProfilePath(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-profile-store-"));
  temporaryDirectories.push(directory);
  return join(directory, "application-data", "kafka-profiles.json");
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

describe("Kafka profile file store", () => {
  it("reads the previous version-three binding payload without rewriting it", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const legacyProtector: KafkaProfileProtector = {
      protect: (plaintext) => {
        const value = JSON.parse(plaintext) as { version: number };
        return protector.protect(JSON.stringify({ ...value, version: 3 }));
      },
      unprotect: protector.unprotect.bind(protector),
    };
    const bound = {
      ...profile,
      revision: 1,
      binding: { recipe: { ...trustRecipeInput(), id: "legacy", revision: 1 }, overrides: {} },
    };
    await new AtomicKafkaProfileFileStore(path, legacyProtector, capability).commit([bound]);
    const before = await readFile(path);
    expect(await new AtomicKafkaProfileFileStore(path, protector, capability).load()).toEqual([
      bound,
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
      value,
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
    ).resolves.toEqual([bound]);
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
    ).resolves.toEqual([profile]);
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
    await expect(restarted.load()).resolves.toEqual([versioned]);
    expect(await readFile(path, "utf8")).not.toContain('"revision"');
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

  it("atomically persists and restores a complete protected record with restrictive modes", async () => {
    const path = await temporaryProfilePath();
    const protector = new ReversibleProtector();
    const store = new AtomicKafkaProfileFileStore(path, protector, capability, {
      createTempId: (): string => "commit-1",
    });

    await store.commit([profile]);

    await expect(store.load()).resolves.toEqual([profile]);
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
          trust: { materialPresent: true, passwordPresent: false },
        },
      ],
      version: 2,
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
