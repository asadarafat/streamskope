import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
} from "../../src/features/kafka/contracts";
import {
  createElectronKafkaBackend,
  type ElectronSafeStoragePort,
} from "../../src/platform/electron/main";

class ReversibleSafeStorage implements ElectronSafeStoragePort {
  decryptStringAsync(
    encrypted: Buffer,
  ): Promise<{ readonly result: string; readonly shouldReEncrypt: boolean }> {
    return Promise.resolve({
      result: Buffer.from(encrypted).reverse().toString("utf8"),
      shouldReEncrypt: false,
    });
  }

  encryptStringAsync(plainText: string): Promise<Buffer> {
    return Promise.resolve(Buffer.from(plainText, "utf8").reverse());
  }

  getSelectedStorageBackend(): "gnome_libsecret" {
    return "gnome_libsecret";
  }

  isAsyncEncryptionAvailable(): Promise<boolean> {
    return Promise.resolve(true);
  }
}

const temporaryDirectories: string[] = [];

async function temporaryUserData(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-electron-preferences-"));
  temporaryDirectories.push(directory);
  return directory;
}

function preferencePath(userDataPath: string): string {
  return join(userDataPath, "preferences", "kafka-operational-preferences.json");
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

describe("Electron operational-preference backend composition", () => {
  it("restores one complete confirmed preference document after backend restart", async () => {
    const userDataPath = await temporaryUserData();
    const safeStorage = new ReversibleSafeStorage();
    const first = await createElectronKafkaBackend({
      platform: "linux",
      safeStorage,
      userDataPath,
    });

    await expect(
      first.execute({
        command: "preferences.update",
        id: "save-preferences",
        payload: {
          patch: {
            fetch: { maxMessages: 100, mode: "newest" },
            rules: { logLevel: "warn", notificationsEnabled: false },
            stream: { batchSize: 100, queueDepth: 500 },
          },
        },
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({
      ok: true,
      result: {
        snapshot: {
          preferences: {
            fetch: { maxMessages: 100, mode: "newest" },
            rules: { logLevel: "warn", notificationsEnabled: false },
            stream: { batchSize: 100, queueDepth: 500 },
          },
          store: { durability: "durable", state: "ready" },
        },
      },
    });
    await first.shutdown();

    const second = await createElectronKafkaBackend({
      platform: "linux",
      safeStorage,
      userDataPath,
    });
    await expect(
      second.execute({
        command: "preferences.get",
        id: "restore-preferences",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({
      ok: true,
      result: {
        snapshot: {
          preferences: {
            fetch: { maxMessages: 100, mode: "newest" },
            rules: { logLevel: "warn", notificationsEnabled: false },
            stream: { batchSize: 100, queueDepth: 500 },
          },
          store: { durability: "durable", state: "ready" },
        },
      },
    });
    expect(JSON.parse(await readFile(preferencePath(userDataPath), "utf8"))).toMatchObject({
      version: 1,
    });
    await second.shutdown();
  });

  it("preserves corrupt bytes until reset and never mutates a neighboring store", async () => {
    const userDataPath = await temporaryUserData();
    const path = preferencePath(userDataPath);
    const neighboringRulePath = join(userDataPath, "rules", "kafka-rules.json");
    await Promise.all([
      mkdir(dirname(path), { recursive: true }),
      mkdir(dirname(neighboringRulePath), { recursive: true }),
    ]);
    const corrupt = '{"version":999,"preferences":"private-runbook"}';
    await writeFile(path, corrupt, { mode: 0o600 });
    await writeFile(neighboringRulePath, "operator-owned-rule-bytes", {
      mode: 0o600,
    });
    const backend = await createElectronKafkaBackend({
      platform: "linux",
      safeStorage: new ReversibleSafeStorage(),
      userDataPath,
    });

    await expect(
      backend.execute({
        command: "preferences.get",
        id: "load-corrupt",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({
      ok: true,
      result: {
        snapshot: {
          preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
          store: { durability: "durable", state: "unavailable" },
        },
      },
    });
    await expect(readFile(path, "utf8")).resolves.toBe(corrupt);

    await expect(
      backend.execute({
        command: "preferences.reset",
        id: "reset-corrupt",
        payload: {},
        version: HOST_PROTOCOL_VERSION,
      }),
    ).resolves.toMatchObject({
      ok: true,
      result: {
        snapshot: {
          preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
          store: { durability: "durable", state: "ready" },
        },
      },
    });
    await expect(readFile(neighboringRulePath, "utf8")).resolves.toBe("operator-owned-rule-bytes");
    expect(JSON.parse(await readFile(path, "utf8"))).toEqual({
      preferences: KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
      version: 1,
    });
    await backend.shutdown();
  });
});
