import { mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  type KafkaOperationalPreferences,
} from "../../src/kafka/contracts";
import {
  AtomicKafkaOperationalPreferenceFileStore,
  KafkaOperationalPreferenceFileCorruptError,
  KafkaOperationalPreferenceFileWriteError,
} from "../../src/main/kafka-operational-preference-file-store";

const temporaryDirectories: string[] = [];

async function temporaryUserData(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-preference-store-"));
  temporaryDirectories.push(directory);
  return join(directory, "application-data");
}

function preferencePath(userData: string): string {
  return join(userData, "preferences", "kafka-operational-preferences.json");
}

function changedPreferences(): KafkaOperationalPreferences {
  return {
    fetch: { maxMessages: 100, mode: "newest" },
    latency: {
      acknowledgements: -1,
      messageCount: 50,
      runbookUrl: "https://runbooks.example.test/kafka/latency",
      timeoutMs: 20_000,
    },
    rules: {
      logLevel: "warn",
      loggingEnabled: false,
      notificationsEnabled: false,
    },
    stream: {
      batchSize: 100,
      historySamples: 120,
      intervalMs: 50,
      queueDepth: 500,
    },
  };
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

describe("Kafka operational-preference file store", () => {
  it("does not create a missing file, then atomically restores an exact versioned document", async () => {
    const path = preferencePath(await temporaryUserData());
    const store = new AtomicKafkaOperationalPreferenceFileStore(path, {
      createTempId: (): string => "commit-1",
    });

    await expect(store.load()).resolves.toBeUndefined();
    await expect(stat(path)).rejects.toMatchObject({ code: "ENOENT" });

    const preferences = changedPreferences();
    await store.commit(preferences);

    await expect(new AtomicKafkaOperationalPreferenceFileStore(path).load()).resolves.toEqual(
      preferences,
    );
    expect(JSON.parse(await readFile(path, "utf8"))).toEqual({
      preferences,
      version: 1,
    });
    expect((await stat(path)).mode & 0o777).toBe(0o600);
    expect((await stat(dirname(path))).mode & 0o777).toBe(0o700);
    expect(await readdir(dirname(path))).toEqual(["kafka-operational-preferences.json"]);
    expect(store.capability()).toEqual({ durability: "durable", state: "ready" });
  });

  it("preserves unsupported and oversized bytes and reports isolated recovery", async () => {
    const path = preferencePath(await temporaryUserData());
    await mkdir(dirname(path), { recursive: true });
    const unsupported = Buffer.from(
      '{"version":2,"preferences":{},"storagePath":"/private/operator.json"}',
    );
    await writeFile(path, unsupported, { mode: 0o600 });
    const unsupportedStore = new AtomicKafkaOperationalPreferenceFileStore(path);

    const unsupportedLoad = unsupportedStore.load();
    await expect(unsupportedLoad).rejects.toBeInstanceOf(
      KafkaOperationalPreferenceFileCorruptError,
    );
    await expect(unsupportedLoad).rejects.not.toThrow(/private|storagePath/);
    await expect(readFile(path)).resolves.toEqual(unsupported);
    expect(unsupportedStore.capability()).toEqual({
      durability: "durable",
      recovery: "Reset operational preferences to replace the unreadable file.",
      state: "unavailable",
    });

    const oversized = Buffer.alloc(33, 0x78);
    await writeFile(path, oversized, { mode: 0o600 });
    const oversizedStore = new AtomicKafkaOperationalPreferenceFileStore(path, {
      maximumFileBytes: 32,
    });
    await expect(oversizedStore.load()).rejects.toBeInstanceOf(
      KafkaOperationalPreferenceFileCorruptError,
    );
    await expect(readFile(path)).resolves.toEqual(oversized);
  });

  it("rejects unknown, unsafe, and out-of-range preference knowledge from disk", async () => {
    const path = preferencePath(await temporaryUserData());
    await mkdir(dirname(path), { recursive: true });
    const preferences = changedPreferences();
    const invalidDocuments: readonly unknown[] = [
      { preferences, unknown: true, version: 1 },
      {
        preferences: {
          ...preferences,
          latency: { ...preferences.latency, runbookUrl: "file:///private/runbook" },
        },
        version: 1,
      },
      {
        preferences: {
          ...preferences,
          stream: { ...preferences.stream, queueDepth: 10_000 },
        },
        version: 1,
      },
      {
        preferences: {
          ...preferences,
          rules: { ...preferences.rules, expression: "payload.secret == true" },
        },
        version: 1,
      },
    ];

    for (const invalid of invalidDocuments) {
      await writeFile(path, JSON.stringify(invalid), { mode: 0o600 });
      await expect(
        new AtomicKafkaOperationalPreferenceFileStore(path).load(),
      ).rejects.toBeInstanceOf(KafkaOperationalPreferenceFileCorruptError);
    }
  });

  it("keeps the prior document when a temporary collision blocks a commit", async () => {
    const path = preferencePath(await temporaryUserData());
    await new AtomicKafkaOperationalPreferenceFileStore(path, {
      createTempId: (): string => "first",
    }).commit(changedPreferences());
    const committed = await readFile(path);
    const temporaryPath = join(dirname(path), `.${basename(path)}.blocked.tmp`);
    await writeFile(temporaryPath, "operator-owned", { mode: 0o600 });
    const blocked = new AtomicKafkaOperationalPreferenceFileStore(path, {
      createTempId: (): string => "blocked",
    });

    await expect(blocked.commit(KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS)).rejects.toBeInstanceOf(
      KafkaOperationalPreferenceFileWriteError,
    );
    await expect(readFile(path)).resolves.toEqual(committed);
    await expect(readFile(temporaryPath, "utf8")).resolves.toBe("operator-owned");
  });

  it("removes its owned temporary file when destination replacement fails", async () => {
    const path = preferencePath(await temporaryUserData());
    await mkdir(path, { recursive: true });
    await writeFile(join(path, "preserve.txt"), "operator-owned", { mode: 0o600 });
    const store = new AtomicKafkaOperationalPreferenceFileStore(path, {
      createTempId: (): string => "rename-failure",
    });

    await expect(store.commit(changedPreferences())).rejects.toBeInstanceOf(
      KafkaOperationalPreferenceFileWriteError,
    );
    await expect(readFile(join(path, "preserve.txt"), "utf8")).resolves.toBe("operator-owned");
    expect(await readdir(dirname(path))).toEqual(["kafka-operational-preferences.json"]);
  });

  it("replaces only the corrupt preference file when explicit service reset commits defaults", async () => {
    const userData = await temporaryUserData();
    const path = preferencePath(userData);
    const neighbors = [
      join(userData, "profiles", "kafka-profiles.json"),
      join(userData, "rules", "kafka-rules.json"),
      join(userData, "templates", "kafka-connection-templates.json"),
      join(userData, "history", "kafka-topic-configuration-history.json"),
    ];
    await Promise.all(
      [path, ...neighbors].map((entry) => mkdir(dirname(entry), { recursive: true })),
    );
    const corrupt = Buffer.from('{"version":1,"preferences":"operator-secret"}');
    await writeFile(path, corrupt, { mode: 0o600 });
    await Promise.all(
      neighbors.map((entry, index) =>
        writeFile(entry, `neighbor-${String(index)}`, { mode: 0o600 }),
      ),
    );
    const store = new AtomicKafkaOperationalPreferenceFileStore(path);

    await expect(store.load()).rejects.toBeInstanceOf(KafkaOperationalPreferenceFileCorruptError);
    await store.commit(KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS);

    expect(store.capability()).toEqual({ durability: "durable", state: "ready" });
    await expect(store.load()).resolves.toEqual(KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS);
    await Promise.all(
      neighbors.map(async (entry, index) => {
        await expect(readFile(entry, "utf8")).resolves.toBe(`neighbor-${String(index)}`);
      }),
    );
  });
});
