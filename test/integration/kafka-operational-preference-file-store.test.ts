import { renameSync, symlinkSync, writeFileSync } from "node:fs";
import {
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  stat,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
  type KafkaOperationalPreferences,
} from "../../src/features/kafka/contracts";
import {
  AtomicKafkaOperationalPreferenceFileStore,
  KafkaOperationalPreferenceFileCorruptError,
  KafkaOperationalPreferenceFileWriteError,
} from "../../src/platform/node/kafka-operational-preference-file-store";

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
    codecs: { key: "auto", value: "auto" },
    protection: { readOnly: false, maskKey: false, maskHeaders: [], valuePaths: [] },
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
  it("migrates legacy codec defaults, preserves exact predecessor bytes and restores overrides after restart", async () => {
    const path = preferencePath(await temporaryUserData());
    await mkdir(dirname(path), { recursive: true });
    const { codecs, ...legacy } = changedPreferences();
    expect(codecs).toEqual({ key: "auto", value: "auto" });
    const predecessor = Buffer.from(
      `${JSON.stringify({ version: 1, preferences: legacy }, null, 2)}\n`,
    );
    await writeFile(path, predecessor, { mode: 0o600 });
    const store = new AtomicKafkaOperationalPreferenceFileStore(path);
    expect((await store.load())?.codecs).toEqual({ key: "auto", value: "auto" });
    const preferences = {
      ...changedPreferences(),
      codecs: { key: "utf8", value: "protobuf" },
    } as const;
    await store.commit(preferences);
    expect(await readFile(`${path}.pre-codecs-v1`)).toEqual(predecessor);
    expect((await stat(`${path}.pre-codecs-v1`)).mode & 0o777).toBe(0o600);
    expect(JSON.parse(await readFile(path, "utf8"))).toEqual({ version: 2, preferences });
    expect(await new AtomicKafkaOperationalPreferenceFileStore(path).load()).toEqual(preferences);
    await store.commit({ ...preferences, codecs: { key: "bytes", value: "json" } });
    expect(await readFile(`${path}.pre-codecs-v1`)).toEqual(predecessor);
    expect(await readdir(dirname(path))).toEqual([
      "kafka-operational-preferences.json",
      "kafka-operational-preferences.json.pre-codecs-v1",
    ]);
  });

  it("refuses unknown codec settings and incomplete format 2 documents without altering stored bytes", async () => {
    const path = preferencePath(await temporaryUserData());
    await mkdir(dirname(path), { recursive: true });
    for (const codecs of [{ key: "auto", value: "guess" }, { key: "json" }, undefined]) {
      const { codecs: omittedCodecs, ...rest } = changedPreferences();
      expect(omittedCodecs).toEqual({ key: "auto", value: "auto" });
      const bytes = Buffer.from(
        JSON.stringify({ version: 2, preferences: { ...rest, ...(codecs ? { codecs } : {}) } }),
      );
      await writeFile(path, bytes);
      await expect(
        new AtomicKafkaOperationalPreferenceFileStore(path).load(),
      ).rejects.toBeInstanceOf(KafkaOperationalPreferenceFileCorruptError);
      expect(await readFile(path)).toEqual(bytes);
    }
  });

  it("requires explicit format 2 protection settings while preserving the legacy default", async () => {
    const path = preferencePath(await temporaryUserData());
    await mkdir(dirname(path), { recursive: true });
    const { protection, ...withoutProtection } = changedPreferences();
    expect(protection).toEqual(KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS.protection);
    const bytes = JSON.stringify({ version: 2, preferences: withoutProtection });
    await writeFile(path, bytes);
    await expect(new AtomicKafkaOperationalPreferenceFileStore(path).load()).rejects.toBeInstanceOf(
      KafkaOperationalPreferenceFileCorruptError,
    );
    expect(await readFile(path, "utf8")).toBe(bytes);

    const { codecs, ...legacy } = withoutProtection;
    expect(codecs).toEqual({ key: "auto", value: "auto" });
    await writeFile(path, JSON.stringify({ version: 1, preferences: legacy }));
    expect(await new AtomicKafkaOperationalPreferenceFileStore(path).load()).toEqual(
      changedPreferences(),
    );
  });

  it("resets an oversized corrupt regular file and restores durable preferences after restart", async () => {
    const path = preferencePath(await temporaryUserData());
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, Buffer.alloc(33 * 1_024, 0x78));
    const store = new AtomicKafkaOperationalPreferenceFileStore(path);
    await expect(store.load()).rejects.toBeInstanceOf(KafkaOperationalPreferenceFileCorruptError);
    await store.commit(KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS);
    expect(await new AtomicKafkaOperationalPreferenceFileStore(path).load()).toEqual(
      KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS,
    );
    expect(store.capability()).toEqual({ durability: "durable", state: "ready" });
    expect(await readdir(dirname(path))).toEqual([basename(path)]);
  });

  it("refuses to reset an oversized file through a symlink", async () => {
    const path = preferencePath(await temporaryUserData());
    await mkdir(dirname(path), { recursive: true });
    const target = `${path}.outside`;
    const bytes = Buffer.alloc(33 * 1_024, 0x78);
    await writeFile(target, bytes);
    await symlink(target, path);
    await expect(
      new AtomicKafkaOperationalPreferenceFileStore(path).commit(changedPreferences()),
    ).rejects.toBeInstanceOf(KafkaOperationalPreferenceFileWriteError);
    expect((await lstat(path)).isSymbolicLink()).toBe(true);
    expect(await readFile(target)).toEqual(bytes);
  });

  it.each(["regular", "symlink"])(
    "does not reset a %s replacement raced into an oversized reset",
    async (replacement) => {
      const path = preferencePath(await temporaryUserData());
      await mkdir(dirname(path), { recursive: true });
      const oversized = Buffer.alloc(33 * 1_024, 0x78);
      await writeFile(path, oversized);
      const { codecs, ...legacy } = changedPreferences();
      expect(codecs).toEqual({ key: "auto", value: "auto" });
      const replacementBytes = JSON.stringify({ version: 1, preferences: legacy });
      const target = `${path}.outside`;
      await writeFile(target, replacementBytes);
      const store = new AtomicKafkaOperationalPreferenceFileStore(path, {
        createTempId: (): string => {
          renameSync(path, `${path}.original`);
          if (replacement === "symlink") symlinkSync(target, path);
          else writeFileSync(path, replacementBytes);
          return "race";
        },
      });
      await expect(store.commit(KAFKA_OPERATIONAL_PREFERENCE_DEFAULTS)).rejects.toBeInstanceOf(
        KafkaOperationalPreferenceFileWriteError,
      );
      expect((await lstat(path)).isSymbolicLink()).toBe(replacement === "symlink");
      expect(await readFile(path, "utf8")).toBe(replacementBytes);
      expect(await readFile(target, "utf8")).toBe(replacementBytes);
      expect(await readFile(`${path}.original`)).toEqual(oversized);
      expect((await readdir(dirname(path))).filter((name) => name.endsWith(".tmp"))).toEqual([]);
    },
  );

  it("preserves earlier migration backups when a restored legacy file is upgraded again", async () => {
    const path = preferencePath(await temporaryUserData());
    await mkdir(dirname(path), { recursive: true });
    const { codecs, ...legacy } = changedPreferences();
    expect(codecs).toEqual({ key: "auto", value: "auto" });
    const first = JSON.stringify({ version: 1, preferences: legacy });
    const second = JSON.stringify({
      version: 1,
      preferences: { ...legacy, fetch: { maxMessages: 25, mode: "tail" } },
    });
    const store = new AtomicKafkaOperationalPreferenceFileStore(path);
    await writeFile(path, first);
    await store.commit(changedPreferences());
    await writeFile(path, second);
    await store.commit(changedPreferences());
    expect(await readFile(`${path}.pre-codecs-v1`, "utf8")).toBe(first);
    expect(await readFile(`${path}.pre-codecs-v1.1`, "utf8")).toBe(second);
  });

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
      version: 2,
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
