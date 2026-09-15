import { mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  KAFKA_TOPIC_CONFIGURATION_LIMITS,
  KAFKA_TOPIC_CONFIGURATION_REDACTION,
  type KafkaTopicConfigurationHistoryEntry,
} from "../../src/kafka/contracts";
import type { KafkaTopicConfigurationHistoryDocument } from "../../src/kafka/application";
import {
  AtomicKafkaTopicConfigurationHistoryFileStore,
  KafkaTopicConfigurationHistoryFileCorruptError,
  KafkaTopicConfigurationHistoryFileWriteError,
} from "../../src/main/kafka-topic-configuration-history-file-store";

const temporaryDirectories: string[] = [];

function historyEntry(index = 1): KafkaTopicConfigurationHistoryEntry {
  return {
    action: index % 2 === 0 ? "apply" : "validate",
    at: new Date(Date.UTC(2026, 6, 25, 12, 0, index)).toISOString(),
    changes: [
      {
        from: "86400000",
        isSensitive: false,
        name: "retention.ms",
        to: String(index),
        wasDefault: false,
      },
      {
        from: KAFKA_TOPIC_CONFIGURATION_REDACTION,
        isSensitive: true,
        name: "ssl.keystore.password",
        to: KAFKA_TOPIC_CONFIGURATION_REDACTION,
        wasDefault: false,
      },
    ],
    connectionName: "Local aio",
    connectionTarget: "localhost:19093",
    id: `history-${String(index)}`,
    success: true,
    topic: "orders.events",
  };
}

async function temporaryUserData(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-topic-history-"));
  temporaryDirectories.push(directory);
  return join(directory, "application-data");
}

function historyPath(userData: string): string {
  return join(userData, "history", "kafka-topic-configuration-history.json");
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

describe("Kafka topic-configuration history file store", () => {
  it("distinguishes missing history, commits privately and restores after restart", async () => {
    const path = historyPath(await temporaryUserData());
    const store = new AtomicKafkaTopicConfigurationHistoryFileStore(path, {
      createTempId: (): string => "commit-1",
    });
    const document: KafkaTopicConfigurationHistoryDocument = {
      entries: [historyEntry()],
    };

    await expect(store.load()).resolves.toBeUndefined();
    await expect(stat(path)).rejects.toMatchObject({ code: "ENOENT" });
    await store.commit(document);

    await expect(new AtomicKafkaTopicConfigurationHistoryFileStore(path).load()).resolves.toEqual(
      document,
    );
    expect(JSON.parse(await readFile(path, "utf8"))).toEqual({
      entries: document.entries,
      version: 1,
    });
    expect((await stat(path)).mode & 0o777).toBe(0o600);
    expect((await stat(dirname(path))).mode & 0o777).toBe(0o700);
    expect(await readdir(dirname(path))).toEqual(["kafka-topic-configuration-history.json"]);
    expect(store.capability()).toEqual({
      durability: "durable",
      state: "ready",
    });
  });

  it("accepts the exact bound and rejects oversized or undeclared documents", async () => {
    const path = historyPath(await temporaryUserData());
    const bounded: KafkaTopicConfigurationHistoryDocument = {
      entries: Array.from({ length: KAFKA_TOPIC_CONFIGURATION_LIMITS.historyEntries }, (_, index) =>
        historyEntry(index + 1),
      ),
    };
    const store = new AtomicKafkaTopicConfigurationHistoryFileStore(path);

    await expect(store.commit(bounded)).resolves.toBeUndefined();
    await expect(
      store.commit({
        entries: [...bounded.entries, historyEntry(51)],
      }),
    ).rejects.toBeInstanceOf(KafkaTopicConfigurationHistoryFileWriteError);

    await writeFile(
      path,
      JSON.stringify({
        entries: bounded.entries,
        storagePath: "/private/history.json",
        version: 1,
      }),
      { mode: 0o600 },
    );
    const invalid = new AtomicKafkaTopicConfigurationHistoryFileStore(path);
    const result = invalid.load();

    await expect(result).rejects.toBeInstanceOf(KafkaTopicConfigurationHistoryFileCorruptError);
    await expect(result).rejects.not.toThrow(/private|storagePath/);
  });

  it("rejects unredacted sensitive values and preserves corrupt or oversized source bytes", async () => {
    const path = historyPath(await temporaryUserData());
    await mkdir(dirname(path), { recursive: true });
    const exposed = {
      ...historyEntry(),
      changes: [
        {
          from: "old-secret",
          isSensitive: true,
          name: "ssl.keystore.password",
          to: "new-secret",
          wasDefault: false,
        },
      ],
    };
    const corrupt = Buffer.from(JSON.stringify({ entries: [exposed], version: 1 }));
    await writeFile(path, corrupt, { mode: 0o600 });
    const corruptStore = new AtomicKafkaTopicConfigurationHistoryFileStore(path);
    const corruptLoad = corruptStore.load();

    await expect(corruptLoad).rejects.toBeInstanceOf(
      KafkaTopicConfigurationHistoryFileCorruptError,
    );
    await expect(corruptLoad).rejects.not.toThrow(/old-secret|new-secret/);
    await expect(readFile(path)).resolves.toEqual(corrupt);
    expect(corruptStore.capability()).toMatchObject({
      durability: "durable",
      state: "unavailable",
    });

    const oversized = Buffer.alloc(65, 0x78);
    await writeFile(path, oversized, { mode: 0o600 });
    const oversizedStore = new AtomicKafkaTopicConfigurationHistoryFileStore(path, {
      maximumFileBytes: 64,
    });
    await expect(oversizedStore.load()).rejects.toBeInstanceOf(
      KafkaTopicConfigurationHistoryFileCorruptError,
    );
    await expect(readFile(path)).resolves.toEqual(oversized);
  });

  it("does not replace prior history when an owned temporary write cannot start", async () => {
    const path = historyPath(await temporaryUserData());
    const first = new AtomicKafkaTopicConfigurationHistoryFileStore(path, {
      createTempId: (): string => "first",
    });
    await first.commit({ entries: [historyEntry()] });
    const committed = await readFile(path);
    const temporaryPath = join(dirname(path), `.${basename(path)}.blocked.tmp`);
    await writeFile(temporaryPath, "operator-owned", { mode: 0o600 });
    const blocked = new AtomicKafkaTopicConfigurationHistoryFileStore(path, {
      createTempId: (): string => "blocked",
    });

    await expect(blocked.commit({ entries: [historyEntry(2)] })).rejects.toBeInstanceOf(
      KafkaTopicConfigurationHistoryFileWriteError,
    );
    await expect(readFile(path)).resolves.toEqual(committed);
    await expect(readFile(temporaryPath, "utf8")).resolves.toBe("operator-owned");
  });

  it("propagates cancellation without marking durable history corrupt", async () => {
    const path = historyPath(await temporaryUserData());
    const store = new AtomicKafkaTopicConfigurationHistoryFileStore(path);
    const controller = new AbortController();
    controller.abort();

    await expect(store.load(controller.signal)).rejects.toMatchObject({
      name: "AbortError",
    });
    await expect(
      store.commit({ entries: [historyEntry()] }, controller.signal),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(store.capability()).toEqual({
      durability: "durable",
      state: "ready",
    });
    await expect(stat(path)).rejects.toMatchObject({ code: "ENOENT" });
  });
});
