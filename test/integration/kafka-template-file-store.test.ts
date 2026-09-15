import { mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { DEFAULT_CONNECTION_TEMPLATE_DOCUMENT } from "../../src/kafka/application";
import {
  AtomicKafkaConnectionTemplateFileStore,
  KafkaConnectionTemplateFileCorruptError,
  KafkaConnectionTemplateFileWriteError,
} from "../../src/main/kafka-connection-template-file-store";

const temporaryDirectories: string[] = [];

async function temporaryTemplatePath(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-template-store-"));
  temporaryDirectories.push(directory);
  return join(directory, "application-data", "kafka-connection-templates.json");
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

describe("Kafka connection-template file store", () => {
  it("atomically persists and restores the exact plain document with restrictive modes", async () => {
    const path = await temporaryTemplatePath();
    const store = new AtomicKafkaConnectionTemplateFileStore(path, {
      createTempId: (): string => "commit-1",
    });

    await store.commit(DEFAULT_CONNECTION_TEMPLATE_DOCUMENT);

    await expect(store.load()).resolves.toEqual(DEFAULT_CONNECTION_TEMPLATE_DOCUMENT);
    const serialized = await readFile(path, "utf8");
    expect(JSON.parse(serialized)).toEqual({
      catalogs: DEFAULT_CONNECTION_TEMPLATE_DOCUMENT.catalogs,
      version: 1,
    });
    expect((await stat(path)).mode & 0o777).toBe(0o600);
    expect((await stat(join(path, ".."))).mode & 0o777).toBe(0o700);
    expect(await readdir(join(path, ".."))).toEqual(["kafka-connection-templates.json"]);
    expect(store.capability()).toEqual({
      durability: "durable",
      state: "ready",
    });
  });

  it("distinguishes a missing document from an intentionally empty stored catalog", async () => {
    const path = await temporaryTemplatePath();
    const store = new AtomicKafkaConnectionTemplateFileStore(path);
    await expect(store.load()).resolves.toBeUndefined();

    const empty = {
      catalogs: DEFAULT_CONNECTION_TEMPLATE_DOCUMENT.catalogs.map((catalog) =>
        catalog.catalog === "truststore-fetch"
          ? { ...catalog, entries: [], selectedName: null }
          : catalog,
      ),
    };
    await store.commit(empty);

    const restarted = new AtomicKafkaConnectionTemplateFileStore(path);
    await expect(restarted.load()).resolves.toEqual(empty);
  });

  it("preserves a corrupt or unsupported original and marks only template storage unavailable", async () => {
    const path = await temporaryTemplatePath();
    await mkdir(join(path, ".."), { recursive: true });
    const corrupt = Buffer.from(
      '{"version":2,"catalogs":[],"storagePath":"/private/templates.json"}',
    );
    await writeFile(path, corrupt, { mode: 0o600 });
    const store = new AtomicKafkaConnectionTemplateFileStore(path);

    const result = store.load();

    await expect(result).rejects.toBeInstanceOf(KafkaConnectionTemplateFileCorruptError);
    await expect(result).rejects.not.toThrow(/private|storagePath/);
    await expect(readFile(path)).resolves.toEqual(corrupt);
    expect(store.capability()).toEqual({
      durability: "durable",
      recovery:
        "Preserve the template file, restore a known-good copy, or move it aside after confirming a backup.",
      state: "unavailable",
    });
  });

  it("rejects an oversized document before JSON parsing and preserves it", async () => {
    const path = await temporaryTemplatePath();
    await mkdir(join(path, ".."), { recursive: true });
    const oversized = Buffer.alloc(33, 0x78);
    await writeFile(path, oversized, { mode: 0o600 });
    const store = new AtomicKafkaConnectionTemplateFileStore(path, {
      maximumFileBytes: 32,
    });

    await expect(store.load()).rejects.toBeInstanceOf(KafkaConnectionTemplateFileCorruptError);
    await expect(readFile(path)).resolves.toEqual(oversized);
  });

  it("leaves the prior commit authoritative when a temporary-file collision prevents a write", async () => {
    const path = await temporaryTemplatePath();
    const firstStore = new AtomicKafkaConnectionTemplateFileStore(path, {
      createTempId: (): string => "first",
    });
    await firstStore.commit(DEFAULT_CONNECTION_TEMPLATE_DOCUMENT);
    const committed = await readFile(path);
    const temporaryPath = join(join(path, ".."), `.${basename(path)}.blocked.tmp`);
    await writeFile(temporaryPath, "belongs-to-another-operation", { mode: 0o600 });
    const blockedStore = new AtomicKafkaConnectionTemplateFileStore(path, {
      createTempId: (): string => "blocked",
    });
    const changed = {
      catalogs: DEFAULT_CONNECTION_TEMPLATE_DOCUMENT.catalogs.map((catalog) =>
        catalog.catalog === "oauth-endpoint" ? { ...catalog, selectedName: null } : catalog,
      ),
    };

    await expect(blockedStore.commit(changed)).rejects.toBeInstanceOf(
      KafkaConnectionTemplateFileWriteError,
    );
    await expect(readFile(path)).resolves.toEqual(committed);
    await expect(readFile(temporaryPath, "utf8")).resolves.toBe("belongs-to-another-operation");
  });

  it("preserves a conflicting destination and removes its owned temporary file when rename fails", async () => {
    const path = await temporaryTemplatePath();
    await mkdir(path, { recursive: true });
    await writeFile(join(path, "preserve.txt"), "operator-owned", { mode: 0o600 });
    const store = new AtomicKafkaConnectionTemplateFileStore(path, {
      createTempId: (): string => "rename-failure",
    });

    await expect(store.commit(DEFAULT_CONNECTION_TEMPLATE_DOCUMENT)).rejects.toBeInstanceOf(
      KafkaConnectionTemplateFileWriteError,
    );

    await expect(readFile(join(path, "preserve.txt"), "utf8")).resolves.toBe("operator-owned");
    expect(await readdir(join(path, ".."))).toEqual(["kafka-connection-templates.json"]);
  });

  it("rejects invalid selection and unexpected entry fields from disk", async () => {
    const path = await temporaryTemplatePath();
    await mkdir(join(path, ".."), { recursive: true });
    const unsafeDocument = {
      catalogs: DEFAULT_CONNECTION_TEMPLATE_DOCUMENT.catalogs.map((catalog) =>
        catalog.catalog === "oauth-endpoint"
          ? {
              ...catalog,
              entries: catalog.entries.map((entry) => ({
                ...entry,
                commandOutput: "must-not-cross",
              })),
              selectedName: "missing",
            }
          : catalog,
      ),
      version: 1,
    };
    await writeFile(path, JSON.stringify(unsafeDocument), { mode: 0o600 });
    const store = new AtomicKafkaConnectionTemplateFileStore(path);

    await expect(store.load()).rejects.toBeInstanceOf(KafkaConnectionTemplateFileCorruptError);
  });
});
