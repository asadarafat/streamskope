import { mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import type { KafkaRuleDefinition } from "../../src/features/kafka/contracts";
import type { KafkaRuleDocument } from "../../src/features/kafka/application";
import {
  AtomicKafkaRuleFileStore,
  KafkaRuleFileCorruptError,
  KafkaRuleFileWriteError,
} from "../../src/platform/electron/main/kafka-rule-file-store";

const rule: KafkaRuleDefinition = {
  cooldownMs: 5_000,
  description: "Detect critical orders.",
  enabled: true,
  expression: '$.priority == "high"',
  level: "warn",
  name: "High priority",
  topic: "orders",
};

const document: KafkaRuleDocument = { rules: [rule] };
const temporaryDirectories: string[] = [];

async function temporaryUserData(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-rule-store-"));
  temporaryDirectories.push(directory);
  return join(directory, "application-data");
}

function rulePath(userData: string): string {
  return join(userData, "rules", "kafka-rules.json");
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

describe("Kafka rule file store", () => {
  it("distinguishes missing data, persists atomically and restores after restart", async () => {
    const userData = await temporaryUserData();
    const path = rulePath(userData);
    const store = new AtomicKafkaRuleFileStore(path, {
      createTempId: (): string => "commit-1",
    });

    await expect(store.load()).resolves.toBeUndefined();
    await expect(stat(path)).rejects.toMatchObject({ code: "ENOENT" });

    await store.commit(document);

    await expect(new AtomicKafkaRuleFileStore(path).load()).resolves.toEqual(document);
    expect(JSON.parse(await readFile(path, "utf8"))).toEqual({
      rules: document.rules,
      version: 1,
    });
    expect((await stat(path)).mode & 0o777).toBe(0o600);
    expect((await stat(dirname(path))).mode & 0o777).toBe(0o700);
    expect(await readdir(dirname(path))).toEqual(["kafka-rules.json"]);
    expect(store.capability()).toEqual({
      durability: "durable",
      state: "ready",
    });
  });

  it("preserves unsupported and oversized bytes while marking only rule storage unavailable", async () => {
    const userData = await temporaryUserData();
    const path = rulePath(userData);
    await mkdir(dirname(path), { recursive: true });
    const unsupported = Buffer.from(
      '{"version":2,"rules":[],"storagePath":"/private/operator-rules.json"}',
    );
    await writeFile(path, unsupported, { mode: 0o600 });
    const unsupportedStore = new AtomicKafkaRuleFileStore(path);

    const unsupportedLoad = unsupportedStore.load();

    await expect(unsupportedLoad).rejects.toBeInstanceOf(KafkaRuleFileCorruptError);
    await expect(unsupportedLoad).rejects.not.toThrow(/private|storagePath/);
    await expect(readFile(path)).resolves.toEqual(unsupported);
    expect(unsupportedStore.capability()).toEqual({
      durability: "durable",
      recovery:
        "Preserve the rule file, restore a known-good copy, or move it aside after confirming a backup.",
      state: "unavailable",
    });

    const oversized = Buffer.alloc(33, 0x78);
    await writeFile(path, oversized, { mode: 0o600 });
    const oversizedStore = new AtomicKafkaRuleFileStore(path, {
      maximumFileBytes: 32,
    });

    await expect(oversizedStore.load()).rejects.toBeInstanceOf(KafkaRuleFileCorruptError);
    await expect(readFile(path)).resolves.toEqual(oversized);
  });

  it("rejects duplicate, non-canonical and undeclared rule data from disk", async () => {
    const userData = await temporaryUserData();
    const path = rulePath(userData);
    await mkdir(dirname(path), { recursive: true });
    const invalidDocuments: readonly unknown[] = [
      { rules: [rule, { ...rule, name: " high PRIORITY " }], version: 1 },
      { rules: [{ ...rule, description: "", name: " High priority " }], version: 1 },
      {
        rules: [{ ...rule, evaluatedPayload: "must-not-cross-the-boundary" }],
        version: 1,
      },
      { rules: [rule], undeclared: true, version: 1 },
    ];

    for (const invalid of invalidDocuments) {
      await writeFile(path, JSON.stringify(invalid), { mode: 0o600 });
      const store = new AtomicKafkaRuleFileStore(path);
      await expect(store.load()).rejects.toBeInstanceOf(KafkaRuleFileCorruptError);
    }
  });

  it("leaves the prior document authoritative when its owned temporary write cannot start", async () => {
    const userData = await temporaryUserData();
    const path = rulePath(userData);
    const firstStore = new AtomicKafkaRuleFileStore(path, {
      createTempId: (): string => "first",
    });
    await firstStore.commit(document);
    const committed = await readFile(path);
    const temporaryPath = join(dirname(path), `.${basename(path)}.blocked.tmp`);
    await writeFile(temporaryPath, "operator-owned", { mode: 0o600 });
    const blockedStore = new AtomicKafkaRuleFileStore(path, {
      createTempId: (): string => "blocked",
    });

    await expect(
      blockedStore.commit({
        rules: [{ ...rule, enabled: false }],
      }),
    ).rejects.toBeInstanceOf(KafkaRuleFileWriteError);

    await expect(readFile(path)).resolves.toEqual(committed);
    await expect(readFile(temporaryPath, "utf8")).resolves.toBe("operator-owned");
  });

  it("removes its temporary file and preserves a conflicting destination on rename failure", async () => {
    const userData = await temporaryUserData();
    const path = rulePath(userData);
    await mkdir(path, { recursive: true });
    await writeFile(join(path, "preserve.txt"), "operator-owned", { mode: 0o600 });
    const store = new AtomicKafkaRuleFileStore(path, {
      createTempId: (): string => "rename-failure",
    });

    await expect(store.commit(document)).rejects.toBeInstanceOf(KafkaRuleFileWriteError);

    await expect(readFile(join(path, "preserve.txt"), "utf8")).resolves.toBe("operator-owned");
    expect(await readdir(dirname(path))).toEqual(["kafka-rules.json"]);
  });

  it("isolates corrupt rule data from profile and template stores", async () => {
    const userData = await temporaryUserData();
    const path = rulePath(userData);
    const profilePath = join(userData, "profiles", "kafka-profiles.json");
    const templatePath = join(userData, "templates", "kafka-connection-templates.json");
    await Promise.all([
      mkdir(dirname(path), { recursive: true }),
      mkdir(dirname(profilePath), { recursive: true }),
      mkdir(dirname(templatePath), { recursive: true }),
    ]);
    const corrupt = Buffer.from('{"version":1,"rules":"operator-secret"}');
    const profiles = Buffer.from("profile-bytes");
    const templates = Buffer.from("template-bytes");
    await Promise.all([
      writeFile(path, corrupt, { mode: 0o600 }),
      writeFile(profilePath, profiles, { mode: 0o600 }),
      writeFile(templatePath, templates, { mode: 0o600 }),
    ]);
    const store = new AtomicKafkaRuleFileStore(path);

    const result = store.load();

    await expect(result).rejects.toBeInstanceOf(KafkaRuleFileCorruptError);
    await expect(result).rejects.not.toThrow(/operator-secret/);
    await expect(readFile(path)).resolves.toEqual(corrupt);
    await expect(readFile(profilePath)).resolves.toEqual(profiles);
    await expect(readFile(templatePath)).resolves.toEqual(templates);
  });
});
