import { writeFileSync } from "node:fs";
import {
  link,
  mkdir,
  mkdtemp,
  open,
  readFile,
  readdir,
  rm,
  stat,
  symlink,
  unlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";

import { afterEach, expect, it, vi } from "vitest";

import { KafkaQueryLibrary } from "../../src/features/kafka/application";
import type { KafkaSavedView } from "../../src/features/kafka/contracts";
import { AtomicKafkaQueryFileStore } from "../../src/platform/node/kafka-query-file-store";

const directories: string[] = [];
async function location(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-view-migration-"));
  directories.push(directory);
  return join(directory, "kafka-queries.json");
}
afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});
const legacyEntry = {
  id: "incident-1",
  name: "Prior incident",
  profileId: "local-profile",
  configuration: {
    schemaVersion: 1,
    request: { topic: "events", mode: "earliest", maxMessages: 100 },
  },
} as const;
const defaults = {
  schemaVersion: 1,
  destination: { kind: "topic", workspace: "messages" },
  messages: {
    visibleColumns: ["timestamp", "key", "preview", "partition", "offset", "rules"],
    columnWidths: [],
    inspectorWidth: 320,
    filtersOpen: false,
  },
} as const;
const canonical: KafkaSavedView = {
  ...legacyEntry,
  view: defaults,
  records: { selected: null, comparison: null, bookmarks: [] },
};
const legacyBytes = ` { "schemaVersion": 1, "queries": [\n${JSON.stringify(legacyEntry)}\n] }\n`;
async function seed(file: string): Promise<void> {
  await writeFile(file, legacyBytes, { mode: 0o600 });
}
async function syncDirectory(path: string): Promise<void> {
  if (process.platform === "win32") return;
  const handle = await open(path, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

it("loads exact legacy bytes without migration, including a no-change save and absent delete", async () => {
  const file = await location();
  await seed(file);
  const before = await stat(file);
  const store = new AtomicKafkaQueryFileStore(file);
  const commit = vi.spyOn(store, "commit");
  const library = new KafkaQueryLibrary(store);
  expect(await library.list()).toEqual({ durability: "durable", queries: [canonical] });
  await library.put(canonical);
  await library.delete("absent");
  expect(commit).not.toHaveBeenCalled();
  expect(await readFile(file, "utf8")).toBe(legacyBytes);
  expect(await readdir(dirname(file))).toEqual([basename(file)]);
  const after = await stat(file);
  expect([after.ino, after.mtimeMs, after.ctimeMs, after.mode]).toEqual([
    before.ino,
    before.mtimeMs,
    before.ctimeMs,
    before.mode,
  ]);
});

it("preserves the exact private predecessor once, then reconstructs topic and standalone-group views from format 3", async () => {
  const file = await location();
  await seed(file);
  const library = new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(file));
  const changed: KafkaSavedView = {
    ...canonical,
    view: {
      ...defaults,
      destination: { kind: "topic", workspace: "monitor" },
      messages: {
        ...defaults.messages,
        visibleColumns: ["key", "preview"],
        columnWidths: [{ column: "preview", pixels: 410 }],
        inspectorWidth: 480,
        filtersOpen: true,
      },
    },
  };
  await library.put(changed);
  const group: KafkaSavedView = {
    id: "group",
    name: "Group only",
    configuration: null,
    records: { selected: null, comparison: null, bookmarks: [] },
    view: { ...defaults, destination: { kind: "consumer-group", groupId: "payments" } },
  };
  await library.put(group);
  expect(JSON.parse(await readFile(file, "utf8"))).toEqual({
    schemaVersion: 3,
    queries: [
      { ...legacyEntry, view: changed.view },
      { id: group.id, name: group.name, configuration: null, view: group.view },
    ],
  });
  expect(await readFile(`${file}.pre-views-v1`, "utf8")).toBe(legacyBytes);
  expect((await readdir(dirname(file))).sort()).toEqual(
    [basename(file), `${basename(file)}.pre-views-v1`].sort(),
  );
  expect(await new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(file)).list()).toEqual({
    durability: "durable",
    queries: [changed, group],
  });
  if (process.platform !== "win32") {
    expect((await stat(file)).mode & 0o777).toBe(0o600);
    expect((await stat(`${file}.pre-views-v1`)).mode & 0o777).toBe(0o600);
    expect((await stat(dirname(file))).mode & 0o777).toBe(0o700);
  }
});

it("does not create missing storage on reads and writes fresh format 3 without a fabricated backup", async () => {
  const file = await location();
  const library = new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(file));
  expect((await library.list()).queries).toEqual([]);
  expect(await readdir(dirname(file))).toEqual([]);
  await library.put(canonical);
  expect(JSON.parse(await readFile(file, "utf8"))).toMatchObject({ schemaVersion: 3 });
  expect(await readdir(dirname(file))).toEqual([basename(file)]);
});

it("never overwrites a different predecessor and reuses a matching verified backup after interruption", async () => {
  const file = await location();
  await seed(file);
  const earlier = JSON.stringify({ schemaVersion: 1, queries: [] });
  await writeFile(`${file}.pre-views-v1`, earlier, { mode: 0o600 });
  const temporary = join(dirname(file), `.${basename(file)}.blocked.tmp`);
  await writeFile(temporary, "occupied", { mode: 0o600 });
  const library = new KafkaQueryLibrary(
    new AtomicKafkaQueryFileStore(file, { createTempId: (): string => "blocked" }),
  );
  await expect(library.put({ ...canonical, name: "Changed" })).rejects.toThrow("not replaced");
  expect(await readFile(file, "utf8")).toBe(legacyBytes);
  expect(await readFile(`${file}.pre-views-v1.1`, "utf8")).toBe(legacyBytes);
  await unlink(temporary);
  await library.put({ ...canonical, name: "Changed" });
  expect(await readFile(`${file}.pre-views-v1`, "utf8")).toBe(earlier);
  expect((await readdir(dirname(file))).sort()).toEqual(
    [basename(file), `${basename(file)}.pre-views-v1`, `${basename(file)}.pre-views-v1.1`].sort(),
  );
});

it("retains the original when backup directory durability fails, and retries its verified predecessor", async () => {
  const file = await location();
  await seed(file);
  const flush = vi.fn(syncDirectory).mockRejectedValueOnce(new Error("private backup failure"));
  const library = new KafkaQueryLibrary(
    new AtomicKafkaQueryFileStore(file, { syncDirectory: flush }),
  );
  await expect(library.put({ ...canonical, name: "Changed" })).rejects.toThrow("not replaced");
  expect(await readFile(file, "utf8")).toBe(legacyBytes);
  expect(await readFile(`${file}.pre-views-v1`, "utf8")).toBe(legacyBytes);
  await library.put({ ...canonical, name: "Changed" });
  expect((await readdir(dirname(file))).length).toBe(2);
});

it("reports uncertainty after a real replacement when directory sync fails and permits authoritative readback", async () => {
  const file = await location();
  await seed(file);
  let calls = 0;
  const store = new AtomicKafkaQueryFileStore(file, {
    syncDirectory: async (path): Promise<void> => {
      await syncDirectory(path);
      if (++calls === 2) throw new Error("sentinel-password-in-private-path");
    },
  });
  const library = new KafkaQueryLibrary(store);
  const changed = { ...canonical, name: "Committed despite sync failure" };
  const failure = await library.put(changed).catch((error: unknown) => error);
  expect(failure).toBeInstanceOf(Error);
  expect(String(failure)).toMatch(/replacement occurred.*could not be confirmed/u);
  expect(String(failure)).not.toContain("sentinel-password");
  expect(JSON.parse(await readFile(file, "utf8"))).toEqual({
    schemaVersion: 3,
    queries: [{ ...legacyEntry, name: changed.name }],
  });
  expect(await readFile(`${file}.pre-views-v1`, "utf8")).toBe(legacyBytes);
  expect((await library.list()).queries).toEqual([changed]);
  await library.put(changed);
  expect(calls).toBe(2);
});

it("rejects a changed loaded predecessor before creating backups or replacing the file", async () => {
  const file = await location();
  await seed(file);
  const store = new AtomicKafkaQueryFileStore(file);
  await store.load();
  const replacement = JSON.stringify({ schemaVersion: 1, queries: [] });
  await writeFile(file, replacement);
  await expect(store.commit([{ ...canonical, name: "Stale" }])).rejects.toThrow("not replaced");
  expect(await readFile(file, "utf8")).toBe(replacement);
  expect(await readdir(dirname(file))).toEqual([basename(file)]);
});

it("rechecks the predecessor after preparing the replacement and preserves external changes", async () => {
  const file = await location();
  await seed(file);
  const external = JSON.stringify({ schemaVersion: 1, queries: [] });
  const library = new KafkaQueryLibrary(
    new AtomicKafkaQueryFileStore(file, {
      createTempId: (): string => {
        writeFileSync(file, external);
        return "race";
      },
    }),
  );
  await expect(library.put({ ...canonical, name: "Stale" })).rejects.toThrow("not replaced");
  expect(await readFile(file, "utf8")).toBe(external);
  expect(await readFile(`${file}.pre-views-v1`, "utf8")).toBe(legacyBytes);
  expect((await readdir(dirname(file))).some((entry) => entry.endsWith(".tmp"))).toBe(false);
});

it("does not overwrite a library created after an initially absent load", async () => {
  const file = await location();
  const store = new AtomicKafkaQueryFileStore(file);
  expect(await store.load()).toEqual([]);
  await seed(file);
  await expect(store.commit([])).rejects.toThrow("not replaced");
  expect(await readFile(file, "utf8")).toBe(legacyBytes);
  expect(await readdir(dirname(file))).toEqual([basename(file)]);
});

it("verifies the predecessor backup again at the replacement boundary", async () => {
  const file = await location();
  await seed(file);
  const replacement = JSON.stringify({ schemaVersion: 1, queries: [] });
  const library = new KafkaQueryLibrary(
    new AtomicKafkaQueryFileStore(file, {
      createTempId: (): string => {
        writeFileSync(`${file}.pre-views-v1`, replacement);
        return "backup-race";
      },
    }),
  );
  await expect(library.put({ ...canonical, name: "Changed" })).rejects.toThrow("not replaced");
  expect(await readFile(file, "utf8")).toBe(legacyBytes);
  expect(await readFile(`${file}.pre-views-v1`, "utf8")).toBe(replacement);
});

it.skipIf(process.platform === "win32")(
  "does not follow a predecessor symlink or the library directory symlink",
  async () => {
    const file = await location();
    await seed(file);
    const target = `${file}.target`;
    await seed(target);
    await symlink(target, `${file}.pre-views-v1`);
    await expect(
      new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(file)).put({
        ...canonical,
        name: "Changed",
      }),
    ).rejects.toThrow("not replaced");
    expect(await readFile(file, "utf8")).toBe(legacyBytes);
    expect(await readFile(target, "utf8")).toBe(legacyBytes);
    const linkedDirectory = join(dirname(file), "linked");
    await symlink(dirname(file), linkedDirectory);
    await expect(
      new AtomicKafkaQueryFileStore(join(linkedDirectory, basename(file))).load(),
    ).rejects.toThrow("unreadable");
  },
);

it.each(["corrupt", "future", "canonical-in-legacy", "oversized"] as const)(
  "never replaces %s source storage",
  async (kind) => {
    const file = await location();
    const source =
      kind === "corrupt"
        ? "{"
        : kind === "future"
          ? JSON.stringify({ schemaVersion: 4, queries: [] })
          : kind === "canonical-in-legacy"
            ? JSON.stringify({ schemaVersion: 1, queries: [canonical] })
            : "x".repeat(1_048_577);
    await writeFile(file, source);
    const library = new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(file));
    await expect(library.put(canonical)).rejects.toThrow("unreadable");
    expect(await readFile(file, "utf8")).toBe(source);
    expect(await readdir(dirname(file))).toEqual([basename(file)]);
  },
);

it.skipIf(process.platform === "win32").each(["symlink", "hardlink", "directory"] as const)(
  "rejects an unsafe %s source without following or replacing it",
  async (kind) => {
    const file = await location();
    const target = `${file}.target`;
    await seed(target);
    if (kind === "symlink") await symlink(target, file);
    else if (kind === "hardlink") await link(target, file);
    else await mkdir(file);
    await expect(new AtomicKafkaQueryFileStore(file).load()).rejects.toThrow("unreadable");
    await expect(new AtomicKafkaQueryFileStore(file).commit([canonical])).rejects.toThrow(
      "not replaced",
    );
    expect(await readFile(target, "utf8")).toBe(legacyBytes);
  },
);

it.each(["corrupt", "current-format", "capacity"] as const)(
  "does not replace a legacy source when predecessor storage is %s",
  async (kind) => {
    const file = await location();
    await seed(file);
    const backup = `${file}.pre-views-v1`;
    if (kind === "capacity") {
      for (let index = 0; index < 100; index++)
        await writeFile(
          `${backup}${index === 0 ? "" : `.${index}`}`,
          JSON.stringify({ schemaVersion: 1, queries: [] }),
          { mode: 0o600 },
        );
    } else
      await writeFile(
        backup,
        kind === "corrupt" ? "{" : JSON.stringify({ schemaVersion: 2, queries: [] }),
        { mode: 0o600 },
      );
    await expect(
      new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(file)).put({
        ...canonical,
        name: "Changed",
      }),
    ).rejects.toThrow("not replaced");
    expect(await readFile(file, "utf8")).toBe(legacyBytes);
  },
);

it("keeps near-capacity legacy libraries readable and mutable using compact defaults, while enforcing the nondefault storage limit", async () => {
  const file = await location();
  const make = (length: number): { schemaVersion: number; queries: unknown[] } => {
    const filter = {
      key: "\0".repeat(length),
      value: "\0".repeat(length),
      offset: "\0".repeat(length),
      timestamp: "\0".repeat(length),
      partition: null,
    };
    return {
      schemaVersion: 1,
      queries: Array.from({ length: 100 }, (_, index) => ({
        ...legacyEntry,
        id: String(index),
        name: `View ${index}`,
        configuration: {
          ...legacyEntry.configuration,
          filters: filter,
          request: { ...legacyEntry.configuration.request, mode: "earliest", search: filter },
        },
      })),
    };
  };
  let size = 0;
  while (size < 256 && Buffer.byteLength(JSON.stringify(make(size + 1))) <= 1_048_576) size++;
  const source = JSON.stringify(make(size));
  expect(Buffer.byteLength(source)).toBeGreaterThan(1_040_000);
  await writeFile(file, source, { mode: 0o600 });
  const store = new AtomicKafkaQueryFileStore(file);
  const library = new KafkaQueryLibrary(store);
  const loaded = await library.list();
  expect(loaded.queries).toHaveLength(100);
  expect(
    Buffer.byteLength(JSON.stringify({ schemaVersion: 2, queries: loaded.queries })),
  ).toBeGreaterThan(1_048_576);
  await library.put(loaded.queries[0]!);
  await expect(
    store.commit(
      loaded.queries.map((view) => ({
        ...view,
        view: { ...view.view, messages: { ...view.view.messages, filtersOpen: true } },
      })),
    ),
  ).rejects.toThrow("1 MiB");
  expect(await readFile(file, "utf8")).toBe(source);
  expect(await readdir(dirname(file))).toEqual([basename(file)]);
  const changed = { ...loaded.queries[0]!, name: "Changed" };
  await library.put(changed);
  const migrated = await readFile(file, "utf8");
  expect(Buffer.byteLength(migrated)).toBeLessThanOrEqual(1_048_576);
  expect(migrated).not.toContain('"view":');
  expect(JSON.parse(migrated)).toMatchObject({ schemaVersion: 3 });
  expect(await readFile(`${file}.pre-views-v1`, "utf8")).toBe(source);
  expect((await library.list()).queries[0]).toEqual(changed);
  await library.delete(changed.id);
  expect((await library.list()).queries).toHaveLength(99);
  expect(Buffer.byteLength(await readFile(file, "utf8"))).toBeLessThan(Buffer.byteLength(migrated));
});

const versionTwoBytes = ` { "schemaVersion": 2, "queries": [\n${JSON.stringify({ ...legacyEntry, view: defaults })}\n] }\n`;
const savedLocator = {
  schemaVersion: 1,
  clusterId: "test-cluster",
  topicId: "27c1c482-b9e0-43f2-abd0-ae257fd6a6df",
  topic: "events",
  partition: 1,
  offset: "9007199254740993",
  leaderEpoch: 9,
} as const;

it("leaves real format-2 views unchanged on inspection and no-op mutation, then preserves exact v2 bytes before storing locators", async () => {
  const file = await location();
  await writeFile(file, versionTwoBytes, { mode: 0o600 });
  await writeFile(`${file}.pre-views-v1`, legacyBytes, { mode: 0o600 });
  const before = await stat(file);
  const store = new AtomicKafkaQueryFileStore(file);
  const commit = vi.spyOn(store, "commit");
  const library = new KafkaQueryLibrary(store);
  expect((await library.list()).queries).toEqual([canonical]);
  await library.put(canonical, canonical);
  await library.delete("absent");
  expect(commit).not.toHaveBeenCalled();
  expect(await readFile(file, "utf8")).toBe(versionTwoBytes);
  expect((await stat(file)).ino).toBe(before.ino);
  const saved: KafkaSavedView = {
    ...canonical,
    records: {
      selected: savedLocator,
      comparison: { ...savedLocator, offset: "9007199254740994" },
      bookmarks: [{ id: "bookmark", name: "Investigation position", locator: savedLocator }],
    },
  };
  await library.put(saved, canonical);
  expect(JSON.parse(await readFile(file, "utf8"))).toEqual({
    schemaVersion: 3,
    queries: [{ ...legacyEntry, records: saved.records }],
  });
  expect(await readFile(`${file}.pre-records-v2`, "utf8")).toBe(versionTwoBytes);
  expect(await readFile(`${file}.pre-views-v1`, "utf8")).toBe(legacyBytes);
  expect((await readdir(dirname(file))).sort()).toEqual(
    [basename(file), `${basename(file)}.pre-records-v2`, `${basename(file)}.pre-views-v1`].sort(),
  );
  expect((await new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(file)).list()).queries).toEqual(
    [saved],
  );
  if (process.platform !== "win32")
    expect((await stat(`${file}.pre-records-v2`)).mode & 0o777).toBe(0o600);
});

it("uses independent bounded generations for interrupted v2 migration without overwriting either predecessor family", async () => {
  const file = await location();
  await writeFile(file, versionTwoBytes, { mode: 0o600 });
  await writeFile(`${file}.pre-views-v1`, legacyBytes, { mode: 0o600 });
  const earlier = JSON.stringify({ schemaVersion: 2, queries: [] });
  await writeFile(`${file}.pre-records-v2`, earlier, { mode: 0o600 });
  const temporary = join(dirname(file), `.${basename(file)}.blocked.tmp`);
  await writeFile(temporary, "occupied", { mode: 0o600 });
  const library = new KafkaQueryLibrary(
    new AtomicKafkaQueryFileStore(file, { createTempId: (): string => "blocked" }),
  );
  const updated = {
    ...canonical,
    name: "Recorded position",
    records: { selected: savedLocator, comparison: null, bookmarks: [] },
  };
  await expect(library.put(updated, canonical)).rejects.toThrow("not replaced");
  expect(await readFile(file, "utf8")).toBe(versionTwoBytes);
  expect(await readFile(`${file}.pre-records-v2.1`, "utf8")).toBe(versionTwoBytes);
  await unlink(temporary);
  await library.put(updated, canonical);
  expect(await readFile(`${file}.pre-records-v2`, "utf8")).toBe(earlier);
  expect(await readFile(`${file}.pre-views-v1`, "utf8")).toBe(legacyBytes);
  expect(
    (await readdir(dirname(file))).filter((name) => name.includes("pre-records")),
  ).toHaveLength(2);
});

it.each(["corrupt", "wrong-format", "current-format", "capacity"] as const)(
  "preserves format-2 source when its predecessor family is %s",
  async (kind) => {
    const file = await location();
    await writeFile(file, versionTwoBytes, { mode: 0o600 });
    const backup = `${file}.pre-records-v2`;
    if (kind === "capacity") {
      for (let index = 0; index < 100; index++)
        await writeFile(
          `${backup}${index === 0 ? "" : `.${index}`}`,
          JSON.stringify({ schemaVersion: 2, queries: [] }),
          { mode: 0o600 },
        );
    } else
      await writeFile(
        backup,
        kind === "corrupt"
          ? "{"
          : JSON.stringify({ schemaVersion: kind === "wrong-format" ? 1 : 3, queries: [] }),
        { mode: 0o600 },
      );
    await expect(
      new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(file)).put(
        { ...canonical, name: "Changed" },
        canonical,
      ),
    ).rejects.toThrow("not replaced");
    expect(await readFile(file, "utf8")).toBe(versionTwoBytes);
  },
);

it("revalidates the exact v2 backup at replacement and keeps hidden record content out of storage", async () => {
  const file = await location();
  await writeFile(file, versionTwoBytes, { mode: 0o600 });
  const changedBackup = JSON.stringify({ schemaVersion: 2, queries: [] });
  const store = new AtomicKafkaQueryFileStore(file, {
    createTempId: (): string => {
      writeFileSync(`${file}.pre-records-v2`, changedBackup);
      return "v2-race";
    },
  });
  const library = new KafkaQueryLibrary(store);
  await expect(library.put({ ...canonical, name: "Changed" }, canonical)).rejects.toThrow(
    "not replaced",
  );
  expect(await readFile(file, "utf8")).toBe(versionTwoBytes);
  expect(await readFile(`${file}.pre-records-v2`, "utf8")).toBe(changedBackup);
  const unsafe = {
    ...canonical,
    records: {
      selected: { ...savedLocator, payload: "private-record-sentinel" },
      comparison: null,
      bookmarks: [],
    },
  };
  await expect(library.put(unsafe)).rejects.toThrow("inspect current state");
  expect(await readFile(file, "utf8")).toBe(versionTwoBytes);
  expect(await readFile(file, "utf8")).not.toContain("private-record-sentinel");
});
