import { mkdtemp, readFile, readdir, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  InMemoryKafkaQueryStore,
  KafkaQueryLibrary,
  type KafkaQueryStore,
} from "../../src/features/kafka/application/query-library";
import {
  parseKafkaSavedView,
  type KafkaInvestigationLibraryState,
} from "../../src/features/kafka/contracts/query-library";
import type { KafkaTopicAnnotation } from "../../src/features/kafka/contracts/topic-catalog";
import { AtomicKafkaQueryFileStore } from "../../src/platform/node/kafka-query-file-store";

const identity = {
  clusterId: "production-eu",
  topicId: "27c1c482-b9e0-43f2-abd0-ae257fd6a6df",
  topic: "orders",
};
const annotation: KafkaTopicAnnotation = {
  identity,
  description: "Check delivery.",
  owner: "Operations",
  labels: ["critical"],
  links: [{ title: "Runbook", url: "https://example.com/orders" }],
};
const view = parseKafkaSavedView({
  id: "incident",
  name: "Incident",
  configuration: {
    schemaVersion: 1,
    request: { topic: "orders", mode: "earliest", maxMessages: 100 },
  },
  view: {
    schemaVersion: 1,
    destination: { kind: "topic", workspace: "monitor" },
    messages: {
      visibleColumns: ["key", "preview"],
      columnWidths: [],
      inspectorWidth: 360,
      filtersOpen: true,
    },
  },
  records: { selected: null, comparison: null, bookmarks: [] },
});
const directories: string[] = [];
async function file(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-catalog-store-"));
  directories.push(root);
  return join(root, "kafka-queries.json");
}
afterEach(async () => {
  await Promise.all(
    directories.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});
function deferred(): { promise: Promise<void>; resolve(): void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function other(index: number): KafkaTopicAnnotation {
  return { ...annotation, identity: { ...identity, clusterId: `cluster-${String(index)}` } };
}

describe("whole investigation library owner", () => {
  it("serializes concurrent view and note changes and retains both through reconstruction", async () => {
    const path = await file();
    const library = new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(path));
    await Promise.all([library.put(view, null), library.putTopic(annotation, null)]);
    const reconstructed = new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(path));
    expect((await reconstructed.list()).queries).toEqual([view]);
    expect((await reconstructed.listTopics()).topics).toEqual([annotation]);
    await reconstructed.put({ ...view, name: "Updated view" }, view);
    expect((await reconstructed.getTopic(identity)).annotation).toEqual(annotation);
    await reconstructed.delete(view.id);
    expect((await reconstructed.listTopics()).topics).toEqual([annotation]);
    expect(JSON.parse(await readFile(path, "utf8"))).toEqual({
      schemaVersion: 4,
      queries: [],
      topics: [annotation],
    });
  });
  it("rejects competing edits and stale deletes without losing admitted changes or poisoning the queue", async () => {
    const library = new KafkaQueryLibrary();
    await library.putTopic(annotation, null);
    const changed = { ...annotation, owner: "Incident response" };
    const results = await Promise.allSettled([
      library.putTopic(changed, annotation),
      library.putTopic({ ...annotation, description: "Overwritten?" }, annotation),
    ]);
    expect(results.map((result) => result.status)).toEqual(["fulfilled", "rejected"]);
    await expect(library.deleteTopic(identity, annotation)).rejects.toThrow(/changed/u);
    expect((await library.getTopic(identity)).annotation).toEqual(changed);
    await library.deleteTopic(identity, changed);
    expect((await library.getTopic(identity)).annotation).toBeNull();
  });
  it("never inherits metadata for a recreated topic and permits exact orphan cleanup", async () => {
    const replacement = {
      ...annotation,
      identity: { ...identity, topicId: "11111111-1111-1111-1111-111111111111" },
      owner: "New owner",
    };
    const library = new KafkaQueryLibrary();
    await library.putTopic(annotation, null);
    expect((await library.getTopic(replacement.identity)).annotation).toBeNull();
    await library.putTopic(replacement, null);
    await expect(library.deleteTopic(replacement.identity, annotation)).rejects.toThrow();
    await library.deleteTopic(identity, annotation);
    expect((await library.listTopics()).topics).toEqual([replacement]);
  });
  it("checks connection authority after a queued load and refuses before write admission", async () => {
    const inner = new InMemoryKafkaQueryStore();
    const started = deferred();
    const resume = deferred();
    const commit = vi.fn((state: KafkaInvestigationLibraryState): Promise<void> =>
      inner.commit(state),
    );
    const store: KafkaQueryStore = {
      durability: "session",
      load: async () => {
        started.resolve();
        await resume.promise;
        return inner.load();
      },
      commit,
    };
    const library = new KafkaQueryLibrary(store);
    let current = true;
    const saving = library.putTopic(annotation, null, () => {
      if (!current) throw new Error("Connection replaced");
    });
    const refusal = expect(saving).rejects.toThrow();
    await started.promise;
    current = false;
    resume.resolve();
    await refusal;
    expect(commit).not.toHaveBeenCalled();
    expect((await library.listTopics()).topics).toEqual([]);
  });
  it("retains the actual outcome after a local write was already admitted", async () => {
    const inner = new InMemoryKafkaQueryStore();
    const admitted = deferred();
    const complete = deferred();
    const store: KafkaQueryStore = {
      durability: "session",
      load: () => inner.load(),
      commit: async (state) => {
        admitted.resolve();
        await complete.promise;
        await inner.commit(state);
      },
    };
    const library = new KafkaQueryLibrary(store);
    let current = true;
    const saving = library.putTopic(annotation, null, () => {
      if (!current) throw new Error("Connection replaced");
    });
    await admitted.promise;
    current = false;
    complete.resolve();
    expect((await saving).annotation).toEqual(annotation);
    expect((await library.getTopic(identity)).annotation).toEqual(annotation);
  });
  it("enforces the shared count and UTF-8 byte budget in memory and durable storage without pruning", async () => {
    for (const store of [
      new InMemoryKafkaQueryStore(),
      new AtomicKafkaQueryFileStore(await file()),
    ]) {
      const full = {
        queries: [view],
        topics: Array.from({ length: 256 }, (_, index) => other(index)),
      };
      await store.commit(full);
      const library = new KafkaQueryLibrary(store);
      await expect(library.putTopic(other(256), null)).rejects.toThrow();
      const oversized: KafkaInvestigationLibraryState = {
        ...full,
        topics: full.topics.map((entry) => ({ ...entry, description: "界".repeat(1365) + "a" })),
      };
      await expect(Promise.resolve().then(() => store.commit(oversized))).rejects.toThrow(/1 MiB/u);
      expect(await store.load()).toEqual(full);
      await library.deleteTopic(full.topics[0]!.identity, full.topics[0]!);
      expect((await library.listTopics()).topics).toHaveLength(255);
      expect((await library.list()).queries).toEqual([view]);
    }
  });
});

describe("actual version3 to catalog storage migration", () => {
  const predecessor = ` { "schemaVersion": 3, "queries": [\n${JSON.stringify(view)}\n] }\n`;
  async function seed(): Promise<string> {
    const path = await file();
    await writeFile(path, predecessor, { mode: 0o600 });
    return path;
  }
  it("keeps exact predecessor bytes and inode on reads and a no-op before the first note mutation", async () => {
    const path = await seed();
    const before = await stat(path);
    const library = new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(path));
    expect((await library.list()).queries).toEqual([view]);
    expect((await library.listTopics()).topics).toEqual([]);
    expect((await library.getTopic(identity)).annotation).toBeNull();
    await library.put(view, view);
    await library.delete("absent");
    expect(await readFile(path, "utf8")).toBe(predecessor);
    expect((await stat(path)).ino).toBe(before.ino);
    expect((await readdir(dirname(path))).length).toBe(1);
    await library.putTopic(annotation, null);
    expect(await readFile(`${path}.pre-catalog-v3`, "utf8")).toBe(predecessor);
    expect(JSON.parse(await readFile(path, "utf8"))).toMatchObject({
      schemaVersion: 4,
      topics: [annotation],
    });
    if (process.platform !== "win32")
      expect((await stat(`${path}.pre-catalog-v3`)).mode & 0o777).toBe(0o600);
    expect((await library.list()).queries).toEqual([view]);
  });
  it("migrates on an actual view mutation and preserves all existing backup families", async () => {
    const path = await seed();
    const old = JSON.stringify({ schemaVersion: 1, queries: [] });
    const previous = JSON.stringify({ schemaVersion: 2, queries: [] });
    await writeFile(`${path}.pre-views-v1`, old, { mode: 0o600 });
    await writeFile(`${path}.pre-records-v2`, previous, { mode: 0o600 });
    await new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(path)).put(
      { ...view, name: "New name" },
      view,
    );
    expect(JSON.parse(await readFile(path, "utf8"))).toMatchObject({ schemaVersion: 4 });
    expect(await readFile(`${path}.pre-catalog-v3`, "utf8")).toBe(predecessor);
    expect(await readFile(`${path}.pre-views-v1`, "utf8")).toBe(old);
    expect(await readFile(`${path}.pre-records-v2`, "utf8")).toBe(previous);
  });
  it("refuses an unsafe predecessor backup without replacing live data", async () => {
    const path = await seed();
    const other = join(dirname(path), "other.json");
    await writeFile(other, predecessor, { mode: 0o600 });
    await symlink(other, `${path}.pre-catalog-v3`);
    await expect(
      new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(path)).putTopic(annotation, null),
    ).rejects.toThrow(/not replaced/u);
    expect(await readFile(path, "utf8")).toBe(predecessor);
    expect(await readFile(other, "utf8")).toBe(predecessor);
  });
  it("preserves differing valid generations and fails closed when the backup bound is exhausted", async () => {
    const path = await seed();
    const old = JSON.stringify({ schemaVersion: 3, queries: [] });
    for (let index = 0; index < 100; index++)
      await writeFile(`${path}.pre-catalog-v3${index === 0 ? "" : `.${String(index)}`}`, old, {
        mode: 0o600,
      });
    await expect(
      new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(path)).putTopic(annotation, null),
    ).rejects.toThrow(/not replaced/u);
    expect(await readFile(path, "utf8")).toBe(predecessor);
    expect(await readFile(`${path}.pre-catalog-v3.99`, "utf8")).toBe(old);
    expect((await readdir(dirname(path))).length).toBe(101);
  });
  it("reports uncertainty honestly after replacement while retaining exact v3 recovery bytes", async () => {
    const path = await seed();
    let calls = 0;
    const store = new AtomicKafkaQueryFileStore(path, {
      syncDirectory: (): Promise<void> =>
        ++calls === 2 ? Promise.reject(new Error("Directory sync failed")) : Promise.resolve(),
    });
    const library = new KafkaQueryLibrary(store);
    await expect(library.putTopic(annotation, null)).rejects.toThrow(/replacement occurred/u);
    expect(await readFile(`${path}.pre-catalog-v3`, "utf8")).toBe(predecessor);
    expect((await library.getTopic(identity)).annotation).toEqual(annotation);
  });
});
