import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostCommand,
  parseHostCommandResponse,
  type KafkaSavedView,
} from "../../src/features/kafka/contracts";
import { KafkaQueryLibrary } from "../../src/features/kafka/application";
import { AtomicKafkaQueryFileStore } from "../../src/platform/node/kafka-query-file-store";
import { createKafkaBackend } from "../../src/platform/node/kafka-backend";

const paths: string[] = [];
async function path(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-queries-"));
  paths.push(directory);
  return join(directory, "queries.json");
}
afterEach(async () => {
  await Promise.all(
    paths.splice(0).map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

const saved: KafkaSavedView = {
  id: "query-1",
  name: "Historical failures",
  profileId: "local-profile",
  records: { selected: null, comparison: null, bookmarks: [] },
  view: {
    schemaVersion: 1,
    destination: { kind: "topic", workspace: "messages" },
    messages: {
      visibleColumns: ["timestamp", "key", "preview", "partition", "offset", "rules"],
      columnWidths: [],
      inspectorWidth: 320,
      filtersOpen: false,
    },
  },
  configuration: {
    schemaVersion: 1,
    request: {
      topic: "orders",
      maxMessages: 100,
      mode: "time-window",
      startTimeMs: 1000,
      endTimeMs: 5000,
    },
    filters: {
      key: "",
      value: "",
      offset: "",
      timestamp: "",
      partition: null,
      expression: '$.status == "failed"',
    },
  },
};

describe("saved investigation persistence", () => {
  it("restores exact settings after host reconstruction, serializes writes and makes retries idempotent", async () => {
    const file = await path();
    const store = new AtomicKafkaQueryFileStore(file);
    const library = new KafkaQueryLibrary(store);
    const commit = vi.spyOn(store, "commit");
    await Promise.all([
      library.put(saved),
      library.put({ ...saved, id: "query-2", name: "Other" }),
    ]);
    await library.put(saved);
    expect(commit).toHaveBeenCalledTimes(2);
    const restored = new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(file));
    expect(await restored.list()).toEqual({
      durability: "durable",
      queries: [saved, { ...saved, id: "query-2", name: "Other" }],
    });
    await library.delete("query-1");
    await library.delete("query-1");
    expect(commit).toHaveBeenCalledTimes(3);
    expect((await restored.list()).queries.map((query) => query.id)).toEqual(["query-2"]);
    if (process.platform !== "win32") expect((await stat(file)).mode & 0o777).toBe(0o600);
  });

  it("preserves the file after duplicate names and failed writes, and permits a later retry", async () => {
    const file = await path();
    const store = new AtomicKafkaQueryFileStore(file);
    const library = new KafkaQueryLibrary(store);
    await library.put(saved);
    const before = await readFile(file, "utf8");
    await expect(library.put({ ...saved, id: "duplicate" })).rejects.toThrow(
      "inspect current state",
    );
    expect(await readFile(file, "utf8")).toBe(before);
    vi.spyOn(store, "commit").mockRejectedValueOnce(new Error("failed write"));
    await expect(library.put({ ...saved, name: "Changed" })).rejects.toThrow(
      "inspect current state",
    );
    expect((await library.list()).queries).toEqual([saved]);
    await library.put({ ...saved, name: "Changed" });
    expect((await library.list()).queries[0]?.name).toBe("Changed");
  });

  it.each([
    JSON.stringify({ schemaVersion: 999, queries: [] }),
    JSON.stringify({ schemaVersion: 1, queries: [{ ...saved, password: "sentinel" }] }),
    "x".repeat(1_048_577),
  ])("never replaces unsupported, unknown-field or oversized storage", async (contents) => {
    const file = await path();
    await writeFile(file, contents);
    const library = new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(file));
    await expect(library.put(saved)).rejects.toThrow("unreadable");
    expect(await readFile(file, "utf8")).toBe(contents);
  });

  it("checks concurrent expectations inside the serialized mutation and never loses a newer view", async () => {
    const file = await path();
    const store = new AtomicKafkaQueryFileStore(file);
    const library = new KafkaQueryLibrary(store);
    await library.put(saved, null);
    const commit = vi.spyOn(store, "commit");
    const first = { ...saved, name: "First reviewed update" };
    const outcomes = await Promise.allSettled([
      library.put(first, saved),
      library.put({ ...saved, name: "Stale second update" }, saved),
    ]);
    expect(outcomes.map((outcome) => outcome.status)).toEqual(["fulfilled", "rejected"]);
    const rejected = outcomes[1];
    if (rejected.status !== "rejected") throw new Error("A stale update unexpectedly succeeded.");
    expect(String(rejected.reason)).toContain("Refresh Saved views");
    expect(commit).toHaveBeenCalledTimes(1);
    expect((await library.list()).queries).toEqual([first]);
    const bytes = await readFile(file);
    await expect(library.put(first, saved)).rejects.toThrow("Nothing was overwritten");
    expect(await readFile(file)).toEqual(bytes);
    expect(commit).toHaveBeenCalledTimes(1);
  });

  it("preserves a newly added bookmark when a concurrent presentation save used an older snapshot", async () => {
    const file = await path();
    const store = new AtomicKafkaQueryFileStore(file);
    const library = new KafkaQueryLibrary(store);
    await library.put(saved, null);
    const bookmarked: KafkaSavedView = {
      ...saved,
      records: {
        selected: null,
        comparison: null,
        bookmarks: [
          {
            id: "evidence",
            name: "Evidence",
            locator: {
              schemaVersion: 1,
              clusterId: "test-cluster",
              topicId: "27c1c482-b9e0-43f2-abd0-ae257fd6a6df",
              topic: "orders",
              partition: 0,
              offset: "9007199254740993",
              leaderEpoch: 7,
            },
          },
        ],
      },
    };
    const layout = {
      ...saved,
      view: { ...saved.view, messages: { ...saved.view.messages, filtersOpen: true } },
    };
    const commit = vi.spyOn(store, "commit");
    const outcomes = await Promise.allSettled([
      library.put(bookmarked, saved),
      library.put(layout, saved),
    ]);
    expect(outcomes.map((outcome) => outcome.status)).toEqual(["fulfilled", "rejected"]);
    expect(commit).toHaveBeenCalledTimes(1);
    expect((await library.list()).queries).toEqual([bookmarked]);
    const refreshed = (await library.list()).queries[0]!;
    await library.put({ ...refreshed, view: layout.view }, refreshed);
    const reloaded = new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(file));
    expect((await reloaded.list()).queries).toEqual([{ ...bookmarked, view: layout.view }]);
  });

  it("rejects stale resurrection after delete and permits an explicit newly reviewed create", async () => {
    const file = await path();
    const store = new AtomicKafkaQueryFileStore(file);
    const library = new KafkaQueryLibrary(store);
    await library.put(saved, null);
    await library.delete(saved.id);
    const commit = vi.spyOn(store, "commit");
    await expect(library.put({ ...saved, name: "Resurrected" }, saved)).rejects.toThrow("deleted");
    expect(commit).not.toHaveBeenCalled();
    expect((await library.list()).queries).toEqual([]);
    await library.put(saved, null);
    expect(commit).toHaveBeenCalledTimes(1);
  });

  it("admits only one concurrent expected-absent create, and omission retains explicit replacement", async () => {
    const library = new KafkaQueryLibrary(new AtomicKafkaQueryFileStore(await path()));
    const outcomes = await Promise.allSettled([
      library.put(saved, null),
      library.put({ ...saved, name: "Duplicate create" }, null),
    ]);
    expect(outcomes.map((outcome) => outcome.status)).toEqual(["fulfilled", "rejected"]);
    await expect(library.put({ ...saved, id: "different" }, saved)).rejects.toThrow(
      "Nothing was overwritten",
    );
    await library.put({ ...saved, name: "Explicit replacement" });
    expect((await library.list()).queries[0]?.name).toBe("Explicit replacement");
  });

  it("keeps the original expectation valid after a failed write and canonicalizes compact stored defaults", async () => {
    const file = await path();
    const store = new AtomicKafkaQueryFileStore(file);
    const library = new KafkaQueryLibrary(store);
    await library.put(saved, null);
    expect(await readFile(file, "utf8")).not.toContain('"view":');
    const changed = { ...saved, name: "Retry after failed write" };
    vi.spyOn(store, "commit").mockRejectedValueOnce(new Error("private write failure"));
    await expect(library.put(changed, saved)).rejects.toThrow("inspect current state");
    expect((await library.list()).queries).toEqual([saved]);
    await library.put(changed, saved);
    expect((await library.list()).queries).toEqual([changed]);
  });

  it("qualifies typed host commands without opening a Kafka connection or embedding credentials", async () => {
    const backend = createKafkaBackend();
    const command = parseHostCommand({
      command: "queries.put",
      id: "save",
      version: HOST_PROTOCOL_VERSION,
      payload: { query: saved },
    });
    const response = parseHostCommandResponse(await backend.execute(command));
    expect(response).toMatchObject({
      command: "queries.put",
      ok: true,
      result: { snapshot: { durability: "session", queries: [saved] } },
    });
    expect(backend.connectionSnapshot().state).toBe("disconnected");
    expect(() =>
      parseHostCommand({
        ...command,
        payload: {
          query: {
            ...saved,
            configuration: { ...saved.configuration, credentials: { password: "sentinel" } },
          },
        },
      }),
    ).toThrow("not declared");
    await backend.shutdown();
  });
});
