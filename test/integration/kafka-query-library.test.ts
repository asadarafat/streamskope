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
