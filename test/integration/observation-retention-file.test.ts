import {
  link,
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
import { dirname, join } from "node:path";

import { afterEach, expect, it, vi } from "vitest";

import { ObservationService } from "../../src/features/kafka/application/observation-service";
import { retainObservations } from "../../src/features/kafka/application/observation-store";
import {
  emptyObservationHistory,
  OBSERVATION_LIMITS as limits,
} from "../../src/features/kafka/contracts/observations";
import { AtomicObservationFileStore } from "../../src/platform/node/kafka-observation-file-store";
import { OBSERVED_AT, observation, observationSeries } from "../support/observation-fixture";

const roots: string[] = [];
afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});
async function fixture(): Promise<{ path: string; bytes: Buffer }> {
  const root = await mkdtemp(join(tmpdir(), "streamskope-retained-observations-"));
  roots.push(root);
  const path = join(root, "history", "kafka-observations.json");
  await mkdir(dirname(path), { mode: 0o700 });
  const bytes = Buffer.from(
    JSON.stringify({ schemaVersion: 1, series: [observationSeries([observation(0)])] }, null, 2) +
      "\n",
  );
  await writeFile(path, bytes, { mode: 0o600 });
  return { path, bytes };
}

it("loads legacy bytes read-only, migrates through host ownership, preserves exact predecessor and physically prunes expired measurements", async () => {
  const { path, bytes } = await fixture(),
    store = new AtomicObservationFileStore(path);
  const before = await stat(path);
  expect((await store.load()).schemaVersion).toBe(1);
  expect(await readFile(path)).toEqual(bytes);
  expect((await stat(path)).mtimeMs).toBe(before.mtimeMs);
  const service = new ObservationService(
    () => null,
    store,
    () => OBSERVED_AT + limits.retentionMs + 21,
  );
  expect(await service.history()).toMatchObject({
    schemaVersion: 2,
    series: [],
    rollups: [],
    durability: "durable",
  });
  expect(await readFile(`${path}.pre-observation-v1`)).toEqual(bytes);
  expect(JSON.parse(await readFile(path, "utf8"))).toEqual(emptyObservationHistory());
  expect((await new AtomicObservationFileStore(path).load()).schemaVersion).toBe(2);
  await expect(store.commit({ schemaVersion: 1, series: [] })).rejects.toThrow(
    "complete predecessor",
  );
  if (process.platform !== "win32")
    expect((await stat(`${path}.pre-observation-v1`)).mode & 0o777).toBe(0o600);
});

it("refuses a changed predecessor and a conflicting migration backup without replacing either", async () => {
  const { path, bytes } = await fixture(),
    store = new AtomicObservationFileStore(path);
  const retained = retainObservations(await store.load(), OBSERVED_AT + 20);
  const changed = Buffer.from(bytes.toString().replace("fixture-topic", "changed-topic"));
  await writeFile(path, changed);
  await expect(store.commit(retained)).rejects.toThrow("changed after loading");
  expect(await readFile(path)).toEqual(changed);
  await store.load();
  await writeFile(`${path}.pre-observation-v1`, bytes, { mode: 0o600 });
  await expect(store.commit(retained)).rejects.toThrow("existing predecessor differs");
  expect(await readFile(path)).toEqual(changed);
  expect(await readFile(`${path}.pre-observation-v1`)).toEqual(bytes);
  expect((await readdir(dirname(path))).filter((name) => name.startsWith("."))).toEqual([]);
});

it("cannot overwrite unreadable data through ordinary commits; explicit Clear retains separate backups", async () => {
  const { path, bytes } = await fixture();
  await writeFile(`${path}.pre-observation-v1`, bytes, { mode: 0o600 });
  const corrupt = '{"schemaVersion":999,"private":"preserve"}';
  await writeFile(path, corrupt);
  const store = new AtomicObservationFileStore(path);
  await expect(store.commit(emptyObservationHistory())).rejects.toThrow("not been replaced");
  expect(await readFile(path, "utf8")).toBe(corrupt);
  await store.clear();
  expect(await store.load()).toEqual(emptyObservationHistory());
  expect(await readFile(`${path}.pre-observation-v1`)).toEqual(bytes);
});

it("reports directory durability failure even after rename and permits explicit reconciliation by reload", async () => {
  const { path } = await fixture();
  const sync = vi
    .fn()
    .mockResolvedValueOnce(undefined)
    .mockRejectedValueOnce(new Error("Directory durability unavailable"));
  const store = new AtomicObservationFileStore(path, sync);
  await expect(
    store.commit(retainObservations(await store.load(), OBSERVED_AT + 20)),
  ).rejects.toThrow("durability unavailable");
  expect((await store.load()).schemaVersion).toBe(2);
  expect(sync).toHaveBeenCalledTimes(2);
});

it("refuses linked active history and redirected directories, including explicit Clear", async () => {
  if (process.platform === "win32") return;
  for (const kind of ["hardlink", "symlink", "directory"] as const) {
    const { path, bytes } = await fixture(),
      store = new AtomicObservationFileStore(path);
    await store.load();
    const other = join(dirname(dirname(path)), "other");
    if (kind === "hardlink") await link(path, other);
    else if (kind === "symlink") {
      await rm(path);
      await writeFile(other, bytes);
      await symlink(other, path);
    } else {
      await mkdir(other);
      await writeFile(join(other, "kafka-observations.json"), bytes);
      await rm(dirname(path), { recursive: true });
      await symlink(other, dirname(path));
    }
    await expect(store.commit(emptyObservationHistory())).rejects.toThrow();
    await expect(store.clear()).rejects.toThrow();
    expect(
      await readFile(kind === "directory" ? join(other, "kafka-observations.json") : other),
    ).toEqual(bytes);
  }
});
