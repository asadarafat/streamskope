import { mkdtemp, readFile, rm, stat, writeFile, symlink, mkdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";

import { afterEach, expect, it } from "vitest";

import type { ProfileProtector } from "../../src/platform/node/profile-protector";
import { AtomicRepairFileStore } from "../../src/platform/node/kafka-repair-file-store";
import { encryptVaultValue, decryptVaultValue } from "../../src/platform/node/vault/vault-crypto";
import { RepairJournal } from "../../src/features/kafka/application/repair-journal";
import {
  replayBatch,
  UNCHANGED_REPLAY_TRANSFORM,
  type RecordReplayReview,
} from "../../src/features/kafka/contracts/record-replay";
import { openPassphraseVault } from "../../src/platform/node/vault/passphrase-vault";
import { inspectBrowserData } from "../../src/platform/node/browser-data-preflight";

const roots: string[] = [];
afterEach(async () => {
  await Promise.all(roots.splice(0).map((r) => rm(r, { recursive: true, force: true })));
});
async function root(): Promise<string> {
  const r = await mkdtemp(join(tmpdir(), "streamskope-repair-"));
  roots.push(r);
  return r;
}
function protection(): ProfileProtector {
  const key = randomBytes(32),
    aad = randomBytes(16);
  return {
    protect: (value: string): Promise<Buffer> =>
      Promise.resolve(encryptVaultValue(key, aad, value)),
    unprotect: (value: Buffer): ReturnType<ProfileProtector["unprotect"]> =>
      Promise.resolve({
        plaintext: decryptVaultValue(key, aad, value),
        shouldReEncrypt: false,
      }),
  };
}
const input = {
  targetProfile: null,
  topic: "destination",
  partition: 0,
  ratePerSecond: 1,
  records: [
    {
      topic: "source",
      partition: 0,
      offset: "1",
      timestampMs: null,
      original: {
        state: "complete" as const,
        encoding: "base64" as const,
        key: null,
        value: Buffer.from("private-repair-payload").toString("base64"),
        headers: [],
      },
    },
  ],
  transform: UNCHANGED_REPLAY_TRANSFORM,
};
const review: RecordReplayReview = {
  planId: "repair",
  sourceName: "Source",
  targetName: "Destination",
  expiresAt: "2026-10-10T12:00:00Z",
  input,
  batch: replayBatch(input),
  destination: { clusterId: "cluster", topicId: "topic", partitions: 1 },
};
it("encrypts exact reviewed bytes and receipts, retains private permissions and opens after restart", async () => {
  const file = join(await root(), "history", "kafka-repair-jobs.json"),
    protector = protection();
  const journal = new RepairJournal(new AtomicRepairFileStore(file, protector));
  await journal.begin(review);
  await journal.intent("repair", 0);
  const disk = await readFile(file, "utf8");
  expect(disk).not.toContain("private-repair-payload");
  expect(disk).not.toContain(input.records[0]!.original.value);
  expect(disk).not.toContain("Destination");
  if (process.platform !== "win32") expect((await stat(file)).mode & 0o777).toBe(0o600);
  const reopened = new AtomicRepairFileStore(file, protector);
  expect((await reopened.load()).jobs[0]?.review).toEqual(review);
  expect(await new RepairJournal(reopened).list()).toMatchObject([
    { uncertainIndex: 0, unsent: 0 },
  ]);
  await expect(new AtomicRepairFileStore(file, protection()).load()).rejects.toThrow("unreadable");
  expect(await readFile(file, "utf8")).toBe(disk);
});
it("reports directory-sync uncertainty after rename and preserves the last readable committed document", async () => {
  const file = join(await root(), "history", "kafka-repair-jobs.json"),
    protector = protection();
  const store = new AtomicRepairFileStore(file, protector, (): Promise<void> => {
    return Promise.reject(new Error("injected directory sync failure"));
  });
  const journal = new RepairJournal(store);
  await expect(journal.begin(review)).rejects.toThrow("uncertain");
  expect((await new AtomicRepairFileStore(file, protector).load()).jobs[0]?.id).toBe("repair");
  await expect(journal.intent("repair", 0)).rejects.toThrow("unavailable");
});
it("refuses corrupt, future and linked files without overwriting them", async () => {
  const r = await root(),
    file = join(r, "repair.json");
  await writeFile(file, '{"schemaVersion":2,"protected":"c2VjcmV0"}', { mode: 0o600 });
  const before = await readFile(file, "utf8");
  await expect(new AtomicRepairFileStore(file, protection()).load()).rejects.toThrow("unreadable");
  expect(await readFile(file, "utf8")).toBe(before);
  await symlink(file, join(r, "linked.json"));
  await expect(
    new AtomicRepairFileStore(join(r, "linked.json"), protection()).load(),
  ).rejects.toThrow("unreadable");
});
it("refuses a stale writer and linked parent without replacing another owner's committed receipts", async () => {
  const r = await root(),
    file = join(r, "repair.json"),
    protector = protection();
  const first = new AtomicRepairFileStore(file, protector),
    stale = new AtomicRepairFileStore(file, protector);
  await stale.load();
  await new RepairJournal(first).begin(review);
  const committed = await readFile(file);
  await expect(stale.commit({ schemaVersion: 1, jobs: [] })).rejects.toThrow(
    "changed after loading",
  );
  expect(await readFile(file)).toEqual(committed);
  const directory = join(r, "linked-parent");
  await symlink(r, directory);
  await expect(
    new AtomicRepairFileStore(join(directory, "repair.json"), protector).load(),
  ).rejects.toThrow("unreadable");
  expect(await readFile(file)).toEqual(committed);
});
it("inspects the protected envelope without decrypting, preserves full backup bytes and refuses unknown formats", async () => {
  const r = await root(),
    vault = await openPassphraseVault({
      dataRoot: r,
      passphrase: "private disposable qualification passphrase",
      mode: "create",
    });
  try {
    await new RepairJournal(
      new AtomicRepairFileStore(join(r, "history", "kafka-repair-jobs.json"), vault.protector),
    ).begin(review);
  } finally {
    await vault.lock();
  }
  const path = join(r, "history", "kafka-repair-jobs.json"),
    original = await readFile(path);
  const report = await inspectBrowserData(r, { hostRelease: "v0.10.3" });
  expect(report.outcome).toBe("eligible");
  expect(report.documents.find((d) => d.kind === "repair-jobs")).toMatchObject({
    state: "verified",
    formats: [1],
    count: 1,
  });
  expect(await readFile(path)).toEqual(original);
  const backup = join(await root(), "history");
  await mkdir(backup);
  await writeFile(join(backup, "kafka-repair-jobs.json"), original, { mode: 0o600 });
  await writeFile(path, JSON.stringify({ schemaVersion: 2, protected: "c2VjcmV0" }));
  const blocked = await inspectBrowserData(r, { hostRelease: "v0.10.3" });
  expect(blocked.outcome).toBe("blocked");
  expect(await readFile(join(backup, "kafka-repair-jobs.json"))).toEqual(original);
});
