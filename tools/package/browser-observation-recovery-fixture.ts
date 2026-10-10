import assert from "node:assert/strict";
import { chown, chmod, lstat, mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

import type { BrowserInstallerTarget } from "./browser-installer-evidence";
import { run } from "./browser-qualification-process";
import type { BrowserVaultFixture } from "./browser-vault-fixture";

/** Independent storage evidence only; real Kafka tests establish measurement provenance. */
export async function seedNativeObservationRecovery(options: {
  readonly fixture: BrowserVaultFixture;
  readonly data: string;
  readonly uid: number;
  readonly gid: number;
}): Promise<{
  migrate(
    container: string,
    predecessor: BrowserInstallerTarget,
    environment: NodeJS.ProcessEnv,
  ): Promise<void>;
  verifyRestore(recovery: string): Promise<void>;
  verifyRollback(): Promise<void>;
}> {
  const { fixture, data, uid, gid } = options;
  const path = join(data, "history", "kafka-observations.json");
  const sample = (id: string, at: number): Record<string, unknown> => ({
    id,
    segmentId: "native-storage-fixture",
    startedAt: at - 20,
    observedAt: at,
    source: "kafka-api",
    requestMs: 20,
    providerCalls: 1,
    state: "ready",
    groupState: null,
    members: null,
    brokerCount: 1,
    controllerKnown: true,
    groupCoverage: "not-selected",
    records: null,
    alerts: [],
    issues: [],
    partitions: [
      {
        partition: 0,
        leader: 1,
        replicas: 1,
        inSyncReplicas: 1,
        endOffset: "10",
        committedOffset: null,
        lag: null,
      },
    ],
  });
  const now = Date.now();
  const resource = {
    clusterId: "native-storage-cluster",
    topicId: "native-storage-topic",
    topic: "native-events",
    groupId: null,
  };
  const legacy = Buffer.from(
    JSON.stringify(
      {
        schemaVersion: 1,
        series: [
          { ...resource, samples: [sample("expired", now - 86_500_000), sample("retained", now)] },
        ],
      },
      null,
      2,
    ) + "\n",
  );
  const settings = {
    input: {
      topic: resource.topic,
      groupId: null,
      thresholds: { lag: 12, requestMs: 250 },
      sampleRecords: true,
    },
    connectionName: "Native storage fixture",
    clusterId: resource.clusterId,
    topicId: resource.topicId,
    savedAt: now,
  };
  const publish = async (contents: Buffer): Promise<void> => {
    await mkdir(join(data, "history"), { recursive: true, mode: 0o700 });
    await chown(join(data, "history"), uid, gid);
    await writeFile(path, contents, { mode: 0o600 });
    await chmod(path, 0o600);
    await chown(path, uid, gid);
  };
  await fixture.lock();
  await publish(legacy);
  const originalMetadata = await lstat(path);
  await fixture.unlockAfterReplacement();
  const old = await fixture.kafkaCommand("observations.history", {});
  assert.equal((old.snapshot as { schemaVersion: number }).schemaVersion, 1);
  assert.deepEqual(await readFile(path), legacy);
  assert.equal((await lstat(path)).ino, originalMetadata.ino);
  assert.equal((await lstat(path)).mtimeMs, originalMetadata.mtimeMs);
  let migrated: Buffer | undefined;
  return {
    migrate: async (container, predecessor, environment): Promise<void> => {
      const result = await fixture.kafkaCommand("observations.history", {});
      const history = result.snapshot as {
        schemaVersion: number;
        series: Array<{ samples: Array<{ id: string }> }>;
        rollups: Array<{ samples: number; lagKnown: number; lastLag: number | null }>;
      };
      assert.equal(history.schemaVersion, 2);
      assert.deepEqual(
        history.series.map((s) => s.samples.map((v) => v.id)),
        [["retained"]],
      );
      assert.equal(history.rollups.length, 1);
      assert.deepEqual(
        [history.rollups[0]!.samples, history.rollups[0]!.lagKnown, history.rollups[0]!.lastLag],
        [1, 0, null],
      );
      assert.deepEqual(await readFile(`${path}.pre-observation-v1`), legacy);
      await fixture.lock();
      await publish(
        Buffer.from(JSON.stringify({ ...history, settings, durability: undefined }) + "\n"),
      );
      await run("docker", ["restart", "--time", "120", container], environment);
      await fixture.unlockAfterReplacement();
      const restarted = await fixture.kafkaCommand("observations.history", {});
      assert.deepEqual((restarted.snapshot as { settings: unknown }).settings, settings);
      const watch = (await fixture.kafkaCommand("observations.watch.status", {})).watch as {
        phase: string;
        input: unknown;
        nextCaptureAt: unknown;
        current: boolean;
      };
      assert.deepEqual(
        [watch.phase, watch.input, watch.nextCaptureAt, watch.current],
        ["stopped", null, null, false],
      );
      migrated = await readFile(path);
      // Isolate the format from the new predecessor path: the exact old inspector
      // must also refuse format2 when no unrecognized backup path exists.
      const isolated = join(data, "..", "observation-inspection-fixture");
      await mkdir(join(isolated, "history"), { recursive: true, mode: 0o700 });
      for (const dir of [isolated, join(isolated, "history")]) await chown(dir, uid, gid);
      const inspectionPath = join(isolated, "history", "kafka-observations.json");
      await writeFile(inspectionPath, migrated, { mode: 0o600 });
      await chown(inspectionPath, uid, gid);
      const before = await lstat(inspectionPath);
      const refused = await run(
        "docker",
        [
          "run",
          "--rm",
          "--network",
          "none",
          "--read-only",
          "--cap-drop",
          "ALL",
          "--security-opt",
          "no-new-privileges",
          "--user",
          `${uid}:${gid}`,
          "--mount",
          `type=bind,source=${isolated},target=/data,readonly`,
          "--entrypoint",
          "node",
          predecessor.imageId,
          "dist/web/data-preflight.cjs",
          "/data",
        ],
        environment,
        true,
      );
      fixture.assertNoSecrets(refused.stdout + refused.stderr);
      assert.equal(refused.code, 2);
      const report = JSON.parse(refused.stdout) as {
        documents: Array<{ kind: string; state: string; reason: string }>;
      };
      assert.deepEqual(
        report.documents.find((r) => r.kind === "observations"),
        {
          kind: "observations",
          state: "blocked",
          count: 0,
          formats: [],
          reason: "unsupported-format",
        },
      );
      assert.deepEqual(await readFile(inspectionPath), migrated);
      assert.equal((await lstat(inspectionPath)).ino, before.ino);
      assert.equal((await lstat(inspectionPath)).mtimeMs, before.mtimeMs);
    },
    verifyRestore: async (recovery): Promise<void> => {
      assert.ok(migrated);
      assert.deepEqual(
        await readFile(join(recovery, "history", "kafka-observations.json")),
        migrated,
      );
      assert.deepEqual(
        await readFile(join(recovery, "history", "kafka-observations.json.pre-observation-v1")),
        legacy,
      );
      assert.deepEqual(await readFile(path), legacy);
    },
    verifyRollback: async (): Promise<void> => {
      const restored = await fixture.kafkaCommand("observations.history", {});
      assert.equal((restored.snapshot as { schemaVersion: number }).schemaVersion, 1);
      assert.deepEqual(await readFile(path), legacy);
    },
  };
}
