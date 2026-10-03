import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { activate } from "../../plugins/nsp/backend";
import manifestJson from "../../plugins/nsp/manifest.json";
import { parseNspConnectInput, parseNspResult, NSP_PLUGIN_ID } from "../../plugins/nsp/contracts";
import { HOST_PROTOCOL_VERSION, type ProfileSummary } from "../../src/features/kafka/contracts";
import {
  createBrowserKafkaProfileStore,
  createKafkaBackend,
} from "../../src/platform/node/kafka-backend";
import { encodePluginPackage, pluginPackageSha256 } from "../../src/platform/node/plugins/package";
import { PluginRuntime } from "../../src/platform/node/plugins/runtime";
import { PluginStore } from "../../src/platform/node/plugins/store";
import { parsePluginJson, parsePluginManifest } from "../../src/plugins/validation";

async function evidence(
  outcome: string,
  checks: readonly string[],
  topicCount?: number,
): Promise<void> {
  await mkdir("dist/ci", { recursive: true });
  await writeFile(
    "dist/ci/nsp-live.json",
    JSON.stringify(
      {
        outcome,
        checkedAt: new Date().toISOString(),
        checks,
        ...(topicCount === undefined ? {} : { topicCount }),
      },
      null,
      2,
    ) + "\n",
  );
}

async function main(): Promise<void> {
  assert(
    !process.env.GITHUB_ACTIONS,
    "Live NSP qualification runs only on the local maintainer host.",
  );
  const path = process.env.STREAMSKOPE_NSP_CONFIG;
  if (!path) {
    await evidence("skipped", []);
    process.stdout.write("Live NSP: skipped (STREAMSKOPE_NSP_CONFIG is not configured).\n");
    return;
  }
  await evidence("running", []);
  const input = parseNspConnectInput(JSON.parse(await readFile(path, "utf8")));
  const root = await mkdtemp(join(tmpdir(), "streamskope-nsp-live-"));
  const store = new PluginStore(root);
  const manifest = parsePluginManifest(manifestJson);
  const resources = await Promise.all(
    (manifest.resources ?? []).map(async ({ path }): Promise<[string, Buffer]> => [
      path,
      await readFile(new URL(`../../plugins/nsp/resources/${path}`, import.meta.url)),
    ]),
  );
  const bytes = encodePluginPackage(
    manifest,
    new Map([
      ["backend.cjs", Buffer.from("exports.activate = () => ({});")],
      ["renderer.js", Buffer.from("export default {};")],
      ...resources,
    ]),
  );
  await store.install(bytes, pluginPackageSha256(bytes));
  const runtime = new PluginRuntime({
    store,
    loadModule: (): Promise<{ activate: typeof activate }> => Promise.resolve({ activate }),
  });
  const profileStore = createBrowserKafkaProfileStore();
  const facade = createKafkaBackend(
    profileStore,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    runtime,
  );
  let profiles: readonly ProfileSummary[] = [];
  let topics: readonly string[] = [];
  const safeEvents: unknown[] = [];
  facade.subscribe((event) => {
    safeEvents.push(event);
    if (event.event === "profiles.changed") profiles = event.payload.profiles;
    if (event.event === "topics.changed") topics = event.payload.topics;
  });
  const checks: string[] = [];
  let topicCount: number | undefined;
  try {
    await runtime.start();
    const activationId = (await runtime.list()).plugins.find(
      (plugin) => plugin.id === NSP_PLUGIN_ID,
    )?.activationId;
    assert(activationId, "NSP plugin did not activate.");
    const connect = async (): Promise<string> => {
      const response = await facade.execute({
        command: "plugin.execute",
        id: randomUUID(),
        version: HOST_PROTOCOL_VERSION,
        payload: {
          pluginId: NSP_PLUGIN_ID,
          activationId,
          method: "nspCapture.connect",
          input: parsePluginJson(input),
        },
      });
      assert(response.ok, response.ok ? "" : response.error.summary);
      const result = parseNspResult(response.result.output);
      assert(result.ok, result.ok ? "" : `${result.error.summary} ${result.error.recovery}`);
      assert(result.profileId);
      return result.profileId;
    };
    const first = await connect();
    checks.push(
      "running-target-version",
      "combined-workflow",
      "core-profile-test",
      "profile-saved",
      "execution-cleanup",
    );
    assert.equal(await store.readRecoveryState(NSP_PLUGIN_ID), null);
    const second = await connect();
    assert.equal(second, first);
    assert.equal(profiles.length, 1);
    checks.push("repeat-reuses-profile");
    const connected = await facade.execute({
      command: "profiles.connect",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: { profileId: first },
    });
    assert(connected.ok, connected.ok ? "" : connected.error.summary);
    const listed = await facade.execute({
      command: "topics.list",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: {},
    });
    assert(listed.ok, listed.ok ? "" : listed.error.summary);
    topicCount = topics.length;
    checks.push("saved-profile-connect", "topic-discovery");
    const topic = topics.find((name) => !name.startsWith("_"));
    assert(topic, "A visible NSP topic is required for read-only observation qualification.");
    const observed = await facade.execute({
      command: "observations.capture",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: {
        topic,
        groupId: null,
        sampleRecords: false,
        thresholds: { lag: null, requestMs: null },
      },
    });
    assert(observed.ok, "NSP Kafka health observation failed.");
    assert.equal(observed.result.capture.series.topic, topic);
    assert(observed.result.capture.series.samples[0]!.partitions.some((p) => p.endOffset !== null));
    checks.push("observed-health");
    const relationships = await facade.execute({
      command: "relationships.capture",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: { topics: [topic], subject: null, version: null, sampleRecords: false },
    });
    assert(relationships.ok, "NSP Kafka relationship discovery failed.");
    assert(relationships.result.graph.nodes.some((n) => n.kind === "topic" && n.label === topic));
    assert.equal(
      relationships.result.graph.coverage.find((c) => c.source === "Kafka metadata")?.state,
      "complete",
    );
    checks.push("relationship-discovery");
    await runtime.remove(NSP_PLUGIN_ID);
    assert.equal(profiles.length, 1);
    assert.equal(await store.readRecoveryState(NSP_PLUGIN_ID), null);
    checks.push("hot-remove-preserves-profile");
    const serializedEvents = JSON.stringify(safeEvents);
    assert(!serializedEvents.includes(input.password));
    for (const record of profileStore.records()) {
      if (record.transport === "plaintext") continue;
      assert(!serializedEvents.includes(record.trust.material));
      if (record.trust.password) assert(!serializedEvents.includes(record.trust.password));
    }
    checks.push("secrets-absent-from-renderer-events");
    await evidence("passed", checks, topicCount);
    process.stdout.write(
      `Live NSP: passed (${checks.join(", ")}; ${topicCount ?? "unknown"} topics).\n`,
    );
  } catch (error) {
    await evidence("failed", checks, topicCount);
    throw error;
  } finally {
    await facade.shutdown();
    // Retain non-secret recovery journal on failure, so an operator can reconcile it.
    if ((await store.readRecoveryState(NSP_PLUGIN_ID)) === null)
      await rm(root, { recursive: true, force: true });
    else process.stderr.write(`NSP cleanup pending; recovery identifiers retained at ${root}.\n`);
  }
}

void main().catch((error: unknown) => {
  process.stderr.write(
    `${error instanceof Error ? error.message : "Live NSP qualification failed."}\n`,
  );
  process.exitCode = 1;
});
