import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

import { NspApiClient } from "../../plugins/nsp/backend/api-client";
import { parseRecovery } from "../../plugins/nsp/backend/recovery";
import { KafkaProfileService } from "../../src/features/kafka/application";
import { parseNspConnectInput, parseNspResult, NSP_PLUGIN_ID } from "../../plugins/nsp/contracts";
import { HOST_PROTOCOL_VERSION, type ProfileSummary } from "../../src/features/kafka/contracts";
import {
  createBrowserKafkaProfileStore,
  createKafkaBackend,
} from "../../src/platform/node/kafka-backend";
import { createHostTrustMaterialDecoder } from "../../src/platform/node/trust-material-decoder";
import { pluginPackageFixtures } from "../../test/support/plugin-package-fixture";
import {
  nspFixtureAdmin,
  ownedNspTopic,
  removeOwnedNspTopic,
} from "../../test/support/nsp-live-broker";
import { packagePlugins } from "../package/plugin";
import { PluginRuntime } from "../../src/platform/node/plugins/runtime";
import { PluginStore } from "../../src/platform/node/plugins/store";
import { parsePluginJson } from "../../src/plugins/validation";

import { interruptNspRetrieval } from "./nsp-live-recovery";

async function evidence(
  outcome: string,
  checks: readonly string[],
  details: Record<string, unknown> = {},
): Promise<void> {
  await mkdir("dist/ci", { recursive: true });
  await writeFile(
    "dist/ci/nsp-live.json",
    JSON.stringify(
      {
        schemaVersion: 1,
        outcome,
        checkedAt: new Date().toISOString(),
        checks,
        ...details,
      },
      null,
      2,
    ) + "\n",
  );
}

async function main(): Promise<void> {
  await evidence("running", []);
  assert(
    !process.env.GITHUB_ACTIONS,
    "Live NSP qualification runs only on the local maintainer host.",
  );
  const path = process.env.STREAMSKOPE_NSP_CONFIG;
  if (!path) {
    await evidence("skipped", [], {
      reasonCode: "not-configured",
      reason: "Local NSP connection is not configured.",
    });
    process.stdout.write("Live NSP: skipped (STREAMSKOPE_NSP_CONFIG is not configured).\n");
    return;
  }
  const input = parseNspConnectInput(JSON.parse(await readFile(path, "utf8")));
  const credentials = {
    apiUrl: input.apiUrl,
    username: input.username,
    password: input.password,
    verifyCertificate: input.verifyCertificate,
  };
  if (process.env.STREAMSKOPE_PLUGIN_PACKAGE_READY !== "1") await packagePlugins("nsp");
  const packages = await pluginPackageFixtures("nsp");
  const root = await mkdtemp(join(tmpdir(), "streamskope-nsp-live-"));
  const store = new PluginStore(root);
  let available = packages.current;
  const profileStore = createBrowserKafkaProfileStore();
  const host = (): { runtime: PluginRuntime; facade: ReturnType<typeof createKafkaBackend> } => {
    const runtime = new PluginRuntime({
      store,
      // Only the catalog is local. Package verification, installation and module loading are real.
      catalog: {
        list: (): Promise<never[]> => Promise.resolve([]),
        download: (): Promise<typeof available> => Promise.resolve(available),
      },
    });
    return {
      runtime,
      facade: createKafkaBackend({ profileStore, plugins: runtime }),
    };
  };
  let { runtime, facade } = host();
  let profiles: readonly ProfileSummary[] = [];
  let topics: readonly string[] = [];
  const safeEvents: unknown[] = [];
  const received: (string | null)[] = [];
  const subscribe = (): void => {
    facade.subscribe((event) => {
      safeEvents.push(event);
      if (event.event === "profiles.changed") profiles = event.payload.profiles;
      if (event.event === "topics.changed") topics = event.payload.topics;
      if (event.event === "messages.batch")
        received.push(...event.payload.messages.map((message) => message.payload));
    });
  };
  subscribe();
  const checks: string[] = [];
  const details: Record<string, unknown> = {
    hostProtocol: HOST_PROTOCOL_VERSION,
    apiCertificateVerification: input.verifyCertificate,
    packages: [packages.current, packages.update].map(({ manifest, sha256 }) => ({
      version: manifest.version,
      apiVersion: manifest.apiVersion,
      sha256,
    })),
    scope: {
      loader: "production package loader; isolated catalog",
      profileStore: "isolated host memory; native protected storage tested separately",
      update: "same built code with a newer qualification-only manifest",
      recovery: "SIGKILL immediately after the real execution identifier is durably journaled",
      writes: "one generated record in one uniquely owned temporary topic",
      businessTopics: "metadata only; no business payload reads or writes",
    },
  };
  const client = new NspApiClient(input);
  const workflow = (): ReturnType<NspApiClient["ensureWorkflow"]> =>
    client.ensureWorkflow().finally(() => client.close());
  let admin: ReturnType<typeof nspFixtureAdmin> | undefined;
  const topic = ownedNspTopic();
  details.ownedTopic = topic;
  let fixtureAttempted = false;
  let failure: Error | undefined;
  const invoke = async (
    method: string,
    value: unknown,
  ): Promise<ReturnType<typeof parseNspResult>> => {
    const active = (await runtime.list()).plugins.find((plugin) => plugin.id === NSP_PLUGIN_ID);
    assert(active?.activationId, "NSP plugin did not activate.");
    const response = await facade.execute({
      command: "plugin.execute",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: {
        pluginId: NSP_PLUGIN_ID,
        activationId: active.activationId,
        method,
        input: parsePluginJson(value),
      },
    });
    assert(response.ok, "NSP plugin host command failed.");
    return parseNspResult(response.result.output);
  };
  try {
    details.target = await client.readVersion().finally(() => client.close());
    assert.equal((await runtime.list()).plugins.length, 0);
    await runtime.install(NSP_PLUGIN_ID);
    checks.push("clean-package-install");
    await runtime.install(NSP_PLUGIN_ID);
    assert.equal((await runtime.list()).plugins.length, 1);
    assert.equal(
      (await runtime.list()).plugins[0]!.active?.version,
      packages.current.manifest.version,
    );
    checks.push("repeat-install-retains-one-plugin");
    const firstActivation = (await runtime.list()).plugins[0]!.activationId;
    const connect = async (): Promise<string> => {
      const result = await invoke("nspCapture.connect", input);
      assert(result.ok && result.profileId, "NSP profile qualification failed.");
      return result.profileId;
    };
    const first = await connect();
    const helper = await workflow();
    checks.push(
      "running-target-version",
      "combined-workflow",
      "core-profile-test",
      "profile-saved",
    );
    assert.equal(await store.readRecoveryState(NSP_PLUGIN_ID), null);
    checks.push("execution-cleanup");
    const second = await connect();
    assert.equal(second, first);
    assert.equal(profiles.length, 1);
    assert.deepEqual(await workflow(), helper);
    checks.push("repeat-reuses-profile-and-workflow");
    const connection = await new KafkaProfileService(
      profileStore,
      createHostTrustMaterialDecoder(),
    ).resolveConnection(first);
    details.kafkaAuthentication = connection.oauth === undefined ? "tls" : "oauth";
    admin = nspFixtureAdmin(connection);
    assert(!(await admin.listTopics()).includes(topic), "Fixture topic already exists.");
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
    details.topicCount = topics.length;
    checks.push("saved-profile-connect", "topic-discovery");
    fixtureAttempted = true;
    await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
    const marker = `streamskope-owned-qualification:${randomUUID()}`;
    const review = await facade.execute({
      command: "writes.review",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: {
        kind: "record",
        topic,
        partition: 0,
        record: {
          state: "complete",
          encoding: "base64",
          key: null,
          value: Buffer.from(marker).toString("base64"),
          headers: [],
        },
      },
    });
    assert(review.ok, "Owned NSP record review failed.");
    const sent = await facade.execute({
      command: "writes.apply",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: { planId: review.result.review.planId },
    });
    assert(
      sent.ok && sent.result.outcome.state === "acknowledged",
      "NSP fixture record was not acknowledged.",
    );
    const reading = await facade.execute({
      command: "messages.start",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: { topic, mode: "earliest", maxMessages: 1 },
    });
    assert(reading.ok, "NSP known-record reading failed.");
    for (let attempt = 0; attempt < 100 && !received.includes(marker); attempt += 1)
      await delay(200);
    assert.deepEqual(received, [marker], "NSP fixture record was not received through the host.");
    details.knownRecord = { count: 1, sha256: createHash("sha256").update(marker).digest("hex") };
    checks.push("known-record-produce-and-receipt");
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
    available = packages.update;
    await runtime.install(NSP_PLUGIN_ID);
    assert.notEqual((await runtime.list()).plugins[0]!.activationId, firstActivation);
    assert.equal(
      (await runtime.list()).plugins[0]!.active?.version,
      packages.update.manifest.version,
    );
    assert.equal(profiles.length, 1);
    assert.equal(await connect(), first);
    checks.push("hot-update-preserves-and-refreshes-profile");
    await runtime.remove(NSP_PLUGIN_ID);
    await runtime.remove(NSP_PLUGIN_ID);
    assert.equal((await runtime.list()).plugins.length, 0);
    assert.equal(profiles.length, 1);
    assert.equal(await store.readRecoveryState(NSP_PLUGIN_ID), null);
    checks.push("hot-remove-preserves-profile");
    await runtime.install(NSP_PLUGIN_ID);
    assert.equal(await connect(), first);
    checks.push("reinstall-reuses-profile");
    await facade.shutdown();
    await interruptNspRetrieval(root);
    const recovery = parseRecovery(await store.readRecoveryState(NSP_PLUGIN_ID));
    assert(
      recovery?.executionId,
      "A real accepted NSP execution must be journaled after interruption.",
    );
    await assert.rejects(
      client.cleanupExecution(randomUUID(), recovery.executionId).finally(() => client.close()),
    );
    checks.push("foreign-request-marker-cleanup-refused");
    ({ runtime, facade } = host());
    subscribe();
    const status = await invoke("nspCapture.status", {});
    assert(status.ok && status.status?.state === "cleanup-required");
    const prompt = await runtime.prepareChange(NSP_PLUGIN_ID, "remove");
    assert(prompt, "Pending cleanup must require a warning.");
    await assert.rejects(runtime.remove(NSP_PLUGIN_ID, prompt.token), /cleanup is still pending/u);
    assert.equal((await runtime.list()).plugins.length, 1);
    const cleaned = await invoke("nspCapture.cleanup", credentials);
    assert(cleaned.ok && cleaned.status?.state === "idle");
    assert.equal(await store.readRecoveryState(NSP_PLUGIN_ID), null);
    await client
      .cleanupExecution(recovery.requestId, recovery.executionId)
      .finally(() => client.close());
    assert((await invoke("nspCapture.cleanup", credentials)).ok);
    assert.equal(await connect(), first);
    assert.deepEqual(await workflow(), helper);
    checks.push(
      "real-interruption-recovery",
      "pending-cleanup-blocks-removal",
      "repeat-cleanup",
      "recovery-reuses-profile-and-workflow",
    );
    await runtime.remove(NSP_PLUGIN_ID);
    const serializedEvents = JSON.stringify(safeEvents);
    assert(!serializedEvents.includes(input.password));
    for (const record of profileStore.records()) {
      if (record.transport === "plaintext") continue;
      assert(!serializedEvents.includes(record.trust.material));
      if (record.trust.password) assert(!serializedEvents.includes(record.trust.password));
    }
    checks.push("secrets-absent-from-renderer-events");
    await removeOwnedNspTopic(admin, topic);
    fixtureAttempted = false;
    checks.push("owned-topic-deletion-confirmed");
  } catch (error) {
    let message = error instanceof Error ? error.message : "NSP qualification failed.";
    const privateValues = [input.password, input.apiUrl, input.username];
    for (const record of profileStore.records()) {
      privateValues.push(...record.brokers);
      if (record.transport !== "plaintext") {
        privateValues.push(record.trust.material);
        if (record.trust.password) privateValues.push(record.trust.password);
      }
    }
    for (const value of privateValues) if (value) message = message.split(value).join("[redacted]");
    details.failure = message.slice(0, 1024);
    failure = new Error(
      "Live NSP qualification failed; inspect completed checks in dist/ci/nsp-live.json.",
    );
  } finally {
    try {
      await facade.shutdown();
      try {
        if (fixtureAttempted && admin !== undefined) await removeOwnedNspTopic(admin, topic);
      } finally {
        await admin?.close();
        await client.close();
      }
      // Retain non-secret recovery journal on failure, so an operator can reconcile it.
      if ((await store.readRecoveryState(NSP_PLUGIN_ID)) === null)
        await rm(root, { recursive: true, force: true });
      else process.stderr.write(`NSP cleanup pending; recovery identifiers retained at ${root}.\n`);
    } catch {
      details.cleanupFailed = true;
      failure = new Error(
        "NSP qualification cleanup failed; reconcile the retained fixture state.",
      );
    }
  }
  await evidence(failure === undefined ? "passed" : "failed", checks, details);
  if (failure !== undefined) throw failure;
  process.stdout.write(`Live NSP: passed (${checks.join(", ")}; ${topics.length} topics).\n`);
}

void main().catch(async () => {
  const previous = JSON.parse(await readFile("dist/ci/nsp-live.json", "utf8")) as {
    outcome: string;
  };
  if (previous.outcome === "running")
    await evidence("failed", [], { reasonCode: "configuration-invalid" });
  process.stderr.write(
    "Live NSP qualification failed. Inspect completed checks in dist/ci/nsp-live.json and reconcile any retained cleanup state.\n",
  );
  process.exitCode = 1;
});
