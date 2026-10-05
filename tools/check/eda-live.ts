import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

import { EDA_CAPTURE_DEFAULTS } from "../../plugins/eda/contracts";
import { KafkaConnectionScopes } from "../../src/features/kafka/application/connection-scope";
import { ObservationService } from "../../src/features/kafka/application/observation-service";
import { RelationshipService } from "../../src/features/kafka/application/relationship-service";
import type { StreamSkopeKafkaEngine } from "../../src/features/kafka/engine";
import type { EdaAgentTunnel } from "../../plugins/eda/backend/eda-agent-tunnel";

import { localEdaClient, requireTargetEdaVersion, selectLiveEdaTopic } from "./eda-fixture";

async function evidence(
  outcome: string,
  checks: readonly string[],
  reason?: string,
): Promise<void> {
  await mkdir("dist/ci", { recursive: true });
  await writeFile(
    "dist/ci/eda-live.json",
    JSON.stringify(
      {
        outcome,
        checkedAt: new Date().toISOString(),
        checks,
        ...(reason ? { reason } : {}),
      },
      null,
      2,
    ) + "\n",
  );
}

async function main(): Promise<void> {
  await evidence("running", []);
  assert(!process.env.GITHUB_ACTIONS, "Live EDA tests run only on the local maintainer host.");
  const client = await localEdaClient();
  if (!client) {
    await evidence("skipped", [], "Local EDA connection is not configured.");
    process.stdout.write("Live EDA: skipped (local EDA connection is not configured).\n");
    return;
  }
  const [{ StreamSkopeKafkaEngine }, { EdaAgentTunnel }, { EdaApiError }, { sourceFromResource }] =
    await Promise.all([
      import("../../src/features/kafka/engine"),
      import("../../plugins/eda/backend/eda-agent-tunnel"),
      import("../../plugins/eda/backend/eda-api-client"),
      import("../../plugins/eda/backend/eda-capture-source"),
    ]);
  const checks: string[] = [];
  const id = randomUUID();
  let created = false;
  let tunnel: EdaAgentTunnel | undefined;
  let connection: Awaited<ReturnType<StreamSkopeKafkaEngine["openConnection"]>> | undefined;
  let stream: Awaited<ReturnType<NonNullable<typeof connection>["openMessageStream"]>> | undefined;
  let failure: Error | undefined;
  try {
    requireTargetEdaVersion((await client.clusterVersion()).releaseVersion);
    checks.push("cluster-version");
    assert.equal(
      (await client.captureApplicationStatus()).state,
      "installed",
      "Install the versioned EDA capture app before live qualification.",
    );
    checks.push("application-version");
    const sources = (await client.listProducers()).flatMap((resource) => {
      if (resource.kind !== "Producer" && resource.kind !== "ClusterProducer") return [];
      const source = sourceFromResource(
        resource,
        "kafka.eda.nokia.com/v1",
        resource.kind,
        EDA_CAPTURE_DEFAULTS.namespace,
      );
      return source ? [source] : [];
    });
    const source = sources.find(
      (item) =>
        !process.env.STREAMSKOPE_EDA_CAPTURE_PRODUCER ||
        item.name === process.env.STREAMSKOPE_EDA_CAPTURE_PRODUCER,
    );
    assert(source, "The local EDA cluster must expose a producer with exported topics.");
    const original = (await client.getProducer(source)).spec;
    checks.push("source-discovery");
    const localPort = Number(process.env.STREAMSKOPE_EDA_CAPTURE_LOCAL_PORT ?? "19092");
    assert(
      Number.isSafeInteger(localPort) && localPort >= 1024 && localPort <= 65535,
      "Invalid local capture port.",
    );
    tunnel = await EdaAgentTunnel.listen(client, id, localPort);
    // Mark cleanup responsibility before sending POST; a lost response may still create the session.
    created = true;
    await client.createCaptureSession({ id, localPort, source, leaseSeconds: 900 });
    const deadline = Date.now() + 300_000;
    while (true) {
      const session = await client.getCaptureSession(id);
      if (session.phase === "Ready") break;
      assert.equal(session.phase, "Pending", "EDA capture failed to start.");
      assert(Date.now() < deadline, "EDA capture readiness timed out.");
      await delay(1000);
    }
    await client.renewCaptureSession(id, 900);
    checks.push("capture-ready", "lease-renewal");
    tunnel.activate();
    connection = await new StreamSkopeKafkaEngine().openConnection(
      {
        brokers: [`127.0.0.1:${localPort}`],
        name: "Local EDA CI capture",
        tls: { enabled: false },
      },
      AbortSignal.timeout(30_000),
    );
    // An unchanged onChange export may stay silent throughout a healthy capture.
    const topic = selectLiveEdaTopic(original, source.topics);
    assert(topic, "The producer needs an exported topic.");
    const receiptDeadline = Date.now() + 60_000;
    while (!(await connection.listTopics()).includes(topic)) {
      assert(Date.now() < receiptDeadline, "No exported Kafka topic appeared within 60 seconds.");
      await delay(1000);
    }
    stream = await connection.openMessageStream(
      { topic, mode: "tail", maxMessages: 1 },
      AbortSignal.timeout(60_000),
    );
    let received = false;
    const timeout = setTimeout(() => {
      void stream?.close();
    }, 60_000);
    try {
      for await (const record of stream) {
        assert.equal(record.topic, topic);
        received = true;
        break;
      }
    } finally {
      clearTimeout(timeout);
    }
    assert(
      received,
      "No real EDA Kafka record was received within 60 seconds; generate an event in the selected producer.",
    );
    checks.push("kafka-record-receipt");
    const context = { connection, generation: 0, connectionName: "Local EDA CI capture" };
    const scopes = new KafkaConnectionScopes(() => context);
    const observation = await new ObservationService(() => scopes.observation()).capture({
      topic,
      groupId: null,
      sampleRecords: false,
      thresholds: { lag: null, requestMs: null },
    });
    assert.equal(observation.series.topic, topic);
    assert(observation.series.samples[0]!.partitions.some((p) => p.endOffset !== null));
    checks.push("observed-health");
    const graph = await new RelationshipService(() => context, undefined, undefined).capture({
      topics: [topic],
      subject: null,
      version: null,
      sampleRecords: false,
    });
    assert(graph.nodes.some((n) => n.kind === "topic" && n.label === topic));
    assert.equal(graph.coverage.find((c) => c.source === "Kafka metadata")?.state, "complete");
    checks.push("relationship-discovery");
    assert.deepEqual(
      (await client.getProducer(source)).spec,
      original,
      "Capture changed the selected producer.",
    );
    checks.push("source-preserved");
  } catch (error) {
    failure = error instanceof Error ? error : new Error("Live EDA request failed.");
  }
  {
    // Close the Kafka consumer before its local tunnel, then ask the agent to remove only this UUID.
    for (const cleanup of [
      (): Promise<void> | undefined => stream?.close(),
      (): Promise<void> | undefined => connection?.close(),
      (): Promise<void> | undefined => tunnel?.close(),
    ]) {
      try {
        await cleanup();
      } catch (error) {
        failure ??= error instanceof Error ? error : new Error("Live EDA cleanup failed.");
      }
    }
    if (created) {
      try {
        await client.removeCaptureSession(id);
        // Agent DELETE returns only after all owned resources are absent and the finalizer is released.
        const cleanupDeadline = Date.now() + 30_000;
        while (true) {
          try {
            await client.getCaptureSession(id);
          } catch (error) {
            if (error instanceof EdaApiError && error.code === "PROFILE_NOT_FOUND") break;
            throw error;
          }
          assert(Date.now() < cleanupDeadline, "The capture session remained after stop.");
          await delay(1000);
        }
        checks.push("stop", "owned-resource-cleanup");
        // An expired session or a lost DELETE response must remain safe to remove again.
        await client.removeCaptureSession(id);
        checks.push("repeat-stop");
      } catch (error) {
        failure ??= error instanceof Error ? error : new Error("Live EDA cleanup failed.");
      }
    }
  }
  await evidence(failure ? "failed" : "passed", checks);
  if (failure) throw failure;
  process.stdout.write(
    "Live EDA: passed (discovery, capture, Kafka receipt, stop and owned-resource cleanup).\n",
  );
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  void main().catch(async () => {
    // Avoid serializing raw backend errors, URLs, credentials or record payloads into CI logs.
    process.stderr.write(
      "Live EDA qualification failed. Check local connection settings, app readiness and event generation. Inspect dist/ci/eda-live.json for completed stages.\n",
    );
    const previous = JSON.parse(await readFile("dist/ci/eda-live.json", "utf8")) as {
      outcome: string;
    };
    if (previous.outcome === "running")
      await evidence("failed", [], "Local EDA configuration failed validation.");
    process.exitCode = 1;
  });
}
