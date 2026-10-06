import { randomUUID } from "node:crypto";
import { monitorEventLoopDelay, performance } from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";

import { connect, headers, type NatsConnection } from "@nats-io/transport-node";

import { InMemoryNatsProfileStore } from "../../src/features/nats/application";
import {
  NATS_LIMITS,
  NATS_PROTOCOL_VERSION,
  parseCorrelatedNatsResponse,
  parseNatsCommand,
  parseNatsEvent,
  type NatsCommand,
  type NatsCommandName,
  type NatsCommandResultMap,
  type NatsSubscriptionCounters,
} from "../../src/features/nats/contracts";
import { createNatsBackend } from "../../src/platform/node/nats-backend";
import { createNatsProviderEndpoint } from "../../src/platform/node/nats-provider";
import type { ProviderWireEndpoint } from "../../src/platform/node/provider-host";
import { prepareLocalNatsDevelopmentProfile } from "../../tools/dev/nats-fixture/development-profile";
import { NatsFixtureLifecycle } from "../../tools/dev/nats-fixture/lifecycle";
import {
  loadNatsFixtureRecord,
  natsFixtureConnection,
} from "../../tools/dev/nats-fixture/ownership";
import { writePerformanceEvidence } from "../support/performance-evidence";
import { STREAM_REPLAY_QUALIFICATION_BUDGET } from "../support/stream-replay-qualification";

const workload = Object.freeze({
  seconds: 60,
  rate: 1_000,
  payloadPaddingBytes: 256,
  burstMilliseconds: 100,
  recordsPerBurst: 100,
  scheduledRecords: 60_000,
  maximumLoadMilliseconds: 65_000,
  overallDeadlineMilliseconds: 80_000,
});

class NatsSoakFailure extends Error {}

function requireCondition(condition: unknown, message: string): asserts condition {
  if (!condition) throw new NatsSoakFailure(message);
}

/** Cancels the wait without losing observation of a late SDK/host rejection. */
async function bounded<T>(
  work: Promise<T>,
  milliseconds: number,
  signal?: AbortSignal,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const onAbort = (): void =>
    rejectCancelled?.(new NatsSoakFailure("Overall soak deadline or cancellation reached."));
  let rejectCancelled: ((error: Error) => void) | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new NatsSoakFailure("A bounded soak operation timed out.")),
          Math.max(1, milliseconds),
        );
      }),
      new Promise<never>((_resolve, reject) => {
        rejectCancelled = reject;
        signal?.addEventListener("abort", onAbort, { once: true });
        if (signal?.aborted) onAbort();
      }),
    ]);
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", onAbort);
  }
}

async function main(): Promise<void> {
  const started = performance.now();
  const cancellation = new AbortController();
  const overallTimer = setTimeout(() => cancellation.abort(), workload.overallDeadlineMilliseconds);
  const onSignal = (): void => cancellation.abort();
  process.once("SIGINT", onSignal);
  process.once("SIGTERM", onSignal);
  const failures = new Set<string>();
  const received = new Set<number>();
  const padding = "x".repeat(workload.payloadPaddingBytes);
  const loop = monitorEventLoopDelay({ resolution: 10 });
  let endpoint: ProviderWireEndpoint | undefined;
  let publisher: NatsConnection | undefined;
  let openingPublisher: Promise<NatsConnection> | undefined;
  let closing = false;
  let latePublisherCleanupFailed = false;
  let unsubscribe = (): void => undefined;
  let memoryTimer: ReturnType<typeof setInterval> | undefined;
  let phase = "preflight";
  let serverImage: string | null = null;
  let generation: string | null = null;
  let published = 0;
  let delivered = 0;
  let duplicates = 0;
  let invalidRecords = 0;
  let counterSamples = 0;
  let observedQueueRecordsMaximum: number | null = null;
  let observedQueueBytesMaximum: number | null = null;
  let observedApplicationOmissionsMaximum = 0;
  let observedTransportOmissionsMaximum = 0;
  let boundarySecretsDetected = false;
  let measuredLoadMilliseconds: number | null = null;
  let sampledPeakNodeRssBytes: number | null = null;
  let cpuPercentOneCore: number | null = null;
  let eventLoopP99Milliseconds: number | null = null;
  let measuredStarted: number | undefined;
  let cpuStarted: ReturnType<typeof process.cpuUsage> | undefined;
  let confirmedRepeatStop = false;
  let noPostStopDelivery = false;
  let confirmedRepeatDisconnect = false;
  let cleanupConfirmed: boolean;
  let existingOwnedServerVerified = false;
  let deliveryDrainMilliseconds: number | null = null;

  const sampleMemory = (): void => {
    sampledPeakNodeRssBytes = Math.max(sampledPeakNodeRssBytes ?? 0, process.memoryUsage().rss);
  };
  const finishLoadMeasurement = (): void => {
    if (measuredStarted === undefined || cpuStarted === undefined) return;
    measuredLoadMilliseconds = performance.now() - measuredStarted;
    const cpu = process.cpuUsage(cpuStarted);
    cpuPercentOneCore = ((cpu.user + cpu.system) / 1_000 / measuredLoadMilliseconds) * 100;
    sampleMemory();
    clearInterval(memoryTimer);
    loop.disable();
    eventLoopP99Milliseconds = loop.percentile(99) / 1e6;
    measuredStarted = undefined;
    cpuStarted = undefined;
  };
  const observeCounters = (counters: NatsSubscriptionCounters): void => {
    counterSamples += 1;
    observedQueueRecordsMaximum = Math.max(
      observedQueueRecordsMaximum ?? 0,
      counters.queuedRecords,
    );
    observedQueueBytesMaximum = Math.max(observedQueueBytesMaximum ?? 0, counters.queuedBytes);
    observedApplicationOmissionsMaximum = Math.max(
      observedApplicationOmissionsMaximum,
      counters.applicationOmittedRecords,
    );
    observedTransportOmissionsMaximum = Math.max(
      observedTransportOmissionsMaximum,
      counters.transportOmittedRecords,
    );
    if (
      counters.queuedRecords > NATS_LIMITS.queuedRecords ||
      counters.queuedBytes > NATS_LIMITS.queuedBytes
    )
      failures.add("A publicly observed queue counter exceeded its bound.");
  };

  try {
    requireCondition(process.versions.node.split(".")[0] === "24", "The soak requires Node 24.");
    requireCondition(
      process.argv.length === 2,
      "The fixed qualification workload accepts no options.",
    );
    const fixtureStatus = await bounded(
      new NatsFixtureLifecycle(process.cwd()).status(),
      30_000,
      cancellation.signal,
    );
    requireCondition(
      fixtureStatus.status === "ready",
      "The existing owned NATS fixture was not ready.",
    );
    const owned = await loadNatsFixtureRecord(process.cwd());
    requireCondition(
      owned !== undefined,
      "Start the persistent Local AIO NATS fixture before qualification.",
    );
    serverImage = owned.image;
    const input = await natsFixtureConnection(owned);
    requireCondition(
      input.authentication.mode === "token" && input.tls.mode === "tls",
      "The existing fixture must use token authentication and verified TLS.",
    );
    const privateToken = input.authentication.token;
    const privateCa = input.tls.caPem;
    existingOwnedServerVerified = true;
    const detectSecrets = (wire: unknown): void => {
      const serialized = JSON.stringify(wire);
      requireCondition(
        typeof serialized === "string",
        "A public NATS boundary was not serializable.",
      );
      boundarySecretsDetected ||=
        serialized.includes(privateToken) ||
        (privateCa !== undefined && serialized.includes(privateCa)) ||
        serialized.includes("-----BEGIN CERTIFICATE-----");
    };
    const store = new InMemoryNatsProfileStore();
    requireCondition(
      (await prepareLocalNatsDevelopmentProfile(store, process.cwd())) === "seeded",
      "The host-only AIO NATS profile was not prepared.",
    );
    const host = createNatsProviderEndpoint(createNatsBackend({ profileStore: store }));
    endpoint = host;
    const subject = `streamskope.qualification.${randomUUID()}`;

    async function request<Name extends NatsCommandName>(
      name: Name,
      payload: Extract<NatsCommand, { readonly command: Name }>["payload"],
    ): Promise<NatsCommandResultMap[Name]> {
      const submitted = parseNatsCommand({
        version: NATS_PROTOCOL_VERSION,
        id: randomUUID(),
        command: name,
        payload,
      }) as Extract<NatsCommand, { readonly command: Name }>;
      const wire = await bounded(host.dispatch(submitted), 5_000, cancellation.signal);
      detectSecrets(wire);
      const response = parseCorrelatedNatsResponse(wire, submitted);
      requireCondition(response.ok, "A production NATS host command failed.");
      return response.result;
    }

    unsubscribe = host.subscribe((wire) => {
      try {
        detectSecrets(wire);
        const event = parseNatsEvent(wire);
        if ("counters" in event.payload) observeCounters(event.payload.counters);
        if (event.event !== "records.batch") return;
        for (const record of event.payload.records) {
          delivered += 1;
          const value: unknown =
            record.payload.encoding === "utf8" ? JSON.parse(record.payload.data) : null;
          const payload =
            value !== null && typeof value === "object" && !Array.isArray(value)
              ? (value as { readonly sequence?: unknown; readonly padding?: unknown })
              : undefined;
          if (
            record.subject !== subject ||
            record.generation !== generation ||
            record.headersTruncated ||
            record.timestampProvenance !== "host-received" ||
            record.headers.find((header) => header.name === "x-fixture")?.values[0] !==
              "aio-nats-soak" ||
            record.headers.find((header) => header.name === "content-type")?.values[0] !==
              "application/json" ||
            typeof payload?.sequence !== "number" ||
            !Number.isSafeInteger(payload.sequence) ||
            payload.sequence < 0 ||
            payload.sequence >= workload.scheduledRecords ||
            payload.padding !== padding ||
            record.payload.data !== JSON.stringify({ sequence: payload.sequence, padding }) ||
            record.payloadBytes !== Buffer.byteLength(record.payload.data)
          ) {
            invalidRecords += 1;
            continue;
          }
          if (received.has(payload.sequence)) duplicates += 1;
          received.add(payload.sequence);
        }
      } catch {
        invalidRecords += 1;
        failures.add("A public event failed strict codec or payload validation.");
      }
    });

    phase = "connection setup";
    await request("profiles.list", {});
    const connected = await request("profiles.connect", {
      profileId: "local-aio-nats",
      expectedRevision: 1,
    });
    requireCondition(
      connected.connection.state === "connected",
      "The production connection was not confirmed.",
    );
    const subscription = await request("subscription.start", { subject });
    requireCondition(
      subscription.subscription.state === "streaming" &&
        subscription.subscription.generation !== null,
      "The production subscription was not confirmed.",
    );
    generation = subscription.subscription.generation;
    openingPublisher = connect({
      servers: [...input.servers],
      token: privateToken,
      tls: { rejectUnauthorized: true, ...(privateCa === undefined ? {} : { ca: privateCa }) },
      reconnect: false,
      timeout: 1_000,
      waitOnFirstConnect: false,
      ignoreClusterUpdates: true,
      noRandomize: true,
      debug: false,
    }).then(async (client) => {
      if (closing || cancellation.signal.aborted) {
        const cleanup = await Promise.allSettled([
          bounded(client.close(), 5_000),
          bounded(client.closed(), 5_000),
        ]);
        latePublisherCleanupFailed ||= cleanup.some((result) => result.status === "rejected");
        throw new NatsSoakFailure("Publisher setup completed after cancellation and was closed.");
      }
      publisher = client;
      return client;
    });
    publisher = await bounded(openingPublisher, 2_000, cancellation.signal);
    const metadata = headers();
    metadata.set("x-fixture", "aio-nats-soak");
    metadata.set("content-type", "application/json");
    await bounded(publisher.flush(), 2_000, cancellation.signal);

    phase = "60-second real load";
    measuredStarted = performance.now();
    cpuStarted = process.cpuUsage();
    loop.enable();
    sampleMemory();
    memoryTimer = setInterval(sampleMemory, 100);
    const loadStarted = measuredStarted;
    const loadDeadline = loadStarted + workload.maximumLoadMilliseconds;
    for (let burst = 0; burst < workload.scheduledRecords / workload.recordsPerBurst; burst += 1) {
      cancellation.signal.throwIfAborted();
      requireCondition(
        performance.now() < loadDeadline,
        "The offered workload exceeded its 65-second deadline.",
      );
      requireCondition(
        !publisher.isClosed(),
        "The independent publisher disconnected during the workload.",
      );
      for (let index = 0; index < workload.recordsPerBurst; index += 1) {
        publisher.publish(subject, JSON.stringify({ sequence: published, padding }), {
          headers: metadata,
        });
        published += 1;
      }
      await bounded(
        publisher.flush(),
        Math.min(2_000, loadDeadline - performance.now()),
        cancellation.signal,
      );
      await delay(
        Math.max(
          0,
          Math.ceil((burst + 1) * workload.burstMilliseconds - (performance.now() - loadStarted)),
        ),
        undefined,
        { signal: cancellation.signal },
      );
    }
    while (performance.now() - loadStarted < workload.seconds * 1_000)
      await delay(
        Math.ceil(workload.seconds * 1_000 - (performance.now() - loadStarted)),
        undefined,
        { signal: cancellation.signal },
      );
    finishLoadMeasurement();

    phase = "delivery drain";
    const drainStarted = performance.now();
    const drainDeadline = drainStarted + 10_000;
    while (received.size < published && performance.now() < drainDeadline)
      await delay(25, undefined, { signal: cancellation.signal });
    deliveryDrainMilliseconds = performance.now() - drainStarted;
    requireCondition(
      received.size === published && delivered === published,
      "Real offered records were missing or delivered more than once.",
    );
    requireCondition(
      duplicates === 0 && invalidRecords === 0,
      "Real record fidelity or uniqueness failed.",
    );
    requireCondition(
      observedApplicationOmissionsMaximum === 0 && observedTransportOmissionsMaximum === 0,
      "The production host reported record omissions.",
    );
    requireCondition(
      !boundarySecretsDetected,
      "The public NATS boundary exposed fixture credentials.",
    );

    phase = "confirmed stop and disconnect";
    const stopped = await request("subscription.stop", {});
    observeCounters(stopped.subscription.counters);
    requireCondition(
      stopped.subscription.state === "stopped" &&
        stopped.subscription.counters.receivedRecords === published &&
        stopped.subscription.counters.publishedRecords === published &&
        stopped.subscription.counters.queuedRecords === 0 &&
        stopped.subscription.counters.queuedBytes === 0 &&
        stopped.subscription.counters.applicationOmittedRecords === 0 &&
        stopped.subscription.counters.transportOmittedRecords === 0,
      "Confirmed stop did not account for every real record.",
    );
    const repeatedStop = await request("subscription.stop", {});
    requireCondition(
      repeatedStop.subscription.state === "stopped",
      "Repeat stop was not confirmed.",
    );
    confirmedRepeatStop = true;
    const beforeSentinel = delivered;
    publisher.publish(subject, JSON.stringify({ sequence: published, padding: "post-stop" }), {
      headers: metadata,
    });
    await bounded(publisher.flush(), 2_000, cancellation.signal);
    await delay(150, undefined, { signal: cancellation.signal });
    requireCondition(delivered === beforeSentinel, "A record arrived after the confirmed stop.");
    noPostStopDelivery = true;
    const disconnected = await request("connection.disconnect", {});
    const repeatedDisconnect = await request("connection.disconnect", {});
    requireCondition(
      disconnected.connection.state === "disconnected" &&
        repeatedDisconnect.connection.state === "disconnected",
      "Repeat disconnect was not confirmed.",
    );
    confirmedRepeatDisconnect = true;
  } catch (error) {
    failures.add(
      error instanceof NatsSoakFailure
        ? error.message
        : `Real NATS qualification failed during ${phase}; private runtime details were suppressed.`,
    );
  } finally {
    closing = true;
    finishLoadMeasurement();
    const cleanup = await Promise.allSettled([
      Promise.resolve().then(async () => {
        if (endpoint !== undefined) await bounded(endpoint.shutdown(), 5_000);
      }),
      Promise.resolve().then(async () => {
        if (openingPublisher !== undefined)
          await bounded(
            openingPublisher.then(
              () => undefined,
              () => undefined,
            ),
            5_000,
          );
        if (latePublisherCleanupFailed)
          throw new NatsSoakFailure("Late publisher cleanup could not be confirmed.");
      }),
      Promise.resolve().then(async () => {
        if (publisher !== undefined) {
          await bounded(publisher.close(), 5_000);
          await bounded(publisher.closed(), 5_000);
        }
      }),
    ]);
    cleanupConfirmed = cleanup.every((result) => result.status === "fulfilled");
    if (!cleanupConfirmed)
      failures.add("Provider or independent publisher cleanup could not be confirmed.");
    unsubscribe();
    loop.disable();
    clearInterval(memoryTimer);
    clearTimeout(overallTimer);
    process.removeListener("SIGINT", onSignal);
    process.removeListener("SIGTERM", onSignal);
  }

  const budget = STREAM_REPLAY_QUALIFICATION_BUDGET;
  if (
    duplicates !== 0 ||
    invalidRecords !== 0 ||
    received.size !== published ||
    delivered !== published
  )
    failures.add("Final real-record accounting, uniqueness or byte/header fidelity failed.");
  if (observedApplicationOmissionsMaximum !== 0 || observedTransportOmissionsMaximum !== 0)
    failures.add("The final public counters reported record omissions.");
  if (confirmedRepeatStop && delivered !== published) {
    noPostStopDelivery = false;
    failures.add("A late record arrived after confirmed stop during disconnect or cleanup.");
  }
  const offeredRecordsAtConfiguredRate =
    measuredLoadMilliseconds === null
      ? workload.scheduledRecords
      : Math.floor((measuredLoadMilliseconds / 1_000) * workload.rate);
  const generatedOfferedRatio =
    offeredRecordsAtConfiguredRate === 0 ? 0 : published / offeredRecordsAtConfiguredRate;
  if (generatedOfferedRatio < budget.minimumGeneratedRatio)
    failures.add("Generated/offered throughput fell below the shared minimum ratio.");
  if (
    measuredLoadMilliseconds === null ||
    measuredLoadMilliseconds < workload.seconds * 1_000 ||
    measuredLoadMilliseconds > workload.maximumLoadMilliseconds
  )
    failures.add("The real load did not complete within its qualified duration.");
  if (
    cpuPercentOneCore === null ||
    !Number.isFinite(cpuPercentOneCore) ||
    cpuPercentOneCore > budget.maximumCpuPercentOneCore
  )
    failures.add("Node CPU exceeded its shared one-core budget or was not measured.");
  if (sampledPeakNodeRssBytes === null || sampledPeakNodeRssBytes > budget.maximumPeakRssBytes)
    failures.add("Sampled Node RSS exceeded its shared budget or was not measured.");
  if (
    eventLoopP99Milliseconds === null ||
    !Number.isFinite(eventLoopP99Milliseconds) ||
    eventLoopP99Milliseconds > budget.maximumEventLoopP99Ms
  )
    failures.add("Node event-loop p99 exceeded its shared budget or was not measured.");
  if (boundarySecretsDetected)
    failures.add("The public NATS boundary exposed fixture credentials.");
  const outcome = failures.size === 0 ? "passed" : "failed";
  await writePerformanceEvidence("nats-live-soak.json", {
    check: "real-aio-nats-provider-soak",
    command: "node --import tsx test/performance/nats-live-soak.ts",
    outcome,
    ...(outcome === "failed" ? { failure: [...failures].join(" ") } : {}),
    sampleMethod:
      "Fixed 60-second independent SDK publication through the existing pinned token/verified-TLS AIO NATS server and production provider endpoint. CPU, event-loop and sampled RSS cover only the load interval in this Node process; server resource use, React interaction latency, renderer paint and native IPC latency are not measured.",
    evidence: {
      workload,
      serverImage,
      published,
      delivered,
      uniqueReceived: received.size,
      offeredRecordsAtConfiguredRate,
      generatedOfferedRatio,
      ungeneratedOfferedRecords: Math.max(0, offeredRecordsAtConfiguredRate - published),
      duplicates,
      invalidRecords,
      boundarySecretsDetected,
      publicCounterObservations: {
        samples: counterSamples,
        observedQueueRecordsMaximum,
        observedQueueBytesMaximum,
        observedApplicationOmissionsMaximum,
        observedTransportOmissionsMaximum,
        internalQueueHighWaterMarks: "not measured; maxima cover only publicly emitted counters",
      },
      measurement: {
        loadMilliseconds: measuredLoadMilliseconds,
        totalMilliseconds: performance.now() - started,
        deliveryDrainMilliseconds,
        sampledPeakNodeRssBytes,
        rssSampleIntervalMilliseconds: 100,
        cpuPercentOneCore,
        eventLoopP99Milliseconds,
      },
      qualificationBudgets: {
        minimumGeneratedRatio: budget.minimumGeneratedRatio,
        maximumCpuPercentOneCore: budget.maximumCpuPercentOneCore,
        maximumPeakRssBytes: budget.maximumPeakRssBytes,
        maximumEventLoopP99Ms: budget.maximumEventLoopP99Ms,
      },
      lifecycle: {
        confirmedRepeatStop,
        noPostStopDelivery,
        confirmedRepeatDisconnect,
        providerAndPublisherCleanupConfirmed: cleanupConfirmed,
        existingOwnedServerVerified,
        providerCreated: endpoint !== undefined,
        publisherConnected: publisher !== undefined,
        persistentServerLifecycle:
          "no persistent server creation, stop or removal operation is invoked by this soak",
      },
      failures: [...failures],
    },
  });
  if (outcome === "failed") process.exitCode = 1;
}

void main().catch(() => {
  process.stderr.write(
    "Real NATS soak evidence could not be completed. Check the private fixture and performance output directory.\n",
  );
  process.exitCode = 1;
});
