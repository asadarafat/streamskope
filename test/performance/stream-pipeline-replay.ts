import { cpus, freemem, totalmem } from "node:os";
import {
  createHistogram,
  monitorEventLoopDelay,
  performance,
  PerformanceObserver,
} from "node:perf_hooks";
import { setTimeout as delay } from "node:timers/promises";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_MESSAGE_LIMITS,
  type KafkaMessage,
} from "../../src/features/kafka/contracts";
import type { KafkaMessageStream } from "../../src/features/kafka/application";
import { initialKafkaUiState, reduceKafkaHostEvent } from "../../src/features/kafka/ui/state";
import {
  command,
  createFacade,
  message,
  RecordingActiveConnection,
  RecordingConnectionPort,
} from "../support/kafka-backend-facade-fixture";

function option(name: string, fallback: number, maximum: number): number {
  const argument = process.argv.find((value) => value.startsWith(`--${name}=`));
  const value = argument === undefined ? fallback : Number(argument.split("=")[1]);
  if (!Number.isSafeInteger(value) || value < 1 || value > maximum) {
    throw new Error(`Invalid ${name}: expected 1..${maximum}`);
  }
  return value;
}

async function main(): Promise<void> {
  const seconds = option("seconds", 10, 3600);
  const rate = option("rate", 1000, 100000);
  const bytes = option("bytes", 256, 262144);
  const partitions = option("partitions", 6, 100);
  const presentationMs = option("presentation-ms", 25, 1000);
  const burstMs = option("burst-ms", 10, 1000);
  const mixed = process.argv.includes("--mixed");
  const rules = process.argv.includes("--rules");
  const roundTrip = process.argv.includes("--clone");
  const payloads = [bytes, ...(mixed ? [1024, 65536] : [])].map((size) => "x".repeat(size));
  let generated = 0;
  let generatedBytes = 0;
  let delivered = 0;
  let batches = 0;
  let serializedBytes = 0;
  let stopped = false;
  let started = 0;
  let state = initialKafkaUiState;
  let peakQueue = 0;
  let peakQueueBytes = 0;
  let peakRetainedBytes = 0;
  let hostDisplayDrops = 0;
  let rendererEvictions = 0;
  let peakRss = 0;
  let peakHeap = 0;
  let peakExternal = 0;
  let gcCount = 0;
  let gcMs = 0;
  const memoryStart = process.memoryUsage();
  const memorySamples: ReturnType<typeof process.memoryUsage>[] = [];
  const reduction = createHistogram();
  const deliveryDelay = createHistogram();
  const loop = monitorEventLoopDelay({ resolution: 10 });
  const gc = new PerformanceObserver((list) => {
    for (const entry of list.getEntries()) {
      gcCount += 1;
      gcMs += entry.duration;
    }
  });
  gc.observe({ entryTypes: ["gc"] });
  loop.enable();

  const stream: KafkaMessageStream = {
    close: () => {
      stopped = true;
      return Promise.resolve();
    },
    async *[Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
      while (!stopped && performance.now() - started < seconds * 1000) {
        // Demand-driven generation with bounded work per turn; never allocate
        // an offered-load backlog when the application falls behind.
        const target = Math.floor(((performance.now() - started) * rate) / 1000);
        const count = Math.min(2000, Math.max(0, target - generated));
        for (let index = 0; index < count && !stopped; index += 1) {
          const sequence = generated++;
          const padding = `${payloads[sequence % payloads.length] ?? ""}${sequence}`;
          const payload = rules ? JSON.stringify({ index: sequence, data: padding }) : padding;
          generatedBytes += Buffer.byteLength(payload);
          yield {
            ...message(`${sequence}:${performance.now()}`, payload),
            offset: String(9007199254740993n + BigInt(sequence)),
            partition: sequence % partitions,
            timestamp: new Date(1700000000000 + sequence).toISOString(),
          };
        }
        await delay(burstMs);
      }
    },
  };
  const active = new RecordingActiveConnection();
  active.messageStreamOperations.push(() => Promise.resolve(stream));
  const port = new RecordingConnectionPort();
  port.openOperations.push(() => Promise.resolve(active));
  const facade = createFacade(
    port,
    (flush) => {
      const timer = setTimeout(flush, presentationMs);
      return (): void => clearTimeout(timer);
    },
    () => performance.now(),
  );
  const unsubscribe = facade.subscribe((event) => {
    if (event.event === "streamMetrics.changed") {
      peakQueue = Math.max(peakQueue, event.payload.queue?.peakMessages ?? 0);
      peakQueueBytes = Math.max(peakQueueBytes, event.payload.queue?.peakBytes ?? 0);
    }
    if (event.event === "messages.batch") {
      batches += 1;
      delivered += event.payload.messages.length;
      if (roundTrip) serializedBytes += Buffer.byteLength(JSON.stringify(event));
      const received = roundTrip ? structuredClone(event) : event;
      const before = performance.now();
      state = reduceKafkaHostEvent(state, received);
      hostDisplayDrops = Math.max(hostDisplayDrops, event.payload.droppedMessages);
      rendererEvictions = Math.max(rendererEvictions, state.rendererWindowEvictions ?? 0);
      reduction.record(Math.max(1, Math.round((performance.now() - before) * 1000)));
      for (const record of event.payload.messages) {
        const issued = Number(record.id.split(":")[1]);
        deliveryDelay.record(Math.max(1, Math.round((performance.now() - issued) * 1000)));
      }
      peakRetainedBytes = Math.max(peakRetainedBytes, state.retainedMessageBytes);
    } else {
      state = reduceKafkaHostEvent(state, event);
    }
  });
  const sample = (): void => {
    const memory = process.memoryUsage();
    peakRss = Math.max(peakRss, memory.rss);
    peakHeap = Math.max(peakHeap, memory.heapUsed);
    peakExternal = Math.max(peakExternal, memory.external);
    memorySamples.push(memory);
    if (memorySamples.length > 60) memorySamples.shift();
  };
  const memoryTimer = setInterval(sample, 1000);
  const cpuStart = process.cpuUsage();
  started = performance.now();
  try {
    if (rules) {
      for (let index = 0; index < 8; index += 1) {
        const response = await facade.execute({
          command: "rules.create",
          id: `rule-${index}`,
          payload: {
            rule: {
              name: `Benchmark ${index}`,
              expression: `$.index > ${index}`,
              enabled: true,
              cooldownMs: 1000,
              level: "info",
            },
          },
          version: HOST_PROTOCOL_VERSION,
        });
        if (!response.ok) throw new Error("Benchmark rule creation failed");
      }
    }
    const connected = await facade.execute(command("connection.connect", "replay-connect"));
    if (!connected.ok) throw new Error("Replay connection failed");
    const consuming = await facade.execute(command("messages.start", "replay-start"));
    if (!consuming.ok) throw new Error("Replay consumption failed");
    while (performance.now() - started < seconds * 1000) await delay(100);
    const stoppedResponse = await facade.execute(command("messages.stop", "replay-stop"));
    if (!stoppedResponse.ok) throw new Error("Replay stop failed");
    await facade.shutdown();
    sample();
    const elapsedMs = performance.now() - started;
    const cpu = process.cpuUsage(cpuStart);
    const cpuMs = (cpu.user + cpu.system) / 1000;
    const offered = rate * seconds;
    const boundsPassed =
      state.messages.length <= KAFKA_MESSAGE_LIMITS.retainedMessages &&
      peakRetainedBytes <= KAFKA_MESSAGE_LIMITS.retainedBytes &&
      peakQueue <= KAFKA_MESSAGE_LIMITS.queuedMessages &&
      peakQueueBytes <= KAFKA_MESSAGE_LIMITS.queuedBytes &&
      delivered + hostDisplayDrops === generated;
    process.stdout.write(
      `${JSON.stringify(
        {
          method:
            "Deterministic application-ingestion replay: production session, facade, live-rule evaluation and reducer in one Node process. Not Kafka fetch, actual IPC, React rendering or interaction latency.",
          config: {
            seconds,
            rate,
            bytes,
            mixed,
            rules,
            partitions,
            presentationMs,
            burstMs,
            roundTrip,
          },
          environment: {
            node: process.versions,
            platform: process.platform,
            arch: process.arch,
            cpu: cpus()[0]?.model,
            logicalCpus: cpus().length,
            totalMemory: totalmem(),
            availableMemory: freemem(),
          },
          elapsedMs,
          cpuMs,
          cpuPercentOneCore: (cpuMs / elapsedMs) * 100,
          offered,
          generated,
          generatedBytes,
          generatedPerSecond: (generated / elapsedMs) * 1000,
          generatedMiBPerSecond: (generatedBytes / 1048576 / elapsedMs) * 1000,
          ungeneratedOffered: Math.max(0, offered - generated),
          delivered,
          batches,
          serializedBytes,
          hostDisplayDrops,
          rendererEvictions,
          retained: state.messages.length,
          retainedBytes: state.retainedMessageBytes,
          peakQueue,
          peakQueueBytes,
          peakRetainedBytes,
          peakRss,
          peakHeap,
          peakExternal,
          memoryStart,
          memoryEnd: process.memoryUsage(),
          lastMemorySamples: memorySamples,
          gcCount,
          gcMs,
          eventLoopP95Ms: loop.percentile(95) / 1e6,
          eventLoopP99Ms: loop.percentile(99) / 1e6,
          reductionP95Ms: reduction.percentile(95) / 1000,
          reductionP99Ms: reduction.percentile(99) / 1000,
          ingestToReducerP95Ms: deliveryDelay.percentile(95) / 1000,
          ingestToReducerP99Ms: deliveryDelay.percentile(99) / 1000,
          boundsPassed,
        },
        null,
        2,
      )}\n`,
    );
    if (!boundsPassed) throw new Error("Replay delivery or retention bound failed");
  } finally {
    stopped = true;
    clearInterval(memoryTimer);
    unsubscribe();
    await facade.shutdown();
    gc.disconnect();
    loop.disable();
  }
}

void main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  process.exitCode = 1;
});
