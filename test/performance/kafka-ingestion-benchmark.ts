import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";

import { Admin, Producer } from "@platformatic/kafka";

import { StreamSkopeKafkaEngine } from "../../src/kafka/engine";
import {
  fixtureClientOptions,
  loadFixtureConfig,
  loadFixtureConnection,
} from "../support/kafka-fixture";

function option(name: string, fallback: number, maximum: number): number {
  const argument = process.argv.find((value) => value.startsWith(`--${name}=`));
  const value = argument === undefined ? fallback : Number(argument.split("=")[1]);
  assert(Number.isSafeInteger(value) && value > 0 && value <= maximum, `Invalid ${name}`);
  return value;
}

async function main(): Promise<void> {
  const seconds = option("seconds", 10, 900);
  const rate = option("rate", 1000, 50000);
  const bytes = option("bytes", 256, 262144);
  const partitions = option("partitions", 6, 32);
  const fixture = await loadFixtureConnection();
  const config = await loadFixtureConfig();
  const options = await fixtureClientOptions(
    fixture,
    config,
    `streamskope-benchmark-${randomUUID()}`,
  );
  const admin = new Admin(options);
  const producer = new Producer<Buffer, Buffer, Buffer, Buffer>({
    ...options,
    autocreateTopics: false,
  });
  const engine = new StreamSkopeKafkaEngine();
  const topic = `streamskope-performance-${randomUUID()}`;
  let created = false;
  let connection: Awaited<ReturnType<StreamSkopeKafkaEngine["openConnection"]>> | undefined;
  let stream: Awaited<ReturnType<NonNullable<typeof connection>["openMessageStream"]>> | undefined;
  let collection: Promise<void> | undefined;
  let collectionFailure: Error | undefined;
  try {
    await admin.createTopics({ topics: [topic], partitions, replicas: 1 });
    created = true;
    connection = await engine.openConnection(
      {
        brokers: [fixture.kafkaEndpoint],
        name: "Isolated performance fixture",
        oauth: {
          clientId: config.oauthClientId,
          clientSecret: config.oauthClientSecret,
          scope: config.oauthScope,
          tokenEndpoint: fixture.oauthEndpoint,
        },
        tls: { enabled: true, caPem: await readFile(fixture.caPath, "utf8") },
      },
      new AbortController().signal,
    );
    stream = await connection.openMessageStream(
      { topic, mode: "tail", maxMessages: 1000 },
      new AbortController().signal,
    );
    let fetched = 0;
    let acknowledged = 0;
    let fetchedBytes = 0;
    let peakRss = process.memoryUsage().rss;
    let peakBacklog = 0;
    const offsets = new Map<number, bigint>();
    const memorySamples: ReturnType<typeof process.memoryUsage>[] = [];
    const cpuStart = process.cpuUsage();
    const started = performance.now();
    collection = (async (): Promise<void> => {
      for await (const record of stream) {
        const offset = BigInt(record.offset);
        assert.equal(
          offset,
          (offsets.get(record.partition) ?? -1n) + 1n,
          "Per-partition gap or reordering",
        );
        offsets.set(record.partition, offset);
        fetched += 1;
        fetchedBytes += Buffer.byteLength(record.payload ?? "");
      }
    })().catch((error: unknown) => {
      collectionFailure = error instanceof Error ? error : new Error(String(error));
    });
    // One producer request in flight, additionally bounded to 1 MiB or 200 records.
    const batchLimit = Math.min(200, Math.max(1, Math.floor(1048576 / bytes)));
    const payload = Buffer.alloc(bytes, 120);
    let nextMemorySample = started;
    while (performance.now() - started < seconds * 1000) {
      if (collectionFailure !== undefined) throw collectionFailure;
      const due = Math.floor(((performance.now() - started) * rate) / 1000) - acknowledged;
      if (due <= 0) {
        await delay(1);
        continue;
      }
      const count = Math.min(due, batchLimit);
      await producer.send({
        messages: Array.from({ length: count }, (_, index) => ({
          topic,
          partition: (acknowledged + index) % partitions,
          key: Buffer.from(String(acknowledged + index)),
          value: payload,
        })),
      });
      acknowledged += count;
      peakBacklog = Math.max(peakBacklog, acknowledged - fetched);
      if (performance.now() >= nextMemorySample) {
        const memory = process.memoryUsage();
        peakRss = Math.max(peakRss, memory.rss);
        memorySamples.push(memory);
        if (memorySamples.length > 60) memorySamples.shift();
        nextMemorySample = performance.now() + 1000;
      }
    }
    const backlogAtEndOfOffer = acknowledged - fetched;
    const drainDeadline = performance.now() + 10000;
    while (
      fetched < acknowledged &&
      performance.now() < drainDeadline &&
      collectionFailure === undefined
    )
      await delay(10);
    const elapsedMs = performance.now() - started;
    const cpu = process.cpuUsage(cpuStart);
    process.stdout.write(
      `${JSON.stringify(
        {
          method:
            "Real TLS/OAuth fixture -> Platformatic -> production engine decoding. Producer and consumer share this Node process; CPU includes both, excludes broker/Electron/rules/IPC/rendering. One unique empty topic, no existing records or consumer offsets changed. Backlog is acknowledged-produced minus decoded records, not consumer-group committed lag.",
          versions: process.versions,
          workload: { seconds, rate, bytes, partitions },
          offered: rate * seconds,
          acknowledged,
          ungeneratedOffered: Math.max(0, rate * seconds - acknowledged),
          fetched,
          backlogAtEndOfOffer,
          backlogAfterDrain: acknowledged - fetched,
          peakBacklog,
          elapsedMs,
          fetchedPerSecond: (fetched * 1000) / elapsedMs,
          fetchedMiBPerSecond: (fetchedBytes * 1000) / elapsedMs / 1048576,
          cpuPercentOneCore: (cpu.user + cpu.system) / (elapsedMs * 10),
          peakRss,
          memorySamples,
          exactPartitionOffsets: Object.fromEntries(
            [...offsets].map(([partition, offset]) => [partition, String(offset)]),
          ),
        },
        null,
        2,
      )}\n`,
    );
    if (collectionFailure !== undefined) throw collectionFailure;
    assert.equal(fetched, acknowledged, "Broker records did not drain within 10 seconds");
  } finally {
    await stream?.close();
    await collection;
    await connection?.close();
    await producer.close();
    if (created) await admin.deleteTopics({ topics: [topic] });
    await admin.close();
  }
}

void main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});
