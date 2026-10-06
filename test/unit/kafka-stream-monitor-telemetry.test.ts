import { afterEach, describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  parseHostEvent,
  type HostEvent,
  type KafkaStreamMonitorSnapshot,
} from "../../src/features/kafka/contracts";
import { ElectronEventDelivery } from "../../src/platform/electron/main/electron-event-delivery";
import {
  command,
  ControlledMessageStream,
  createFacade,
  message,
  RecordingActiveConnection,
  RecordingConnectionPort,
  settleAsyncIteration,
} from "../support/kafka-backend-facade-fixture";

afterEach(() => vi.useRealTimers());

interface TelemetryFixture {
  readonly connection: RecordingActiveConnection;
  readonly events: HostEvent[];
  readonly facade: ReturnType<typeof createFacade>;
  readonly flushes: Array<() => void>;
  readonly origin: number;
  readonly push: (count: number, payload?: string) => Promise<void>;
  readonly snapshot: () => KafkaStreamMonitorSnapshot;
  readonly stream: ControlledMessageStream;
}

async function fixture(batchSize = 200, finite = false): Promise<TelemetryFixture> {
  vi.useFakeTimers();
  const origin = Date.parse("2026-10-04T12:00:00.000Z");
  vi.setSystemTime(origin);
  const stream = new ControlledMessageStream();
  const connection = new RecordingActiveConnection();
  connection.messageStreamOperations.push(() => Promise.resolve(stream));
  const port = new RecordingConnectionPort();
  port.openOperations.push(() => Promise.resolve(connection));
  const flushes: Array<() => void> = [];
  const facade = createFacade(
    port,
    (flush) => {
      flushes.push(flush);
    },
    () => Date.now() - origin,
    undefined,
    undefined,
    undefined,
    undefined,
    () => new Date(),
  );
  const events: HostEvent[] = [];
  facade.subscribe((event) => {
    events.push(parseHostEvent(event));
  });
  await facade.execute(command("connection.connect", "connect"));
  await facade.execute({
    command: "preferences.update",
    id: "preferences",
    version: HOST_PROTOCOL_VERSION,
    payload: { patch: { stream: { batchSize } } },
  });
  await facade.execute({
    command: "messages.start",
    id: "operation-one",
    version: HOST_PROTOCOL_VERSION,
    payload: { mode: finite ? "earliest" : "tail", maxMessages: 1_000, topic: "test" },
  });
  const snapshot = (): KafkaStreamMonitorSnapshot => {
    const latest = events.filter((event) => event.event === "streamMetrics.changed").at(-1);
    if (!latest) throw new Error("Missing aggregate evidence");
    return latest.payload;
  };
  const push = async (count: number, payload = "value"): Promise<void> => {
    for (let index = 0; index < count; index += 1) {
      stream.push(message(String(index), payload));
      await settleAsyncIteration();
    }
  };
  return { connection, events, facade, flushes, origin, push, snapshot, stream };
}

describe("Kafka monitor measurement semantics", () => {
  it("samples quiet windows as zero without refreshing publication measurements and cancels sampling on Stop", async () => {
    const f = await fixture();
    try {
      await vi.advanceTimersByTimeAsync(10);
      await f.push(1);
      await vi.advanceTimersByTimeAsync(10);
      f.flushes.shift()?.();
      const published = f.snapshot();
      expect(published).toMatchObject({
        operationId: "operation-one",
        delivery: {
          publishedMessages: 1,
          messagesPerSecond: 50,
          rateWindowMs: 20,
          queueWaitMs: 10,
        },
        status: "nominal",
      });
      await vi.advanceTimersByTimeAsync(1_000);
      expect(f.snapshot()).toMatchObject({
        operationId: "operation-one",
        delivery: {
          publishedMessages: 1,
          messagesPerSecond: 0,
          rateWindowMs: 1_000,
          publicationSampledAt: published.delivery?.publicationSampledAt,
          queueWaitSampledAt: published.delivery?.queueWaitSampledAt,
        },
        queue: { droppedPerSecond: 0, oldestMessageAgeMs: null },
        status: "idle",
      });
      expect(f.snapshot().delivery?.rateSampledAt).not.toBe(published.delivery?.rateSampledAt);
      await f.facade.execute(command("messages.stop", "stop"));
      const eventCount = f.events.length;
      await vi.advanceTimersByTimeAsync(10_000);
      expect(f.events).toHaveLength(eventCount);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      await f.facade.shutdown();
    }
  });

  it("retains the real age of records remaining after a partial flush", async () => {
    const f = await fixture();
    try {
      await vi.advanceTimersByTimeAsync(10);
      await f.push(6, "x".repeat(600 * 1_024));
      await vi.advanceTimersByTimeAsync(90);
      f.flushes.shift()?.();
      expect(f.snapshot()).toMatchObject({
        delivery: { publishedMessages: 4, queueWaitMs: 90 },
        queue: { currentMessages: 2, oldestMessageAgeMs: 90 },
      });
      await vi.advanceTimersByTimeAsync(1_000);
      expect(f.snapshot().queue?.oldestMessageAgeMs).toBe(1_090);
      f.flushes.shift()?.();
      await f.facade.execute(command("messages.stop", "stop"));
      expect(f.snapshot()).toMatchObject({
        delivery: { publishedMessages: 6, queueWaitMs: 1_090 },
        queue: { currentMessages: 0, oldestMessageAgeMs: null },
      });
    } finally {
      await f.facade.shutdown();
    }
  });

  it("clears current pressure while preserving categorized historical display omissions", async () => {
    const f = await fixture();
    try {
      f.facade.setMessagePresentationPaused(true);
      await f.push(1_001);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(f.snapshot()).toMatchObject({
        status: "backpressure",
        queue: {
          pressureReasons: ["transport", "count-capacity"],
          droppedMessages: 1,
          dropReasons: { countCapacity: 1 },
        },
      });
      f.facade.setMessagePresentationPaused(false);
      for (let index = 0; index < 5; index += 1) f.flushes.shift()?.();
      await vi.advanceTimersByTimeAsync(1_000);
      expect(f.snapshot()).toMatchObject({
        status: "nominal",
        queue: {
          currentMessages: 0,
          pressureReasons: [],
          droppedMessages: 1,
          dropReasons: { countCapacity: 1 },
        },
        delivery: { publishedMessages: 1_000, receivedMessages: 1_001 },
      });
      await vi.advanceTimersByTimeAsync(1_000);
      expect(f.snapshot().status).toBe("idle");
    } finally {
      await f.facade.shutdown();
    }
  });

  it("reports current byte pressure near capacity and clears it when a batch frees room", async () => {
    const f = await fixture();
    try {
      await f.push(28, "x".repeat(600 * 1_024));
      await vi.advanceTimersByTimeAsync(1_000);
      expect(f.snapshot()).toMatchObject({
        status: "backpressure",
        queue: {
          pressureReasons: ["byte-capacity"],
          droppedMessages: 2,
          dropReasons: { byteCapacity: 2 },
        },
      });
      await vi.advanceTimersByTimeAsync(1);
      f.flushes.shift()?.();
      expect(f.snapshot()).toMatchObject({
        status: "nominal",
        queue: { pressureReasons: [], droppedMessages: 2, dropReasons: { byteCapacity: 2 } },
      });
    } finally {
      await f.facade.shutdown();
    }
  });

  it("publishes an unpressured finite queue with the minimum batch tuning without arbitrary count loss", async () => {
    const f = await fixture(10, true);
    try {
      await f.push(1_000);
      f.stream.end();
      await vi.advanceTimersByTimeAsync(10);
      expect(f.events.filter((event) => event.event === "messages.batch")).toHaveLength(100);
      expect(f.snapshot()).toMatchObject({
        state: "complete",
        delivery: { batchSize: 10, receivedMessages: 1_000, publishedMessages: 1_000 },
        queue: { droppedMessages: 0, currentMessages: 0, dropReasons: { terminalDiscarded: 0 } },
      });
    } finally {
      await f.facade.shutdown();
    }
  });

  it("honors synchronous transport pressure between terminal batches and accounts for every leftover record", async () => {
    const f = await fixture();
    try {
      await f.push(20, "x".repeat(600 * 1_024));
      const unsubscribe = f.facade.subscribe((event) => {
        if (event.event === "messages.batch") f.facade.setMessagePresentationPaused(true);
      });
      await vi.advanceTimersByTimeAsync(10);
      await expect(f.facade.execute(command("messages.stop", "stop"))).resolves.toMatchObject({
        ok: true,
      });
      unsubscribe();
      expect(f.events.filter((event) => event.event === "messages.batch")).toHaveLength(1);
      expect(f.snapshot()).toMatchObject({
        operationId: "operation-one",
        state: "stopped",
        delivery: { receivedMessages: 20, publishedMessages: 1 },
        queue: {
          currentMessages: 0,
          droppedMessages: 19,
          pressureReasons: [],
          dropReasons: { terminalDiscarded: 19 },
        },
      });
    } finally {
      await f.facade.shutdown();
    }
  });

  it("keeps a stalled Electron ACK queue within its byte limit during a large-record Stop", async () => {
    const f = await fixture();
    const failures: string[] = [];
    const sent: HostEvent[] = [];
    const delivery = new ElectronEventDelivery(
      (event) => {
        sent.push(event);
      },
      (reason) => {
        failures.push(reason);
      },
      (paused) => f.facade.setMessagePresentationPaused(paused),
    );
    const unsubscribe = f.facade.subscribe((event) => delivery.enqueue(event));
    try {
      await f.push(20, "x".repeat(600 * 1_024));
      await vi.advanceTimersByTimeAsync(10);
      await expect(f.facade.execute(command("messages.stop", "stop"))).resolves.toMatchObject({
        ok: true,
      });
      expect(failures).toEqual([]);
      expect(f.snapshot()).toMatchObject({
        delivery: { receivedMessages: 20, publishedMessages: 6 },
        queue: { droppedMessages: 14, currentMessages: 0, dropReasons: { terminalDiscarded: 14 } },
      });
      // Host publication is distinct from receipt: only one event has crossed IPC until ACKed.
      expect(sent).toHaveLength(1);
      for (let index = 0; index < sent.length; index += 1)
        delivery.acknowledge(sent[index]!.sequence);
      expect(sent.filter((event) => event.event === "messages.batch")).toHaveLength(6);
      expect(failures).toEqual([]);
    } finally {
      unsubscribe();
      delivery.close();
      await f.facade.shutdown();
    }
  });

  it("preserves loading ownership while coalescing later observations for a new operation", async () => {
    const f = await fixture();
    const sent: HostEvent[] = [];
    const delivery = new ElectronEventDelivery(
      (event) => {
        sent.push(event);
      },
      () => {
        throw new Error("Unexpected transport failure");
      },
    );
    const unsubscribe = f.facade.subscribe((event) => delivery.enqueue(event));
    try {
      const replacement = new ControlledMessageStream();
      f.connection.messageStreamOperations.push(() => Promise.resolve(replacement));
      await f.facade.execute(command("messages.start", "operation-two"));
      await vi.advanceTimersByTimeAsync(3_000);
      await f.facade.execute(command("messages.stop", "stop"));
      for (let index = 0; index < sent.length; index += 1)
        delivery.acknowledge(sent[index]!.sequence);
      const monitor = sent.filter((event) => event.event === "streamMetrics.changed");
      expect(monitor.map((event) => event.payload.state)).toEqual(["loading", "stopped"]);
      expect(monitor.every((event) => event.payload.operationId === "operation-two")).toBe(true);
      expect(sent.map((event) => event.sequence)).toEqual(
        sent.map((event) => event.sequence).sort((left, right) => left - right),
      );
    } finally {
      unsubscribe();
      delivery.close();
      await f.facade.shutdown();
    }
  });

  it("coalesces blocked heartbeats so Stop can publish terminal evidence before the ACK deadline", async () => {
    const f = await fixture();
    const failures: string[] = [];
    const delivery = new ElectronEventDelivery(
      () => undefined,
      (reason) => {
        failures.push(reason);
      },
      (paused) => f.facade.setMessagePresentationPaused(paused),
    );
    const unsubscribe = f.facade.subscribe((event) => delivery.enqueue(event));
    try {
      for (let sequence = 1; sequence <= 32; sequence += 1) {
        delivery.enqueue({
          event: "backend.availability",
          payload: { state: "ready" },
          sequence,
          version: HOST_PROTOCOL_VERSION,
        });
      }
      await f.push(20);
      await vi.advanceTimersByTimeAsync(29_000);
      await f.facade.execute(command("messages.stop", "stop"));
      expect(failures).toEqual([]);
      expect(f.snapshot()).toMatchObject({
        state: "stopped",
        delivery: { publishedMessages: 0, receivedMessages: 20 },
        queue: { droppedMessages: 20, dropReasons: { terminalDiscarded: 20 }, pressureReasons: [] },
      });
    } finally {
      unsubscribe();
      delivery.close();
      await f.facade.shutdown();
    }
  });

  it("classifies a serialized oversize record without misreporting pressure or a publication duration", async () => {
    const f = await fixture();
    try {
      // JSON escaping expands this retained 200 KiB string beyond the event byte budget.
      await f.push(1, "\u0000".repeat(200 * 1_024));
      await vi.advanceTimersByTimeAsync(10);
      f.flushes.shift()?.();
      expect(f.snapshot()).toMatchObject({
        status: "idle",
        delivery: { publishedMessages: 0, publicationDurationMs: null, publicationSampledAt: null },
        queue: { droppedMessages: 1, dropReasons: { oversized: 1 }, pressureReasons: [] },
      });
      expect(f.events.some((event) => event.event === "messages.batch")).toBe(false);
    } finally {
      await f.facade.shutdown();
    }
  });

  it("restarts counts and operation ownership for a second identical request and keeps one sampler", async () => {
    const f = await fixture();
    try {
      await f.push(2);
      await vi.advanceTimersByTimeAsync(10);
      f.flushes.shift()?.();
      const replacement = new ControlledMessageStream();
      f.connection.messageStreamOperations.push(() => Promise.resolve(replacement));
      await f.facade.execute(command("messages.start", "operation-two"));
      expect(f.snapshot()).toMatchObject({
        operationId: "operation-two",
        delivery: { publishedMessages: 0, receivedMessages: 0 },
      });
      const rateSamples = (): HostEvent[] =>
        f.events.filter(
          (event) =>
            event.event === "streamMetrics.changed" &&
            event.payload.operationId === "operation-two" &&
            event.payload.delivery?.rateSampledAt !== null,
        );
      const before = rateSamples().length;
      await vi.advanceTimersByTimeAsync(1_000);
      expect(rateSamples()).toHaveLength(before + 1);
      expect(f.snapshot().operationId).toBe("operation-two");
      expect(f.snapshot().delivery?.rateWindowMs).toBe(1_000);
      expect(HOST_PROTOCOL_VERSION).toBe(51);
    } finally {
      await f.facade.shutdown();
    }
  });
});
