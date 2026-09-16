import { describe, expect, it, vi } from "vitest";

import {
  KAFKA_MESSAGE_LIMITS,
  type HostEvent,
  type KafkaOperationalPreferences,
} from "../../src/features/kafka/contracts";
import {
  InMemoryKafkaOperationalPreferenceStore,
  KafkaOperationalPreferenceService,
} from "../../src/features/kafka/application";
import {
  ControlledMessageStream,
  RecordingActiveConnection,
  RecordingConnectionPort,
  command,
  createFacade,
  message,
  settleAsyncIteration,
} from "../support/kafka-backend-facade-fixture";

function preferences(): KafkaOperationalPreferences {
  return {
    fetch: { maxMessages: 100, mode: "newest" },
    latency: {
      acknowledgements: 1,
      messageCount: 20,
      runbookUrl: null,
      timeoutMs: 10_000,
    },
    rules: {
      logLevel: "info",
      loggingEnabled: true,
      notificationsEnabled: true,
    },
    stream: {
      batchSize: 10,
      historySamples: 10,
      intervalMs: 50,
      queueDepth: 100,
    },
  };
}

describe("Kafka operational-preference stream tuning", () => {
  it("binds queue, batch, interval and history to one operation despite a later save", async () => {
    const firstStream = new ControlledMessageStream();
    const secondStream = new ControlledMessageStream();
    const activeConnection = new RecordingActiveConnection();
    activeConnection.messageStreamOperations.push(
      () => Promise.resolve(firstStream),
      () => Promise.resolve(secondStream),
    );
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const preferenceService = new KafkaOperationalPreferenceService(
      new InMemoryKafkaOperationalPreferenceStore(
        { durability: "session", state: "ready" },
        preferences(),
      ),
    );
    const scheduled: Array<{ readonly delayMs: number; readonly flush: () => void }> = [];
    const facade = createFacade(
      port,
      (flush, delayMs) => {
        scheduled.push({ delayMs, flush });
        return (): void => undefined;
      },
      undefined,
      undefined,
      preferenceService,
    );
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });
    await facade.execute(command("connection.connect", "connect"));
    await facade.execute(command("messages.start", "first-operation"));

    for (let index = 0; index < 105; index += 1) {
      firstStream.push(message(String(index)));
    }
    await vi.waitFor(() => {
      expect(firstStream.deliveredMessages).toBe(105);
    });
    expect(scheduled).toHaveLength(1);
    expect(scheduled[0]?.delayMs).toBe(50);

    scheduled.shift()?.flush();
    const firstMeasurement = events
      .filter((event) => event.event === "streamMetrics.changed")
      .at(-1);
    expect(firstMeasurement).toMatchObject({
      payload: {
        delivery: {
          batchSize: 10,
          deliveredMessages: 10,
          historySamples: 10,
          intervalMs: 50,
          lastBatchMessages: 10,
          tuningSource: "confirmed",
        },
        queue: {
          capacityBytes: KAFKA_MESSAGE_LIMITS.queuedBytes,
          capacityMessages: 100,
          currentMessages: 90,
          droppedMessages: 5,
        },
        status: "backpressure",
      },
    });

    await preferenceService.update({
      stream: {
        batchSize: 20,
        historySamples: 20,
        intervalMs: 100,
        queueDepth: 200,
      },
    });
    while (scheduled.length > 0) {
      const next = scheduled.shift();
      expect(next?.delayMs).toBe(50);
      next?.flush();
    }
    const firstOperationMeasurements = events.filter(
      (event): event is Extract<HostEvent, { readonly event: "streamMetrics.changed" }> =>
        event.event === "streamMetrics.changed" &&
        event.payload.request?.mode === "tail" &&
        event.payload.delivery?.tuningSource === "confirmed",
    );
    expect(
      firstOperationMeasurements.every(
        (event) =>
          event.payload.delivery?.batchSize === 10 &&
          event.payload.delivery.historySamples === 10 &&
          event.payload.delivery.intervalMs === 50 &&
          event.payload.queue?.capacityMessages === 100,
      ),
    ).toBe(true);
    expect(
      firstOperationMeasurements.every(
        (event) => (event.payload.delivery?.lastBatchMessages ?? 0) <= 10,
      ),
    ).toBe(true);

    await facade.execute(command("messages.stop", "stop-first"));
    await facade.execute(command("messages.start", "second-operation"));
    expect(events.filter((event) => event.event === "streamMetrics.changed").at(-1)).toMatchObject({
      payload: {
        delivery: {
          batchSize: 20,
          historySamples: 20,
          intervalMs: 100,
          tuningSource: "confirmed",
        },
        queue: { capacityMessages: 200 },
      },
    });
    await facade.execute(command("messages.stop", "stop-second"));
  });

  it("labels factory tuning as fallback when preference storage is unavailable", async () => {
    const stream = new ControlledMessageStream();
    const activeConnection = new RecordingActiveConnection();
    activeConnection.messageStreamOperations.push(() => Promise.resolve(stream));
    const port = new RecordingConnectionPort();
    port.openOperations.push(() => Promise.resolve(activeConnection));
    const preferenceService = new KafkaOperationalPreferenceService(
      new InMemoryKafkaOperationalPreferenceStore({
        durability: "session",
        recovery: "Session preference storage is unavailable.",
        state: "unavailable",
      }),
    );
    const facade = createFacade(
      port,
      (): void => undefined,
      undefined,
      undefined,
      preferenceService,
    );
    const events: HostEvent[] = [];
    facade.subscribe((event) => {
      events.push(event);
    });

    await facade.execute(command("connection.connect", "connect"));
    await facade.execute(command("messages.start", "fallback-operation"));

    expect(events.filter((event) => event.event === "streamMetrics.changed").at(-1)).toMatchObject({
      payload: {
        delivery: {
          batchSize: 200,
          historySamples: 50,
          intervalMs: 20,
          tuningSource: "factory-fallback",
        },
        queue: { capacityMessages: 1_000 },
      },
    });
    await facade.execute(command("messages.stop", "stop"));
    await settleAsyncIteration();
  });
});
