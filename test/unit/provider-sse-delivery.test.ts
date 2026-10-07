import { EventEmitter } from "node:events";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  ProviderSseDelivery,
  type ProviderSseCloseReport,
  type ProviderSseWritable,
} from "../../src/platform/node/provider-sse-delivery";
import { createProviderFixture } from "../support/provider-fixture";
import {
  HOST_PROTOCOL_VERSION,
  parseHostEvent,
  type HostEvent,
} from "../../src/features/kafka/contracts";
import { SseClientEventQueue } from "../../src/platform/node/kafka-sse-event-queue";

function batch(sequence: number, droppedMessages = 0): HostEvent {
  return {
    event: "messages.batch",
    sequence,
    version: HOST_PROTOCOL_VERSION,
    payload: {
      topic: "test",
      droppedMessages,
      messages: [
        {
          topic: "test",
          id: `record-${String(sequence)}`,
          offset: String(sequence),
          partition: 0,
          key: null,
          headers: {},
          payload: "value",
          preview: "value",
          originalByteSize: 5,
          truncated: false,
          timestamp: "2026-10-05T12:00:00Z",
          ruleEvaluation: {
            state: "evaluated",
            activeMatchCount: 0,
            activeMatches: [],
            suppressedMatchCount: 0,
            suppressedMatches: [],
            evaluatedRules: 0,
            omittedRules: 0,
            omittedEvidence: 0,
            durationMicros: 0,
            errorCount: 0,
            errors: [],
          },
        },
      ],
    },
  };
}

function eventAt(writable: ControlledWritable, index: number): HostEvent {
  const write = writable.writes[index];
  if (write === undefined || !write.chunk.startsWith("data: "))
    throw new Error("Expected a provider event write.");
  return parseHostEvent(JSON.parse(write.chunk.slice(6).trim()) as unknown);
}

class ControlledWritable extends EventEmitter implements ProviderSseWritable {
  destroyed = false;
  writableEnded = false;
  accept = true;
  synchronous = false;
  writeFailure: Error | undefined;
  endFailure: Error | undefined;
  endCalls = 0;
  readonly writes: { readonly chunk: string; readonly callback: (error?: Error | null) => void }[] =
    [];

  write(chunk: string, callback: (error?: Error | null) => void): boolean {
    if (this.writeFailure !== undefined) throw this.writeFailure;
    this.writes.push({ chunk, callback });
    if (this.synchronous) callback();
    return this.accept;
  }

  end(): void {
    this.endCalls += 1;
    this.writableEnded = true;
    if (this.endFailure !== undefined) throw this.endFailure;
  }

  complete(index: number, error?: Error): void {
    const write = this.writes[index];
    if (write === undefined) throw new Error("Expected an owned local write.");
    write.callback(error);
  }
}

function fixture(maxEvents = 4): {
  delivery: ProviderSseDelivery;
  writable: ControlledWritable;
  queue: ReturnType<ReturnType<typeof createProviderFixture>["endpoint"]["createEventQueue"]>;
  reports: ProviderSseCloseReport[];
  event: ReturnType<typeof createProviderFixture>["event"];
} {
  const provider = createProviderFixture({ id: "probe", version: 7 });
  const writable = new ControlledWritable();
  const queue = provider.endpoint.createEventQueue({
    maxEvents,
    maxMessageBytes: 1_024,
    maxMessages: 10,
    maxSerializedBytes: 2_048,
  });
  const reports: ProviderSseCloseReport[] = [];
  const delivery = new ProviderSseDelivery({
    response: writable,
    queue,
    maxEventBytes: 1_024,
    writeTimeoutMs: 1_000,
    onClose: (report): void => {
      reports.push(report);
    },
  });
  return { delivery, writable, queue, reports, event: provider.event };
}

afterEach(() => vi.useRealTimers());

describe("owned HTTP event delivery", () => {
  it("retains client-local drop evidence on later writable batches and terminal states, then resets on direct loading", () => {
    const create = (): { delivery: ProviderSseDelivery; writable: ControlledWritable } => {
      const writable = new ControlledWritable();
      const delivery = new ProviderSseDelivery({
        response: writable,
        queue: new SseClientEventQueue({ maxEvents: 8, maxMessageBytes: 8_192, maxMessages: 2 }),
        maxEventBytes: 8_192,
        writeTimeoutMs: 1_000,
        onClose: (): void => undefined,
      });
      return { delivery, writable };
    };
    const slow = create();
    const healthy = create();
    healthy.writable.synchronous = true;
    slow.delivery.start();
    healthy.delivery.start();
    slow.writable.complete(0);
    slow.writable.accept = false;
    const third = batch(3, 5);
    for (const event of [batch(1), batch(2), third]) {
      slow.delivery.enqueue(event);
      healthy.delivery.enqueue(event);
    }
    slow.writable.complete(1);
    slow.writable.emit("drain");
    expect(eventAt(slow.writable, 2)).toMatchObject({
      payload: { droppedMessages: 6, messages: [{ offset: "3" }] },
    });
    expect(eventAt(healthy.writable, 3)).toMatchObject({ payload: { droppedMessages: 5 } });
    expect(third).toMatchObject({ payload: { droppedMessages: 5 } });
    slow.writable.complete(2);
    slow.writable.emit("drain");
    slow.writable.accept = true;
    slow.delivery.enqueue(batch(4));
    expect(eventAt(slow.writable, 3)).toMatchObject({ payload: { droppedMessages: 1 } });
    const state: Extract<HostEvent, { event: "consumption.state" }> = {
      event: "consumption.state",
      sequence: 5,
      version: HOST_PROTOCOL_VERSION,
      payload: {
        state: "stopped",
        droppedMessages: 2,
        receivedMessages: 4,
        request: { topic: "test", mode: "tail", maxMessages: 1_000 },
        ruleEvaluation: { state: "ready", applicableRules: 0, omittedRules: 0 },
      },
    };
    slow.delivery.enqueue(state);
    slow.writable.complete(3);
    expect(eventAt(slow.writable, 4)).toMatchObject({ payload: { droppedMessages: 3 } });
    expect(state.payload.droppedMessages).toBe(2);
    slow.writable.complete(4);
    const loading: Extract<HostEvent, { event: "consumption.state" }> = {
      ...state,
      sequence: 6,
      payload: { ...state.payload, state: "loading", droppedMessages: 0, receivedMessages: 0 },
    };
    slow.delivery.enqueue(loading);
    expect(eventAt(slow.writable, 5)).toMatchObject({ payload: { droppedMessages: 0 } });
    slow.writable.complete(5);
    slow.writable.accept = false;
    slow.delivery.enqueue(batch(7));
    expect(eventAt(slow.writable, 6)).toMatchObject({ payload: { droppedMessages: 0 } });
    slow.delivery.close();
    healthy.delivery.close();
  });

  it("retains a successful local write in aggregate capacity until its callback completes", () => {
    const { delivery, writable, queue, reports, event } = fixture(2);
    delivery.start();
    writable.complete(0);
    delivery.enqueue(event("first", 1));
    delivery.enqueue(event("second", 2));
    expect(queue.costs.events).toBe(2);
    expect(queue.costs.serializedBytes).toBeGreaterThan(0);
    expect(writable.writes).toHaveLength(2);
    delivery.enqueue(event("third", 3));
    expect(reports).toHaveLength(1);
    expect(reports[0]?.deliveryFailure).toBe("event-limit");
    expect(queue.costs.events).toBe(0);
    writable.complete(1);
    delivery.close();
    expect(writable.writes).toHaveLength(2);
    expect(writable.endCalls).toBe(1);
    expect(reports).toHaveLength(1);
  });

  it.each(["callback-before-drain", "drain-before-callback"])(
    "requires both completion and capacity, and ignores a duplicate retired completion (%s)",
    (order) => {
      const { delivery, writable, queue, event } = fixture();
      delivery.start();
      writable.complete(0);
      writable.accept = false;
      delivery.enqueue(event("first", 1));
      delivery.enqueue(event("second", 2));
      if (order === "callback-before-drain") {
        writable.complete(1);
        expect(queue.costs.events).toBe(1);
        expect(writable.writes).toHaveLength(2);
        writable.emit("drain");
      } else {
        writable.emit("drain");
        expect(queue.costs.events).toBe(2);
        expect(writable.writes).toHaveLength(2);
        writable.complete(1);
      }
      expect(writable.writes).toHaveLength(3);
      expect(queue.costs.events).toBe(1);
      writable.complete(1);
      expect(queue.costs.events).toBe(1);
      writable.complete(2);
      expect(queue.costs.events).toBe(0);
      delivery.close();
      expect(writable.listenerCount("drain")).toBe(0);
      expect(writable.listenerCount("close")).toBe(0);
      expect(writable.listenerCount("error")).toBe(0);
    },
  );

  it("owns a stalled readiness comment and a stalled drain even with no queued provider event", () => {
    vi.useFakeTimers();
    const first = fixture();
    first.delivery.start();
    expect(vi.getTimerCount()).toBe(1);
    vi.advanceTimersByTime(1_000);
    expect(first.reports[0]?.deliveryFailure).toBe("write-timeout");
    expect(first.writable.endCalls).toBe(1);
    expect(vi.getTimerCount()).toBe(0);
    first.writable.complete(0);
    expect(first.writable.writes).toHaveLength(1);

    const second = fixture();
    second.writable.accept = false;
    second.delivery.start();
    second.writable.complete(0);
    expect(vi.getTimerCount()).toBe(1);
    vi.advanceTimersByTime(1_000);
    expect(second.reports[0]?.deliveryFailure).toBe("write-timeout");
    expect(vi.getTimerCount()).toBe(0);
  });

  it("handles synchronous local completion without creating another unowned write or timer", () => {
    vi.useFakeTimers();
    const { delivery, writable, queue, event } = fixture();
    writable.synchronous = true;
    delivery.start();
    delivery.enqueue(event("one", 1));
    delivery.enqueue(event("two", 2));
    expect(writable.writes).toHaveLength(3);
    expect(queue.costs.events).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
    delivery.close();
  });

  it.each(["throw", "callback"])(
    "contains a local write failure and still attempts independent cleanup (%s)",
    (mode) => {
      const { delivery, writable, queue, reports, event } = fixture();
      const writeFailure = new Error("private socket failure");
      const endFailure = new Error("private end failure");
      delivery.start();
      writable.complete(0);
      writable.endFailure = endFailure;
      if (mode === "throw") writable.writeFailure = writeFailure;
      expect(() => delivery.enqueue(event("failing", 1))).not.toThrow();
      if (mode === "callback") expect(() => writable.complete(1, writeFailure)).not.toThrow();
      expect(reports).toHaveLength(1);
      expect(reports[0]?.deliveryFailure).toBe(writeFailure);
      expect(reports[0]?.cleanupFailures).toEqual([endFailure]);
      expect(queue.costs.events).toBe(0);
      expect(writable.endCalls).toBe(1);
      expect(writable.listenerCount("close")).toBe(0);
    },
  );
});
