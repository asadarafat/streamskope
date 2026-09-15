import { describe, expect, it, vi } from "vitest";

import {
  HOST_PROTOCOL_VERSION,
  KAFKA_STREAM_MONITOR_HISTORY_LIMIT,
  type HostEvent,
} from "../../src/kafka/contracts";
import {
  RENDERER_STREAM_MONITOR_PENDING_EVENT_LIMIT,
  createRendererStreamMonitorObserver,
} from "../../src/kafka/ui/stream-monitor-observer";

function hostEvent(sequence: number): HostEvent {
  return {
    event: "backend.availability",
    payload: { state: "ready" },
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

function messageBatchEvent(sequence: number): HostEvent {
  return {
    event: "messages.batch",
    payload: {
      droppedMessages: 0,
      messages: [],
      topic: "orders.events",
    },
    sequence,
    version: HOST_PROTOCOL_VERSION,
  };
}

describe("renderer stream-monitor observer", () => {
  it("publishes one immutable aggregate snapshot at the post-commit boundary", () => {
    let monotonicTime = 10;
    let wallTime = Date.UTC(2026, 6, 26, 9, 0, 0);
    const observer = createRendererStreamMonitorObserver({
      monotonicNow: () => monotonicTime,
      wallNow: () => new Date(wallTime),
    });
    const listener = vi.fn();
    const unsubscribe = observer.subscribe(listener);

    observer.eventReceived(hostEvent(1));
    monotonicTime = 12;
    observer.eventReceived(messageBatchEvent(2));
    expect(observer.getSnapshot().eventBacklog).toBe(2);
    expect(listener).toHaveBeenCalledTimes(2);

    observer.recordFilterDuration(3);
    observer.recordRenderDuration(5);
    expect(listener).toHaveBeenCalledTimes(2);

    monotonicTime = 20;
    wallTime += 1_000;
    observer.commit({
      lastSequence: 2,
      rendererDroppedMessages: 1,
      retainedMessages: 100,
      visibleMessages: 40,
    });

    const committed = observer.getSnapshot();
    expect(committed).toMatchObject({
      eventBacklog: 0,
      eventToCommitMs: 8,
      filterDurationMs: 3,
      fps: null,
      rendererDroppedMessages: 1,
      renderDurationMs: 5,
      retainedMessages: 100,
      sampledAt: "2026-07-26T09:00:01.000Z",
      samplingState: "unavailable",
      visibleMessages: 40,
    });
    expect(committed.history).toEqual([
      {
        eventBacklog: 0,
        eventToCommitMs: 8,
        filterDurationMs: 3,
        fps: null,
        rendererDroppedMessages: 1,
        renderDurationMs: 5,
        retainedMessages: 100,
        sampledAt: "2026-07-26T09:00:01.000Z",
        samplingState: "unavailable",
        visibleMessages: 40,
      },
    ]);
    expect(listener).toHaveBeenCalledTimes(3);

    monotonicTime = 30;
    wallTime += 1_000;
    observer.commit({
      lastSequence: 2,
      rendererDroppedMessages: 1,
      retainedMessages: 101,
      visibleMessages: 41,
    });
    expect(committed.retainedMessages).toBe(100);
    expect(committed.history).toHaveLength(1);
    expect(observer.getSnapshot()).not.toBe(committed);
    unsubscribe();
    observer.dispose();
  });

  it("does not attribute navigation or lifecycle renders to message pipeline work", () => {
    let time = 0;
    const observer = createRendererStreamMonitorObserver({
      monotonicNow: () => time,
      wallNow: () => new Date("2026-07-26T09:30:00.000Z"),
    });

    observer.eventReceived(hostEvent(1));
    observer.recordFilterDuration(30);
    observer.recordRenderDuration(40);
    time = 5;
    observer.commit({
      lastSequence: 1,
      rendererDroppedMessages: 0,
      retainedMessages: 0,
      visibleMessages: 0,
    });

    expect(observer.getSnapshot()).toMatchObject({
      filterDurationMs: null,
      renderDurationMs: null,
    });
    observer.dispose();
  });

  it("retains only the newest bounded commit history", () => {
    let time = 0;
    const observer = createRendererStreamMonitorObserver({
      monotonicNow: () => time,
      wallNow: () => new Date(Date.UTC(2026, 6, 26, 10, 0, time)),
    });

    for (let sequence = 1; sequence <= KAFKA_STREAM_MONITOR_HISTORY_LIMIT + 1; sequence += 1) {
      observer.eventReceived(hostEvent(sequence));
      time = sequence;
      observer.commit({
        lastSequence: sequence,
        rendererDroppedMessages: 0,
        retainedMessages: sequence,
        visibleMessages: sequence,
      });
    }

    expect(observer.getSnapshot().history).toHaveLength(KAFKA_STREAM_MONITOR_HISTORY_LIMIT);
    expect(observer.getSnapshot().history[0]?.retainedMessages).toBe(2);
    expect(observer.getSnapshot().history.at(-1)?.retainedMessages).toBe(
      KAFKA_STREAM_MONITOR_HISTORY_LIMIT + 1,
    );
    observer.dispose();
  });

  it("bounds pending event evidence and ignores invalid timing samples", () => {
    let time = 0;
    const observer = createRendererStreamMonitorObserver({
      monotonicNow: () => time,
      wallNow: () => new Date("2026-07-26T11:00:00.000Z"),
    });
    for (
      let sequence = 1;
      sequence <= RENDERER_STREAM_MONITOR_PENDING_EVENT_LIMIT + 1;
      sequence += 1
    ) {
      time = sequence;
      observer.eventReceived(hostEvent(sequence));
    }

    expect(observer.getSnapshot().eventBacklog).toBe(RENDERER_STREAM_MONITOR_PENDING_EVENT_LIMIT);
    observer.recordFilterDuration(Number.NaN);
    observer.recordRenderDuration(-1);
    time += 10;
    observer.commit({
      lastSequence: RENDERER_STREAM_MONITOR_PENDING_EVENT_LIMIT + 1,
      rendererDroppedMessages: 0,
      retainedMessages: 0,
      visibleMessages: 0,
    });
    expect(observer.getSnapshot()).toMatchObject({
      eventBacklog: 0,
      filterDurationMs: null,
      renderDurationMs: null,
    });
    observer.dispose();
  });

  it("does not retain an event sequence the UI has already committed", () => {
    let time = 10;
    const observer = createRendererStreamMonitorObserver({
      monotonicNow: () => time,
      wallNow: () => new Date("2026-07-26T11:30:00.000Z"),
    });
    observer.commit({
      lastSequence: 5,
      rendererDroppedMessages: 0,
      retainedMessages: 0,
      visibleMessages: 0,
    });

    time = 20;
    observer.eventReceived(hostEvent(4));
    expect(observer.getSnapshot().eventBacklog).toBe(0);

    observer.eventReceived(hostEvent(6));
    expect(observer.getSnapshot().eventBacklog).toBe(1);
    time = 25;
    observer.commit({
      lastSequence: 6,
      rendererDroppedMessages: 0,
      retainedMessages: 0,
      visibleMessages: 0,
    });
    expect(observer.getSnapshot()).toMatchObject({
      eventBacklog: 0,
      eventToCommitMs: 5,
    });
    observer.dispose();
  });

  it("pauses inactive presentation work without replaying events on resume", () => {
    let time = 10;
    const observer = createRendererStreamMonitorObserver({
      monotonicNow: () => time,
      wallNow: () => new Date("2026-07-29T08:00:00.000Z"),
    });
    const listener = vi.fn();
    observer.subscribe(listener);

    observer.eventReceived(messageBatchEvent(1));
    time = 15;
    observer.recordFilterDuration(2);
    observer.recordRenderDuration(3);
    observer.commit({
      lastSequence: 1,
      rendererDroppedMessages: 0,
      retainedMessages: 1,
      visibleMessages: 1,
    });
    const activeHistory = observer.getSnapshot().history;
    listener.mockClear();

    observer.setPresentationActive(false);
    observer.eventReceived(messageBatchEvent(2));
    observer.recordFilterDuration(40);
    observer.recordRenderDuration(50);
    time = 30;
    observer.commit({
      lastSequence: 2,
      rendererDroppedMessages: 0,
      retainedMessages: 2,
      visibleMessages: 2,
    });

    expect(observer.getSnapshot()).toMatchObject({
      eventBacklog: 0,
      fps: null,
      samplingState: "unavailable",
    });
    expect(observer.getSnapshot().history).toEqual(activeHistory);
    expect(listener).not.toHaveBeenCalled();

    observer.setPresentationActive(true);
    observer.eventReceived(messageBatchEvent(3));
    time = 35;
    observer.commit({
      lastSequence: 3,
      rendererDroppedMessages: 0,
      retainedMessages: 3,
      visibleMessages: 3,
    });

    expect(observer.getSnapshot()).toMatchObject({
      eventBacklog: 0,
      retainedMessages: 3,
      visibleMessages: 3,
    });
    expect(observer.getSnapshot().history).toHaveLength(activeHistory.length + 1);
    expect(observer.getSnapshot().history.at(-1)?.eventToCommitMs).toBe(5);
    observer.dispose();
  });

  it("samples visible frames only while observed and releases every scheduler resource", () => {
    let monotonicTime = 0;
    let visible = true;
    let nextFrameId = 0;
    let visibilityListener: (() => void) | undefined;
    const frames = new Map<number, (timestamp: number) => void>();
    const cancelFrame = vi.fn((frameId: number) => {
      frames.delete(frameId);
    });
    const removeVisibilityListener = vi.fn(() => {
      visibilityListener = undefined;
    });
    const requestFrame = vi.fn((callback: (timestamp: number) => void): number => {
      const frameId = ++nextFrameId;
      frames.set(frameId, callback);
      return frameId;
    });
    const runFrame = (durationMs: number): void => {
      monotonicTime += durationMs;
      const frame = frames.entries().next().value as
        readonly [number, (timestamp: number) => void] | undefined;
      if (frame === undefined) {
        throw new Error("No frame was scheduled.");
      }
      frames.delete(frame[0]);
      frame[1](monotonicTime);
    };
    const observer = createRendererStreamMonitorObserver({
      cancelFrame,
      isDocumentVisible: () => visible,
      monotonicNow: () => monotonicTime,
      requestFrame,
      subscribeVisibility(listener) {
        visibilityListener = listener;
        return removeVisibilityListener;
      },
      wallNow: () => new Date(Date.UTC(2026, 6, 26, 12, 0, 0) + monotonicTime),
    });
    const firstListener = vi.fn();
    const secondListener = vi.fn();
    const unsubscribeFirst = observer.subscribe(firstListener);
    const unsubscribeSecond = observer.subscribe(secondListener);

    expect(observer.getSnapshot()).toMatchObject({
      fps: null,
      samplingState: "sampling",
    });
    expect(requestFrame).toHaveBeenCalledTimes(1);
    expect(frames).toHaveLength(1);

    for (let frame = 0; frame < 50; frame += 1) {
      runFrame(20);
    }
    expect(observer.getSnapshot()).toMatchObject({
      fps: 50,
      sampledAt: "2026-07-26T12:00:01.000Z",
      samplingState: "ready",
    });
    expect(observer.getSnapshot().history.at(-1)).toMatchObject({
      fps: 50,
      samplingState: "ready",
    });

    unsubscribeFirst();
    expect(frames).toHaveLength(1);
    visible = false;
    visibilityListener?.();
    expect(observer.getSnapshot()).toMatchObject({
      fps: null,
      samplingState: "hidden",
    });
    expect(frames).toHaveLength(0);
    expect(cancelFrame).toHaveBeenCalled();

    visible = true;
    visibilityListener?.();
    expect(observer.getSnapshot()).toMatchObject({
      fps: null,
      samplingState: "sampling",
    });
    expect(frames).toHaveLength(1);

    unsubscribeSecond();
    expect(observer.getSnapshot()).toMatchObject({
      fps: null,
      samplingState: "unavailable",
    });
    expect(frames).toHaveLength(0);

    const unsubscribeReopened = observer.subscribe(vi.fn());
    expect(observer.getSnapshot().samplingState).toBe("sampling");
    expect(frames).toHaveLength(1);
    observer.dispose();
    expect(frames).toHaveLength(0);
    expect(removeVisibilityListener).toHaveBeenCalledOnce();
    unsubscribeReopened();
  });
});
