import { afterEach, describe, expect, it, vi } from "vitest";

import { HOST_PROTOCOL_VERSION, type HostEvent } from "../../src/kafka/contracts";
import { ElectronEventDelivery } from "../../src/main/electron-event-delivery";
import { message } from "../support/kafka-backend-facade-fixture";

const event = (sequence: number): HostEvent => ({
  event: "backend.availability",
  payload: { state: "ready" },
  sequence,
  version: HOST_PROTOCOL_VERSION,
});

afterEach(() => vi.useRealTimers());

describe("bounded Electron event delivery", () => {
  it("applies record pressure independently of event count and releases it on close", () => {
    vi.useFakeTimers();
    const pressure = vi.fn();
    const failed = vi.fn();
    const delivery = new ElectronEventDelivery(() => undefined, failed, pressure);
    const batch: HostEvent = {
      event: "messages.batch",
      payload: {
        topic: "test",
        droppedMessages: 0,
        messages: Array.from({ length: 500 }, (_, index) => ({
          ...message(String(index)),
          ruleEvaluation: {
            state: "evaluated" as const,
            evaluatedRules: 0,
            activeMatchCount: 0,
            activeMatches: [],
            suppressedMatchCount: 0,
            suppressedMatches: [],
            durationMicros: 0,
            errorCount: 0,
            errors: [],
            omittedEvidence: 0,
            omittedRules: 0,
          },
        })),
      },
      sequence: 1,
      version: HOST_PROTOCOL_VERSION,
    };
    delivery.enqueue(batch);
    expect(pressure.mock.calls).toEqual([[true]]);
    delivery.enqueue({ ...batch, sequence: 2 });
    delivery.enqueue({ ...batch, sequence: 3 });
    expect(failed).toHaveBeenCalledWith("record-limit");
    expect(pressure.mock.calls).toEqual([[true], [false]]);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("pauses publication at high water and resumes only below low water", () => {
    vi.useFakeTimers();
    const pressure = vi.fn();
    const delivery = new ElectronEventDelivery(() => undefined, vi.fn(), pressure);
    for (let sequence = 1; sequence <= 32; sequence += 1) delivery.enqueue(event(sequence));
    expect(pressure.mock.calls).toEqual([[true]]);
    for (let sequence = 1; sequence < 16; sequence += 1) delivery.acknowledge(sequence);
    expect(pressure.mock.calls).toEqual([[true]]);
    delivery.acknowledge(16);
    expect(pressure.mock.calls).toEqual([[true], [false]]);
    delivery.close();
    expect(vi.getTimerCount()).toBe(0);
  });
  it("keeps one in-flight event and preserves order until the renderer acknowledges", () => {
    vi.useFakeTimers();
    const sent: number[] = [];
    const failed = vi.fn();
    const delivery = new ElectronEventDelivery((value) => sent.push(value.sequence), failed);
    delivery.enqueue(event(1));
    delivery.enqueue(event(2));
    delivery.enqueue(event(3));
    expect(sent).toEqual([1]);
    delivery.acknowledge(99);
    expect(sent).toEqual([1]);
    delivery.acknowledge(1);
    delivery.acknowledge(1);
    expect(sent).toEqual([1, 2]);
    delivery.acknowledge(2);
    delivery.acknowledge(3);
    expect(sent).toEqual([1, 2, 3]);
    expect(vi.getTimerCount()).toBe(0);
    expect(failed).not.toHaveBeenCalled();
    delivery.close();
  });

  it("fails explicitly once when the event capacity is exhausted", () => {
    vi.useFakeTimers();
    const sent = vi.fn();
    const failed = vi.fn();
    const delivery = new ElectronEventDelivery(sent, failed);
    for (let index = 1; index <= 100; index += 1) delivery.enqueue(event(index));
    expect(sent).toHaveBeenCalledTimes(1);
    expect(failed).toHaveBeenCalledTimes(1);
    expect(failed).toHaveBeenCalledWith("event-limit");
    delivery.acknowledge(1);
    expect(sent).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("bounds bytes including the in-flight event and releases them on close", () => {
    vi.useFakeTimers();
    const failed = vi.fn();
    const sent = vi.fn();
    const pressure = vi.fn();
    const delivery = new ElectronEventDelivery(sent, failed, pressure);
    const large: HostEvent = {
      ...event(1),
      event: "backend.availability",
      payload: { state: "unavailable", recovery: "x".repeat(5 * 1024 * 1024) },
    };
    delivery.enqueue(large);
    expect(pressure).toHaveBeenCalledWith(true);
    delivery.enqueue({ ...large, sequence: 2 });
    expect(failed).toHaveBeenCalledTimes(1);
    expect(failed).toHaveBeenCalledWith("byte-limit");
    expect(pressure).toHaveBeenLastCalledWith(false);
    delivery.close();
    delivery.enqueue(event(3));
    expect(sent).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("fails a stalled renderer instead of retaining pending events indefinitely", () => {
    vi.useFakeTimers();
    const failed = vi.fn();
    const delivery = new ElectronEventDelivery(() => undefined, failed);
    delivery.enqueue(event(1));
    vi.advanceTimersByTime(30000);
    expect(failed).toHaveBeenCalledTimes(1);
    expect(failed).toHaveBeenCalledWith("ack-timeout");
    expect(vi.getTimerCount()).toBe(0);
  });
});
