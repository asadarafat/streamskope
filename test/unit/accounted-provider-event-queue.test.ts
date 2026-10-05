import { describe, expect, it, vi } from "vitest";

import {
  AccountedProviderEventQueue,
  createControlDeliveryPolicy,
  type ProviderDeliveryLimits,
  type ProviderDeliveryPolicy,
  type ProviderDeliveryQueue,
  type ProviderDeliveryLease,
} from "../../src/platform/node/accounted-provider-event-queue";
import type { ProviderWireEvent } from "../../src/platform/providers/host";

interface ProbeEvent extends ProviderWireEvent {
  readonly kind: "records" | "control" | "loading";
  readonly records: readonly string[];
  readonly lost: number;
}

function event(sequence: number, records: readonly string[] = [], lost = 0): ProbeEvent {
  return {
    kind: records.length === 0 ? "control" : "records",
    lost,
    records,
    sequence,
    version: 7,
  };
}

function probePolicy(): ProviderDeliveryPolicy<ProbeEvent, string> {
  return {
    records: (value): readonly string[] | undefined =>
      value.kind === "records" ? value.records : undefined,
    retainedRecordBytes: (record): number => Buffer.byteLength(record),
    withRecords: (value, records): ProbeEvent => ({ ...value, records }),
    startsGeneration: (value): boolean => value.kind === "loading",
    decorateDrops: (value, dropped): ProbeEvent => {
      const lost = value.lost + dropped;
      if (!Number.isSafeInteger(lost)) throw new RangeError("Counter exhausted.");
      return { ...value, lost };
    },
    decorationReserveBytes: (value): number => 16 - String(value.lost).length,
    replacementKey: (): undefined => undefined,
  };
}

function queue(
  overflow: "reject" | "evict-oldest-pending-records" = "reject",
  limits: Partial<ProviderDeliveryLimits> = {},
  policy = probePolicy(),
): ProviderDeliveryQueue<ProbeEvent> {
  return new AccountedProviderEventQueue({
    limits: { maxEvents: 8, maxSerializedBytes: 16_384, maxRecords: 16, ...limits },
    overflow,
    policy,
  });
}

function lease(queue: ProviderDeliveryQueue<ProbeEvent>): ProviderDeliveryLease<ProbeEvent> {
  const next = queue.begin();
  if (next.kind !== "lease") throw new Error(`Expected a lease, received ${next.kind}.`);
  return next.lease;
}

function reserved(value: ProbeEvent): number {
  return Buffer.byteLength(JSON.stringify(value)) + 16 - String(value.lost).length;
}

describe("accounted provider event queue", () => {
  it("encodes an immutable rejecting event once without unused per-record prefix encodings", () => {
    const bounded = queue(
      "reject",
      {},
      {
        ...probePolicy(),
        decorateDrops: (value): ProbeEvent => value,
      },
    );
    const source = event(1, ["first", "second"]);
    const stringify = vi.spyOn(JSON, "stringify");
    let inputs: readonly unknown[];
    let writing: ProviderDeliveryLease<ProbeEvent> | undefined;
    try {
      expect(bounded.enqueue(source)).toBeUndefined();
      writing = lease(bounded);
      inputs = stringify.mock.calls.map(([value]): unknown => value);
    } finally {
      stringify.mockRestore();
    }
    expect(inputs).toHaveLength(1);
    expect(inputs[0]).toEqual(source);
    expect(writing?.event).toBe(inputs[0]);
    expect(bounded.costs.records).toBe(2);
    writing?.complete();
    expect(bounded.costs.records).toBe(0);
  });

  it("still validates a changed decoration against its admitted byte reservation", () => {
    const bounded = queue(
      "reject",
      {},
      {
        ...probePolicy(),
        decorateDrops: (value): ProbeEvent => {
          const decorated = { ...value, marker: "x".repeat(256) };
          return decorated;
        },
      },
    );
    expect(bounded.enqueue(event(1, ["unchanged"]))).toBeUndefined();
    const admitted = bounded.costs;
    expect(bounded.begin()).toEqual({ kind: "failure", reason: "event-validation" });
    expect(bounded.costs).toEqual(admitted);
    bounded.close();
    expect(bounded.costs.events).toBe(0);
  });

  it("keeps the active write in record, byte and event budgets until its own completion", () => {
    const first = event(1, ["aa", "bbb"]);
    const bounded = queue("reject", {
      maxEvents: 1,
      maxSerializedBytes: reserved(first),
      maxRecords: 2,
      maxRecordBytes: 5,
    });
    expect(bounded.enqueue(first)).toBeUndefined();
    const admitted = bounded.costs;
    const writing = lease(bounded);
    expect(bounded.costs).toEqual(admitted);
    expect(bounded.begin()).toEqual({ kind: "empty" });
    expect(bounded.enqueue(event(2))).toBe("event-limit");
    expect(bounded.costs).toEqual(admitted);
    writing.complete();
    expect(bounded.costs).toEqual({ events: 0, serializedBytes: 0, records: 0, recordBytes: 0 });
    expect(admitted.records).toBe(2);
    expect(bounded.enqueue(event(3))).toBeUndefined();
  });

  it.each([
    [{ maxRecords: 2 }, "record-limit"],
    [{ maxRecordBytes: 4 }, "record-byte-limit"],
    [{ maxSerializedBytes: reserved(event(1, ["aa", "bb"])) }, "byte-limit"],
  ] as const)("uses independent limits %j while a write owns its records", (limits, failure) => {
    const bounded = queue("reject", limits);
    expect(bounded.enqueue(event(1, ["aa", "bb"]))).toBeUndefined();
    const writing = lease(bounded);
    expect(bounded.enqueue(event(2, ["c"]))).toBe(failure);
    expect(bounded.costs.records).toBe(2);
    writing.complete();
    expect(bounded.enqueue(event(3, ["c"]))).toBeUndefined();
  });

  it("ignores duplicate and retired completions without releasing the next write", () => {
    const bounded = queue();
    bounded.enqueue(event(1));
    bounded.enqueue(event(2));
    const old = lease(bounded);
    old.complete();
    const current = lease(bounded);
    const cost = bounded.costs;
    old.complete();
    expect(bounded.costs).toEqual(cost);
    bounded.close();
    current.complete();
    expect(bounded.costs).toEqual({ events: 0, serializedBytes: 0, records: 0, recordBytes: 0 });
    expect(bounded.begin()).toEqual({ kind: "empty" });
    expect(bounded.enqueue(event(3))).toBe("event-validation");
  });

  it("rejects unsafe aggregate retained costs without corrupting an already owned write", () => {
    const bounded = queue(
      "reject",
      {},
      {
        ...probePolicy(),
        retainedRecordBytes: (): number => Number.MAX_SAFE_INTEGER,
      },
    );
    expect(bounded.enqueue(event(1, ["first"]))).toBeUndefined();
    const first = lease(bounded);
    const before = bounded.costs;
    expect(before.recordBytes).toBe(Number.MAX_SAFE_INTEGER);
    expect(bounded.enqueue(event(2, ["second"]))).toBe("event-validation");
    expect(bounded.costs).toEqual(before);
    first.complete();
    expect(bounded.costs.recordBytes).toBe(0);
    expect(bounded.enqueue(event(3, ["next"]))).toBeUndefined();
  });

  it("evicts only pending records while a written batch and controls remain owned", () => {
    const bounded = queue("evict-oldest-pending-records", { maxRecords: 1 });
    const original = event(1, ["written"]);
    bounded.enqueue(original);
    const writing = lease(bounded);
    expect(bounded.enqueue(event(2, ["omitted"]))).toBeUndefined();
    expect(bounded.enqueue(event(3))).toBeUndefined();
    expect(bounded.costs.records).toBe(1);
    writing.complete();
    const terminal = lease(bounded);
    expect(terminal.event).toMatchObject({ kind: "control", lost: 1 });
    expect(writing.event.lost).toBe(0);
    expect(original.lost).toBe(0);
    terminal.complete();

    const controlBounded = queue("evict-oldest-pending-records", { maxEvents: 2 });
    controlBounded.enqueue(event(4));
    const controlWrite = lease(controlBounded);
    controlBounded.enqueue(event(5));
    expect(controlBounded.enqueue(event(6, ["new"]))).toBe("event-limit");
    controlWrite.complete();
    expect(lease(controlBounded).event.sequence).toBe(5);
  });

  it("keeps old write costs and frozen evidence through loading and charges the old terminal", () => {
    const bounded = queue("evict-oldest-pending-records");
    bounded.enqueue(event(1, ["written"]));
    const old = lease(bounded);
    bounded.enqueue(event(2, ["unsent-a", "unsent-b"]));
    bounded.enqueue(event(3, [], 4));
    bounded.enqueue({ ...event(4), kind: "loading" });
    bounded.enqueue(event(5, ["new"]));
    expect(bounded.costs.records).toBe(2);
    expect(old.event.lost).toBe(0);
    old.complete();
    const terminal = lease(bounded);
    expect(terminal.event).toMatchObject({ sequence: 3, lost: 6 });
    terminal.complete();
    const loading = lease(bounded);
    expect(loading.event).toMatchObject({ sequence: 4, lost: 0 });
    loading.complete();
    const current = lease(bounded);
    const currentCost = bounded.costs;
    old.complete();
    expect(bounded.costs).toEqual(currentCost);
    expect(current.event).toMatchObject({ sequence: 5, lost: 0, records: ["new"] });
  });

  it("caches escaped UTF-8 record costs and replaces one retained suffix per touched batch", () => {
    const policy = probePolicy();
    const withRecords = vi.fn(policy.withRecords);
    const bounded = queue(
      "evict-oldest-pending-records",
      { maxRecords: 2 },
      { ...policy, withRecords },
    );
    const source = event(1, ['quote"', "å\n", "last"]);
    bounded.enqueue(source);
    const suffix = { ...source, records: ["å\n", "last"] };
    expect(bounded.costs.serializedBytes).toBe(reserved(suffix));
    expect(bounded.costs.recordBytes).toBe(Buffer.byteLength("å\nlast"));
    expect(withRecords.mock.calls.length).toBeLessThanOrEqual(2);
    const writing = lease(bounded);
    expect(writing.event).toMatchObject({ records: suffix.records, lost: 1 });
    expect(source.records).toEqual(['quote"', "å\n", "last"]);
  });

  it("reserves growing omission digits without changing accounted costs or source events", () => {
    const records = Array.from({ length: 10 }, () => "r");
    const terminal = event(2, [], 1);
    const newest = event(1, ["r"]);
    const limit = reserved(newest) + reserved(terminal);
    const bounded = queue("evict-oldest-pending-records", {
      maxRecords: 1,
      maxSerializedBytes: limit,
    });
    bounded.enqueue(event(1, records));
    expect(bounded.enqueue(terminal)).toBeUndefined();
    expect(bounded.costs.serializedBytes).toBe(limit);
    const recordsWrite = lease(bounded);
    expect(recordsWrite.event.lost).toBe(9);
    recordsWrite.complete();
    const remainingCost = bounded.costs.serializedBytes;
    const terminalWrite = lease(bounded);
    expect(terminalWrite.event.lost).toBe(10);
    expect(Buffer.byteLength(JSON.stringify(terminalWrite.event))).toBeLessThanOrEqual(
      remainingCost,
    );
    expect(bounded.costs.serializedBytes).toBe(remainingCost);
    expect(terminal.lost).toBe(1);
  });

  it("reports fallible acquisition for unsafe counters without acquiring or throwing", () => {
    const bounded = queue("evict-oldest-pending-records", { maxRecords: 1 });
    bounded.enqueue(event(1, ["old", "new"]));
    bounded.enqueue(event(2, [], Number.MAX_SAFE_INTEGER));
    lease(bounded).complete();
    const cost = bounded.costs;
    expect(bounded.begin()).toEqual({ kind: "failure", reason: "event-validation" });
    expect(bounded.costs).toEqual(cost);
    bounded.close();
    expect(bounded.begin()).toEqual({ kind: "empty" });
  });

  it("rejects unreserved marker fields and converts policy faults into queue failures", () => {
    const policy = createControlDeliveryPolicy<ProbeEvent>();
    const bounded = new AccountedProviderEventQueue({
      limits: { maxEvents: 1, maxSerializedBytes: 1_024, maxRecords: 1 },
      overflow: "reject",
      policy: {
        ...policy,
        decorateDrops: (value): ProbeEvent => ({ ...value, records: ["unexpected marker"] }),
      },
    });
    expect(bounded.enqueue(event(1))).toBeUndefined();
    expect(bounded.begin()).toEqual({ kind: "failure", reason: "event-validation" });
    const invalid = queue(
      "reject",
      {},
      {
        ...probePolicy(),
        retainedRecordBytes: (): number => {
          throw new Error("Provider detail must stay private.");
        },
      },
    );
    expect(invalid.enqueue(event(2, ["record"]))).toBe("event-validation");
    expect(invalid.costs.events).toBe(0);
  });

  it("rejects incorrect record projections even when their extra bytes fit the decoration reserve", () => {
    const policy = probePolicy();
    const replacement = queue(
      "evict-oldest-pending-records",
      { maxRecords: 1 },
      {
        ...policy,
        withRecords: (value): ProbeEvent => value,
      },
    );
    expect(replacement.enqueue(event(1, ["", ""]))).toBe("event-validation");
    expect(replacement.costs.records).toBe(0);

    const decoration = queue(
      "reject",
      { maxRecords: 1 },
      {
        ...policy,
        decorateDrops: (value): ProbeEvent => ({ ...value, records: ["", ""] }),
      },
    );
    expect(decoration.enqueue(event(2, [""]))).toBeUndefined();
    expect(decoration.begin()).toEqual({ kind: "failure", reason: "event-validation" });
    expect(decoration.costs.records).toBe(1);
    decoration.close();
    expect(decoration.costs.records).toBe(0);
  });
});
