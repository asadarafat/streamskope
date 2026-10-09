import { randomUUID } from "node:crypto";

import { afterEach, expect, it, vi } from "vitest";

import {
  KAFKA_RECORD_PROTECTION_DEFAULTS,
  type KafkaMessage,
  type KafkaReadCoverage,
} from "../../src/features/kafka/contracts";
import type {
  RecordAnalysisInput,
  RecordAnalysisLimits,
  RecordAnalysisSnapshot,
} from "../../src/features/kafka/contracts/record-analysis";
import type { RecordReadSettings } from "../../src/features/kafka/contracts/finite-record-read";
import { RecordAnalysisAccumulator } from "../../src/features/kafka/application/record-analysis-accumulator";
import { parseRecordAnalysisSnapshot } from "../../src/features/kafka/contracts/record-analysis-validation";
import { RecordAnalysisService } from "../../src/features/kafka/application/record-analysis-service";
import type { KafkaReadCheckpoint } from "../../src/features/kafka/application/read-checkpoint";
import type { RecordReadScope } from "../../src/features/kafka/application/connection-scope";
import type { KafkaMessageStream } from "../../src/features/kafka/application/types";

const input = (): RecordAnalysisInput => ({
  requestId: randomUUID(),
  topic: "events",
  range: { mode: "earliest" },
  search: { key: "", value: "", offset: "", timestamp: "", partition: null },
  maxRecords: 10_000,
  columns: [{ id: "group", label: "Group", source: "value", path: "$.group" }],
  groupBy: "group",
});
function record(offset: number, group: string, masked: boolean): KafkaMessage {
  const text = JSON.stringify({ group });
  return {
    id: String(offset),
    topic: "events",
    partition: 0,
    offset: String(offset),
    timestamp: "2026-10-09T10:00:00.000Z",
    key: null,
    payload: text,
    preview: text,
    headers: {},
    originalByteSize: text.length,
    truncated: false,
    structured: {
      version: 1,
      key: { state: "null", codec: "auto", writerSchema: null },
      value: { state: "decoded", codec: "json", text, json: text, writerSchema: null },
      headers: [],
      headersState: "complete",
      protection: masked ? "masked" : "none",
    },
  };
}
class Reader implements KafkaMessageStream {
  next: number;
  accepted: number;
  closed = false;
  constructor(
    readonly values: readonly string[],
    readonly start: number,
    readonly end: number,
    readonly passSize: number,
    readonly masked: boolean,
  ) {
    this.next = start;
    this.accepted = start;
  }
  readonly close = vi.fn((): Promise<void> => {
    this.closed = true;
    return Promise.resolve();
  });
  readonly acknowledge = vi.fn((message: KafkaMessage): void => {
    this.accepted = Number(message.offset) + 1;
  });
  coverage(): KafkaReadCoverage {
    return {
      reason:
        this.next === this.end ? "range-complete" : this.closed ? "cancelled" : "result-limit",
      scannedRecords: this.next - this.start,
      scannedBytes: this.next - this.start,
      matchedRecords: this.next - this.start,
      unavailableRecords: 0,
      partitions: [
        {
          partition: 0,
          startOffset: String(this.start),
          endOffset: String(this.end),
          nextOffset: String(this.next),
        },
      ],
    };
  }
  checkpoint(): KafkaReadCheckpoint {
    return {
      clusterId: "cluster",
      topicId: "topic",
      partitionCount: 1,
      coverage: {
        ...this.coverage(),
        reason:
          this.accepted === this.end
            ? "range-complete"
            : this.closed
              ? "cancelled"
              : "result-limit",
        partitions: [
          {
            partition: 0,
            startOffset: String(this.start),
            endOffset: String(this.end),
            nextOffset: String(this.accepted),
          },
        ],
      },
    };
  }
  async *[Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
    while (!this.closed && this.next < this.end && this.next - this.start < this.passSize) {
      const offset = this.next++;
      await Promise.resolve();
      yield record(offset, this.values[offset]!, this.masked);
    }
  }
}
const services: RecordAnalysisService[] = [];
const wireSnapshots: RecordAnalysisSnapshot[] = [];
function fixture(
  options: {
    values?: string[];
    passSize?: number;
    limits?: Partial<RecordAnalysisLimits>;
    masked?: boolean;
    failNotifications?: boolean;
  } = {},
): {
  service: RecordAnalysisService;
  open: ReturnType<typeof vi.fn<RecordReadScope["openMessageStream"]>>;
  readers: Reader[];
  events: RecordAnalysisSnapshot[];
  settings: {
    codecs: { key: "auto"; value: "auto" };
    protection: typeof KAFKA_RECORD_PROTECTION_DEFAULTS;
  };
  values: string[];
} {
  const readers: Reader[] = [];
  const values = options.values ?? ["a", "a", "b", "b", "c"];
  const events: RecordAnalysisSnapshot[] = [];
  const open = vi.fn<RecordReadScope["openMessageStream"]>((_request, _signal, checkpoint) => {
    const reader = new Reader(
      values,
      Number(checkpoint?.coverage.partitions[0]?.nextOffset ?? "0"),
      Number(checkpoint?.coverage.partitions[0]?.endOffset ?? values.length),
      options.passSize ?? 2,
      options.masked ?? false,
    );
    readers.push(reader);
    return Promise.resolve(reader);
  });
  const settings = {
    codecs: { key: "auto" as const, value: "auto" as const },
    protection: {
      ...KAFKA_RECORD_PROTECTION_DEFAULTS,
      valuePaths: options.masked ? ["/group"] : [],
    },
  };
  const scope: RecordReadScope = {
    connectionName: "fixture",
    isCurrent: (): boolean => true,
    openMessageStream: open,
  };
  const service = new RecordAnalysisService({
    scope: (): RecordReadScope => scope,
    settings: (): RecordReadSettings => settings,
    changed: (snapshot): void => {
      if (options.failNotifications) throw new Error("transport gone");
      events.push(snapshot);
      wireSnapshots.push(snapshot);
    },
    ...(options.limits === undefined ? {} : { limits: options.limits }),
  });
  services.push(service);
  return { service, open, readers, events, settings, values };
}
afterEach(async () => {
  for (const service of services.splice(0)) {
    const before = service.snapshot();
    service.invalidate();
    await service.idle().catch(() => undefined);
    wireSnapshots.push(before, service.snapshot());
  }
  for (const snapshot of wireSnapshots.splice(0))
    expect(() => parseRecordAnalysisSnapshot(snapshot), JSON.stringify(snapshot)).not.toThrow();
});

it("uses captured multi-pass bounds while keeping full counts after preview retention fills", async () => {
  const f = fixture({ limits: { previewRows: 2 } });
  f.service.start(input());
  await f.service.idle();
  const operation = f.service.snapshot().operation!;
  expect(operation).toMatchObject({
    state: "completed",
    reason: "range-complete",
    counts: { countedRecords: 5, scannedRecords: 5, passes: 3 },
    source: { clusterId: "cluster", topicId: "topic" },
    coverage: { partitions: [{ startOffset: "0", endOffset: "5", nextOffset: "5" }] },
    result: {
      previewOmittedRecords: 3,
      grouping: {
        groupedRecords: 5,
        groups: [
          { key: { state: "scalar", value: "a" }, count: 2 },
          { key: { state: "scalar", value: "b" }, count: 2 },
          { key: { state: "scalar", value: "c" }, count: 1 },
        ],
      },
    },
  });
  expect(operation.result?.preview).toHaveLength(2);
  expect(
    f.events
      .filter((event) => ["preparing", "reading", "stopping"].includes(event.operation!.state))
      .every((event) => event.operation!.result === null),
  ).toBe(true);
  expect(f.readers.every((reader) => reader.closed)).toBe(true);
});

it("reserves admission before notifications, deduplicates the current request and freezes settings", async () => {
  const f = fixture();
  const request = input();
  const admitted = f.service.start(request);
  expect(f.service.start(request).operation?.jobId).toBe(admitted.operation?.jobId);
  expect(() => f.service.start({ ...request, maxRecords: 1 })).toThrow("different options");
  expect(() => f.service.start(input())).toThrow("already running");
  Object.assign(request.columns[0]!, { path: "$.changed" });
  Object.assign(f.settings.protection, { valuePaths: ["/group"] });
  await f.service.idle();
  expect(f.service.snapshot().operation).toMatchObject({
    input: { columns: [{ path: "$.group" }] },
    settings: { protection: { valuePaths: [] } },
    result: { grouping: { excluded: { masked: 0 } } },
  });
});

it("refuses a new group atomically and reports the specific analysis limit", async () => {
  const f = fixture({ limits: { groups: 1 }, passSize: 10 });
  f.service.start(input());
  await f.service.idle();
  expect(f.service.snapshot().operation).toMatchObject({
    state: "partial",
    reason: "group-limit",
    counts: { countedRecords: 2, scannedRecords: 3 },
    coverage: { partitions: [{ nextOffset: "2" }] },
    result: { grouping: { groups: [{ count: 2 }], groupedRecords: 2 }, previewOmittedRecords: 0 },
  });
  expect(f.readers[0]!.acknowledge).toHaveBeenCalledTimes(2);
});

it("separates complete counts from grouping exclusions caused by protection", async () => {
  const f = fixture({ values: ["[MASKED]", "[MASKED]"], masked: true });
  f.service.start(input());
  await f.service.idle();
  expect(f.service.snapshot().operation).toMatchObject({
    state: "completed",
    reason: "range-complete",
    counts: { countedRecords: 2 },
    result: {
      grouping: { groups: [], groupedRecords: 0, excluded: { masked: 2, unavailable: 0 } },
    },
  });
});

it("soft cancellation remains responsive during pure counting and retains a consistent prefix", async () => {
  const f = fixture({ values: Array.from({ length: 1_000 }, () => "a"), passSize: 2_000 });
  const admitted = f.service.start({ ...input(), columns: [], groupBy: null });
  const cancel = setTimeout(() => {
    void f.service.cancel(admitted.operation!.jobId);
  }, 0);
  try {
    await f.service.idle();
    const operation = f.service.snapshot().operation!;
    expect(operation).toMatchObject({ state: "partial", reason: "cancelled" });
    expect(operation.counts.countedRecords).toBeGreaterThan(0);
    expect(operation.counts.countedRecords).toBeLessThanOrEqual(128);
    expect(operation.result!.preview.length + operation.result!.previewOmittedRecords).toBe(
      operation.counts.countedRecords,
    );
  } finally {
    clearTimeout(cancel);
  }
});

it("hard invalidation clears a completed result and denies stale discard authority", async () => {
  const f = fixture();
  const old = f.service.start(input()).operation!.jobId;
  await f.service.idle();
  f.service.invalidate();
  expect(f.service.snapshot().operation).toMatchObject({
    state: "revoked",
    result: null,
    reason: "revoked",
  });
  const next = f.service.start(input()).operation!.jobId;
  await expect(f.service.discard(old)).rejects.toThrow("no longer available");
  expect(f.service.snapshot().operation?.jobId).toBe(next);
});

it("retains cleanup debt, blocks replacement, and retries the original reader on discard", async () => {
  const f = fixture();
  const reader = new Reader(f.values, 0, f.values.length, 10, false);
  reader.close.mockRejectedValue(new Error("sensitive broker failure"));
  f.open.mockResolvedValue(reader);
  const jobId = f.service.start(input()).operation!.jobId;
  await expect(f.service.idle()).rejects.toThrow("cleanup could not be confirmed");
  expect(f.service.snapshot().operation).toMatchObject({
    state: "failed",
    reason: "cleanup-failed",
    result: null,
  });
  expect(JSON.stringify(f.service.snapshot())).not.toContain("sensitive broker");
  expect(() => f.service.start(input())).toThrow("cleanup is not confirmed");
  reader.close.mockResolvedValue();
  expect((await f.service.discard(jobId)).operation).toBeNull();
  expect(reader.close).toHaveBeenCalledTimes(2);
});

it("cannot erase cleanup debt when a late opening failed before returning its handle", async () => {
  const f = fixture();
  f.open.mockRejectedValue(
    Object.assign(new Error("cancelled"), { cleanupCause: new Error("late close failed") }),
  );
  const jobId = f.service.start(input()).operation!.jobId;
  await expect(f.service.idle()).rejects.toThrow("cleanup could not be confirmed");
  await expect(f.service.discard(jobId)).rejects.toThrow("cleanup could not be confirmed");
  expect(f.service.snapshot().operation?.error?.recovery).toContain("restart the host");
});

it("a failing notification transport cannot strand reads or lose the authoritative result", async () => {
  const f = fixture({ failNotifications: true });
  f.service.start(input());
  await f.service.idle();
  expect(f.service.snapshot().operation).toMatchObject({
    state: "completed",
    counts: { countedRecords: 5 },
  });
  expect(f.readers.every((reader) => reader.closed)).toBe(true);
});

it("never exposes a privately committed projection before its reader acknowledgement", async () => {
  const f = fixture();
  const beforeAck: RecordAnalysisSnapshot[] = [];
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound to the actual accumulator with call below.
  const original = RecordAnalysisAccumulator.prototype.accept;
  vi.spyOn(RecordAnalysisAccumulator.prototype, "accept").mockImplementation(function (
    this: RecordAnalysisAccumulator,
    message,
  ) {
    const result = original.call(this, message);
    queueMicrotask(() => beforeAck.push(f.service.snapshot()));
    return result;
  });
  f.service.start(input());
  await f.service.idle();
  expect(beforeAck).toHaveLength(5);
  expect(beforeAck.map((snapshot) => snapshot.operation!.counts.countedRecords)).toEqual([
    0, 1, 2, 3, 4,
  ]);
  expect(beforeAck.every((snapshot) => snapshot.operation!.result === null)).toBe(true);
  expect(
    beforeAck.every(
      (snapshot) =>
        snapshot.operation!.counts.scannedRecords >= snapshot.operation!.counts.countedRecords,
    ),
  ).toBe(true);
  for (const snapshot of beforeAck)
    expect(() => parseRecordAnalysisSnapshot(snapshot)).not.toThrow();
  expect(f.service.snapshot().operation!.result?.preview).toHaveLength(5);
});

it("delivers richer active status after progress events with a newer revision", async () => {
  const f = fixture({ values: ["a"] });
  let release!: () => void;
  const paused = new Promise<void>((resolve) => {
    release = resolve;
  });
  const reader = new Reader(f.values, 0, 1, 1, false);
  vi.spyOn(reader, Symbol.asyncIterator).mockImplementation(async function* () {
    reader.next = 1;
    yield record(0, "a", false);
    await paused;
  });
  f.open.mockResolvedValueOnce(reader);
  f.service.start(input());
  try {
    await vi.waitFor(() => expect(reader.acknowledge).toHaveBeenCalledOnce());
    const event = f.events.at(-1)!;
    expect(event.operation).toMatchObject({ state: "reading", result: null });
    const status = f.service.snapshot();
    expect(status.revision).toBeGreaterThan(event.revision);
    expect(status.operation).toMatchObject({
      state: "reading",
      counts: { countedRecords: 1, scannedRecords: 1 },
      result: { preview: [{ offset: "0", cells: [{ state: "scalar", value: "a" }] }] },
    });
    expect(parseRecordAnalysisSnapshot(status)).toEqual(status);
    const repeated = f.service.snapshot();
    expect(repeated.revision).toBeGreaterThan(status.revision);
    release();
    await f.service.idle();
    expect(f.events.at(-1)!.revision).toBeGreaterThan(repeated.revision);
    expect(f.events.at(-1)!.operation?.state).toBe("completed");
  } finally {
    release();
  }
});

it.each(["cancel", "revoke"] as const)(
  "owns a reader returned after %s and waits for its confirmed close",
  async (action) => {
    const f = fixture();
    let release!: (reader: KafkaMessageStream) => void;
    f.open.mockImplementationOnce(
      () =>
        new Promise<KafkaMessageStream>((resolve) => {
          release = resolve;
        }),
    );
    const jobId = f.service.start(input()).operation!.jobId;
    await vi.waitFor(() => expect(f.open).toHaveBeenCalledOnce());
    let settled = false;
    let stopping: Promise<unknown>;
    if (action === "cancel") stopping = f.service.cancel(jobId);
    else {
      f.service.invalidate();
      stopping = f.service.idle();
    }
    const joined = stopping.then(() => {
      settled = true;
    });
    await Promise.resolve();
    expect(settled).toBe(false);
    const late = new Reader(f.values, 0, f.values.length, 10, false);
    release(late);
    await joined;
    expect(late.close).toHaveBeenCalledOnce();
    expect(late.acknowledge).not.toHaveBeenCalled();
    expect(f.service.snapshot().operation).toMatchObject({
      state: action === "cancel" ? "partial" : "revoked",
      reason: action === "cancel" ? "cancelled" : "revoked",
      counts: { countedRecords: 0 },
    });
  },
);

it.each([
  { limits: { pathCharacters: 3 }, path: "$.group" },
  { limits: { pathSegments: 1 }, path: "$.outer.group" },
])(
  "rejects fields beyond effective host selector limits before reader admission ($path)",
  ({ limits, path }) => {
    const f = fixture({ limits });
    const request = input();
    expect(() =>
      f.service.start({ ...request, columns: [{ ...request.columns[0]!, path }] }),
    ).toThrow("field path exceeds");
    expect(f.open).not.toHaveBeenCalled();
    expect(f.service.snapshot().operation).toBeNull();
  },
);
