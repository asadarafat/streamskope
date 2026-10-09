import { randomUUID } from "node:crypto";

import { afterEach, describe, expect, it, vi, type Mock } from "vitest";

import {
  KAFKA_RECORD_PROTECTION_DEFAULTS,
  type KafkaMessage,
} from "../../src/features/kafka/contracts";
import type { KafkaReadCoverage } from "../../src/features/kafka/contracts/query-search";
import type {
  RecordExportInput,
  RecordExportLimits,
  RecordExportReceiptDetails,
  RecordExportSettings,
  RecordExportSnapshot,
} from "../../src/features/kafka/contracts/record-export";
import { parseRecordExportSnapshot } from "../../src/features/kafka/contracts/record-export-validation";
import { RecordExportService } from "../../src/features/kafka/application/record-export-service";
import type {
  RecordExportArtifacts,
  RecordExportSink,
} from "../../src/features/kafka/application/record-export-artifacts";
import type { KafkaReadCheckpoint } from "../../src/features/kafka/application/read-checkpoint";
import type { KafkaMessageStream } from "../../src/features/kafka/application/types";
import type { RecordReadScope } from "../../src/features/kafka/application/connection-scope";

function deferred<T>(): {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: Error): void;
} {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
const input = (): RecordExportInput => ({
  requestId: randomUUID(),
  topic: "events",
  range: { mode: "earliest" },
  format: "jsonl",
  maxRecords: 100,
  search: { key: "", value: "", offset: "", timestamp: "", partition: null },
});
function record(offset: number): KafkaMessage {
  const text = `record-${offset}`;
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
    recordByteSize: text.length,
    truncated: false,
    original: {
      state: "complete",
      encoding: "base64",
      key: null,
      value: Buffer.from(text).toString("base64"),
      headers: [],
    },
    structured: {
      version: 1,
      key: { state: "null", codec: "auto", writerSchema: null },
      value: { state: "decoded", codec: "utf8", text, json: null, writerSchema: null },
      headers: [],
      headersState: "complete",
      protection: "none",
    },
  };
}
class Reader implements KafkaMessageStream {
  next: number;
  accepted: number;
  scanned = 0;
  reason: KafkaReadCoverage["reason"] = "reading";
  readonly acknowledge = vi.fn((message: KafkaMessage): void => {
    this.accepted = Number(message.offset) + 1;
  });
  readonly close = vi.fn((): Promise<void> => {
    if (this.reason === "reading") this.reason = "cancelled";
    return Promise.resolve();
  });
  readonly yielded: number[] = [];
  constructor(
    readonly start: number,
    readonly end: number,
    readonly passSize: number,
    readonly missingCheckpoint = false,
  ) {
    this.next = start;
    this.accepted = start;
  }
  coverage(): KafkaReadCoverage {
    return {
      reason: this.reason,
      scannedRecords: this.scanned,
      scannedBytes: this.scanned * 10,
      matchedRecords: this.scanned,
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
  checkpoint(): KafkaReadCheckpoint | undefined {
    if (this.missingCheckpoint) return undefined;
    const coverage = this.coverage();
    return {
      clusterId: "cluster",
      topicId: "topic",
      partitionCount: 1,
      coverage: {
        ...coverage,
        reason:
          this.accepted === this.end
            ? "range-complete"
            : coverage.reason === "range-complete"
              ? "cancelled"
              : coverage.reason,
        partitions: [{ ...coverage.partitions[0]!, nextOffset: String(this.accepted) }],
      },
    };
  }
  async *[Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
    if (this.start === this.end) this.reason = "range-complete";
    while (this.reason === "reading" && this.next < this.end && this.scanned < this.passSize) {
      const offset = this.next++;
      this.scanned++;
      this.yielded.push(offset);
      if (this.next === this.end) this.reason = "range-complete";
      else if (this.scanned === this.passSize) this.reason = "result-limit";
      await Promise.resolve();
      yield record(offset);
    }
  }
}
type MockFunctions<T> = {
  [K in keyof T]: T[K] extends (...args: infer A) => infer R ? Mock<(...args: A) => R> : T[K];
};
const services: RecordExportService[] = [];
function fixture(
  options: {
    total?: number;
    passSize?: number;
    limits?: Partial<RecordExportLimits>;
    missingCheckpoint?: boolean;
  } = {},
): {
  service: RecordExportService;
  sink: MockFunctions<RecordExportSink>;
  artifacts: MockFunctions<RecordExportArtifacts>;
  scope: MockFunctions<RecordReadScope>;
  rows: Uint8Array[];
  readers: Reader[];
  checkpoints: (KafkaReadCheckpoint | undefined)[];
  snapshots: RecordExportSnapshot[];
  receipts: RecordExportReceiptDetails[];
  settings: RecordExportSettings;
  disconnect(): void;
} {
  const rows: Uint8Array[] = [];
  const readers: Reader[] = [];
  const snapshots: RecordExportSnapshot[] = [];
  const checkpoints: (KafkaReadCheckpoint | undefined)[] = [];
  const receipts: RecordExportReceiptDetails[] = [];
  let current = true;
  const sink: MockFunctions<RecordExportSink> = {
    write: vi.fn<RecordExportSink["write"]>((row): Promise<void> => {
      rows.push(row);
      return Promise.resolve();
    }),
    seal: vi.fn<RecordExportSink["seal"]>((receipt) => {
      receipts.push(receipt);
      return Promise.resolve({
        artifactId: randomUUID(),
        output: {
          format: receipt.input.format,
          fileName: `records.${receipt.input.format}`,
          bytes: rows.reduce((sum, row) => sum + row.byteLength, 0),
          sha256: "a".repeat(64),
        },
        receiptBytes: 200,
        receiptSha256: "b".repeat(64),
        expiresAt: new Date(Date.now() + 60_000).toISOString(),
      });
    }),
    discard: vi.fn<RecordExportSink["discard"]>(() => Promise.resolve()),
  };
  const artifacts: MockFunctions<RecordExportArtifacts> = {
    create: vi.fn<RecordExportArtifacts["create"]>(() => Promise.resolve(sink)),
    revoke: vi.fn<RecordExportArtifacts["revoke"]>(),
    drain: vi.fn<RecordExportArtifacts["drain"]>(() => Promise.resolve()),
  };
  const scope: MockFunctions<RecordReadScope> = {
    connectionName: "fixture",
    isCurrent: vi.fn((): boolean => current),
    openMessageStream: vi.fn<RecordReadScope["openMessageStream"]>(
      (_request, _signal, checkpoint) => {
        checkpoints.push(checkpoint);
        const reader = new Reader(
          Number(checkpoint?.coverage.partitions[0]?.nextOffset ?? "0"),
          Number(checkpoint?.coverage.partitions[0]?.endOffset ?? options.total ?? 5),
          options.passSize ?? 2,
          options.missingCheckpoint,
        );
        readers.push(reader);
        return Promise.resolve(reader);
      },
    ),
  };
  const settings: RecordExportSettings = {
    codecs: { key: "auto", value: "auto" },
    protection: { ...KAFKA_RECORD_PROTECTION_DEFAULTS },
  };
  const service = new RecordExportService({
    scope: (): RecordReadScope => scope,
    settings: (): RecordExportSettings => settings,
    artifacts,
    changed: (snapshot): void => {
      snapshots.push(parseRecordExportSnapshot(snapshot));
    },
    ...(options.limits === undefined ? {} : { limits: options.limits }),
  });
  services.push(service);
  return {
    service,
    sink,
    artifacts,
    scope,
    rows,
    readers,
    checkpoints,
    snapshots,
    receipts,
    settings,
    disconnect: (): void => {
      current = false;
    },
  };
}
afterEach(async () => {
  for (const service of services.splice(0)) {
    service.invalidate();
    await service.idle().catch(() => undefined);
  }
  vi.useRealTimers();
});

describe("finite streaming export ownership", () => {
  it("delivers refreshed active counts with revisions newer than progress and older than completion", async () => {
    const f = fixture({ total: 1 });
    const paused = deferred<void>();
    const reader = new Reader(0, 1, 1);
    vi.spyOn(reader, Symbol.asyncIterator).mockImplementation(async function* () {
      reader.next = 1;
      reader.scanned = 1;
      yield record(0);
      await paused.promise;
      reader.reason = "range-complete";
    });
    f.scope.openMessageStream.mockResolvedValueOnce(reader);
    f.service.start(input());
    try {
      await vi.waitFor(() => expect(reader.acknowledge).toHaveBeenCalledOnce());
      const event = f.snapshots.at(-1)!;
      expect(event.operation?.state).toBe("reading");
      const status = f.service.snapshot();
      expect(status.revision).toBeGreaterThan(event.revision);
      expect(status.operation).toMatchObject({
        state: "reading",
        counts: { writtenRecords: 1, scannedRecords: 1 },
      });
      expect(parseRecordExportSnapshot(status)).toEqual(status);
      const repeated = f.service.snapshot();
      expect(repeated.revision).toBeGreaterThan(status.revision);
      paused.resolve();
      await f.service.idle();
      expect(f.snapshots.at(-1)!.revision).toBeGreaterThan(repeated.revision);
      expect(f.snapshots.at(-1)!.operation?.state).toBe("completed");
    } finally {
      paused.resolve();
    }
  });

  it("preserves cleanup failure reported by an opening reader even though no handle was returned", async () => {
    const f = fixture();
    f.scope.openMessageStream.mockRejectedValue(
      Object.assign(new Error("cancelled"), { cleanupCause: new Error("late close failed") }),
    );
    const started = f.service.start(input());
    await expect(f.service.idle()).rejects.toThrow("cleanup");
    expect(f.service.snapshot().operation).toMatchObject({
      state: "failed",
      reason: "cleanup-failed",
      artifact: null,
    });
    expect(f.service.snapshot().operation?.error?.recovery).toContain("restart the host");
    await expect(f.service.discard(started.operation!.jobId)).rejects.toThrow("cleanup");
    expect(() => f.service.start(input())).toThrow("cleanup");
  });

  it("labels a fully traversed range partial when some predicates could not evaluate records", async () => {
    const f = fixture({ total: 0 });
    const reader = new Reader(0, 0, 1);
    reader.coverage = (): KafkaReadCoverage => ({
      reason: "range-complete",
      scannedRecords: 1,
      scannedBytes: 10,
      matchedRecords: 0,
      unavailableRecords: 1,
      partitions: [{ partition: 0, startOffset: "0", nextOffset: "1", endOffset: "1" }],
    });
    reader.checkpoint = (): undefined => undefined;
    f.scope.openMessageStream.mockResolvedValue(reader);
    f.service.start(input());
    await f.service.idle();
    expect(f.service.snapshot().operation).toMatchObject({
      state: "partial",
      reason: "records-unavailable",
      counts: { writtenRecords: 0, unavailableRecords: 1 },
      coverage: { reason: "range-complete" },
    });
    expect(f.receipts[0]).toMatchObject({ outcome: "partial", reason: "records-unavailable" });
  });

  it("keeps canonical input and settings immutable while returning detached public snapshots", async () => {
    const f = fixture({ total: 0 });
    const request = input();
    const result = f.service.start(request);
    Object.assign(request.search, { value: "later" });
    Object.assign(f.settings.codecs, { value: "bytes" });
    Object.assign(result.operation!.input.search, { value: "tampered" });
    await f.service.idle();
    expect(f.receipts[0]?.input.search.value).toBe("");
    expect(f.receipts[0]?.settings.codecs.value).toBe("auto");
  });

  it("lets notification transport failure neither strand a reader nor invalidate its accepted rows", async () => {
    const reader = new Reader(0, 1, 1);
    const f = fixture();
    const service = new RecordExportService({
      scope: (): RecordReadScope => ({
        ...f.scope,
        openMessageStream: (): Promise<KafkaMessageStream> => Promise.resolve(reader),
      }),
      settings: (): RecordExportSettings => f.settings,
      artifacts: f.artifacts,
      changed: (): void => {
        throw new Error("Transport gone");
      },
    });
    services.push(service);
    service.start(input());
    await service.idle();
    expect(reader.close).toHaveBeenCalledOnce();
    expect(service.snapshot().operation?.state).toBe("completed");
    expect(reader.acknowledge).toHaveBeenCalledOnce();
  });

  it("ends at the aggregate deadline and waits for a late-opening reader before sealing the partial result", async () => {
    vi.useFakeTimers();
    const f = fixture({ limits: { durationMs: 100 } });
    const opened = deferred<KafkaMessageStream>();
    const opening = deferred<void>();
    vi.mocked(f.scope.openMessageStream).mockImplementation(async () => {
      opening.resolve();
      return opened.promise;
    });
    f.service.start(input());
    await opening.promise;
    await vi.advanceTimersByTimeAsync(100);
    expect(f.service.snapshot().operation?.state).toBe("stopping");
    expect(f.sink.seal).not.toHaveBeenCalled();
    const reader = new Reader(0, 2, 2);
    opened.resolve(reader);
    await f.service.idle();
    expect(reader.close).toHaveBeenCalledOnce();
    expect(reader.yielded).toEqual([]);
    expect(f.service.snapshot().operation).toMatchObject({
      state: "partial",
      reason: "deadline",
      counts: { writtenRecords: 0 },
    });
  });

  it("expires prepared delivery authority and joins its cleanup", async () => {
    vi.useFakeTimers();
    const f = fixture({ total: 0 });
    f.service.start(input());
    await f.service.idle();
    await vi.advanceTimersByTimeAsync(60_000);
    await f.service.idle();
    expect(f.service.snapshot().operation).toMatchObject({ state: "expired", artifact: null });
    expect(f.artifacts.revoke).toHaveBeenCalled();
    expect(f.sink.discard).toHaveBeenCalled();
  });

  it("reserves once, snapshots settings and resumes captured bounds after acknowledged complete rows", async () => {
    const f = fixture();
    const request = input();
    const admitted = f.service.start(request);
    expect(admitted.operation?.state).toBe("preparing");
    expect(f.service.start(structuredClone(request)).operation?.jobId).toBe(
      admitted.operation?.jobId,
    );
    expect(() => f.service.start({ ...request, maxRecords: 2 })).toThrow("identity");
    expect(() => f.service.start(input())).toThrow("already running");
    await f.service.idle();
    expect(f.service.snapshot().operation).toMatchObject({
      state: "completed",
      reason: "range-complete",
      source: { clusterId: "cluster", topicId: "topic" },
      counts: { passes: 3, writtenRecords: 5, scannedRecords: 5 },
      coverage: { partitions: [{ startOffset: "0", nextOffset: "5", endOffset: "5" }] },
    });
    expect(
      f.checkpoints.map((checkpoint) => checkpoint?.coverage.partitions[0]?.nextOffset),
    ).toEqual([undefined, "2", "4"]);
    expect(f.readers.map((reader) => reader.acknowledge.mock.calls.length)).toEqual([2, 2, 1]);
    expect(
      f.rows.map((row) => (JSON.parse(new TextDecoder().decode(row)) as { offset: string }).offset),
    ).toEqual(["0", "1", "2", "3", "4"]);
    expect(f.receipts[0]?.counts.writtenBytes).toBe(
      f.rows.reduce((sum, row) => sum + row.byteLength, 0),
    );
  });

  it("applies backpressure and lets a pending complete row settle and ACK before soft cancellation seals", async () => {
    const f = fixture();
    const blocked = deferred<void>();
    const writing = deferred<void>();
    vi.mocked(f.sink.write).mockImplementationOnce(async (row) => {
      writing.resolve();
      await blocked.promise;
      f.rows.push(row);
    });
    const admitted = f.service.start(input());
    await writing.promise;
    expect(f.readers[0]?.yielded).toEqual([0]);
    expect(f.readers[0]?.acknowledge).not.toHaveBeenCalled();
    let stopped = false;
    const cancel = f.service.cancel(admitted.operation!.jobId).then(() => {
      stopped = true;
    });
    await Promise.resolve();
    expect(stopped).toBe(false);
    expect(f.sink.seal).not.toHaveBeenCalled();
    blocked.resolve();
    await cancel;
    expect(f.readers[0]?.acknowledge).toHaveBeenCalledTimes(1);
    expect(f.service.snapshot().operation).toMatchObject({
      state: "partial",
      reason: "cancelled",
      counts: { writtenRecords: 1 },
      coverage: { partitions: [{ nextOffset: "1", endOffset: "5" }] },
    });
    expect(f.artifacts.revoke).not.toHaveBeenCalled();
  });

  it("owns and closes a reader that appears only after hard revocation, without sealing or acknowledging it", async () => {
    const f = fixture();
    const opened = deferred<KafkaMessageStream>();
    const opening = deferred<void>();
    vi.mocked(f.scope.openMessageStream).mockImplementation(async () => {
      opening.resolve();
      return opened.promise;
    });
    f.service.start(input());
    await opening.promise;
    f.service.invalidate();
    const reader = new Reader(0, 2, 2);
    opened.resolve(reader);
    await f.service.idle();
    expect(reader.close).toHaveBeenCalledOnce();
    expect(reader.acknowledge).not.toHaveBeenCalled();
    expect(f.sink.seal).not.toHaveBeenCalled();
    expect(f.sink.discard).toHaveBeenCalled();
    expect(f.service.snapshot().operation).toMatchObject({
      state: "failed",
      reason: "revoked",
      artifact: null,
    });
  });

  it("retains failed original-reader cleanup and blocks replacement until actual discard retry succeeds", async () => {
    const f = fixture({ total: 1 });
    const reader = new Reader(0, 1, 1);
    reader.close.mockRejectedValue(new Error("sensitive broker details"));
    vi.mocked(f.scope.openMessageStream).mockResolvedValue(reader);
    const start = f.service.start(input());
    await expect(f.service.idle()).rejects.toThrow("cleanup");
    expect(f.service.snapshot().operation).toMatchObject({
      state: "failed",
      reason: "cleanup-failed",
      artifact: null,
    });
    expect(JSON.stringify(f.service.snapshot())).not.toContain("sensitive broker");
    expect(() => f.service.start(input())).toThrow("cleanup");
    expect(f.sink.seal).not.toHaveBeenCalled();
    reader.close.mockResolvedValue();
    await f.service.discard(start.operation!.jobId);
    expect(f.service.snapshot().operation).toBeNull();
    await expect(f.service.idle()).resolves.toBeUndefined();
  });

  it.each(["write", "seal"] as const)(
    "publishes no artifact after a %s failure and does not leak storage errors",
    async (method) => {
      const f = fixture({ total: 1 });
      vi.mocked(f.sink[method]).mockRejectedValue(new Error("secret-private-path"));
      f.service.start(input());
      await f.service.idle();
      expect(f.service.snapshot().operation).toMatchObject({
        state: "failed",
        reason: "storage-failed",
        artifact: null,
      });
      expect(JSON.stringify(f.service.snapshot())).not.toContain("secret-private-path");
      expect(f.sink.discard).toHaveBeenCalled();
      if (method === "write") expect(f.readers[0]?.acknowledge).not.toHaveBeenCalled();
    },
  );

  it("bounds records, passes, and reserved scan work independently with honest partial output", async () => {
    const cases = [
      { options: {}, maxRecords: 3, reason: "record-limit", written: 3, passes: 2 },
      {
        options: { limits: { passes: 1 } },
        maxRecords: 100,
        reason: "pass-limit",
        written: 2,
        passes: 1,
      },
      {
        options: { limits: { scanRecords: 10_001 } },
        maxRecords: 100,
        reason: "scan-limit",
        written: 2,
        passes: 1,
      },
      {
        options: { limits: { scanBytes: 32 * 1_048_576 } },
        maxRecords: 100,
        reason: "scan-limit",
        written: 2,
        passes: 1,
      },
    ];
    for (const item of cases) {
      const f = fixture(item.options);
      f.service.start({ ...input(), maxRecords: item.maxRecords });
      await f.service.idle();
      expect(f.service.snapshot().operation).toMatchObject({
        state: "partial",
        reason: item.reason,
        counts: { writtenRecords: item.written, passes: item.passes },
      });
    }
  });

  it("never writes or ACKs a row that crosses the encoded-byte budget", async () => {
    const f = fixture({ limits: { bytes: 1 } });
    f.service.start(input());
    await f.service.idle();
    expect(f.rows).toHaveLength(0);
    expect(f.readers[0]?.acknowledge).not.toHaveBeenCalled();
    expect(f.service.snapshot().operation).toMatchObject({
      state: "partial",
      reason: "byte-limit",
      counts: { writtenRecords: 0, writtenBytes: 0 },
      coverage: { partitions: [{ nextOffset: "0" }] },
    });
  });

  it.each(["jsonl", "csv"] as const)(
    "seals zero matches as valid %s with separate coverage receipt",
    async (format) => {
      const f = fixture({ total: 0 });
      f.service.start({ ...input(), format });
      await f.service.idle();
      expect(f.service.snapshot().operation).toMatchObject({
        state: "completed",
        reason: "range-complete",
        counts: { writtenRecords: 0 },
      });
      expect(f.rows).toHaveLength(format === "csv" ? 1 : 0);
      expect(f.receipts).toHaveLength(1);
    },
  );

  it("stops without guessing continuation when Kafka cannot provide stable checkpoint identity", async () => {
    const f = fixture({ missingCheckpoint: true });
    f.service.start(input());
    await f.service.idle();
    expect(f.service.snapshot().operation).toMatchObject({
      state: "partial",
      reason: "checkpoint-unavailable",
      source: { clusterId: null, topicId: null },
    });
    expect(f.readers).toHaveLength(1);
  });

  it("revokes the previous ready artifact before opening the next export and rejects stale discard IDs", async () => {
    const f = fixture({ total: 0 });
    const previous = f.service.start(input());
    await f.service.idle();
    const started = f.service.start(input());
    expect(f.artifacts.revoke).toHaveBeenCalledOnce();
    expect(started.operation?.jobId).not.toBe(previous.operation?.jobId);
    await expect(f.service.discard(previous.operation!.jobId)).rejects.toThrow(
      "no longer available",
    );
    await f.service.idle();
    expect(f.sink.discard).toHaveBeenCalled();
  });

  it("retains storage cleanup debt across invalidate and clears it only after a successful retry", async () => {
    const f = fixture({ total: 0 });
    const started = f.service.start(input());
    await f.service.idle();
    vi.mocked(f.sink.discard).mockRejectedValue(new Error("disk failure"));
    f.service.invalidate();
    await expect(f.service.idle()).rejects.toThrow("cleanup");
    expect(f.service.snapshot().operation?.artifact).toBeNull();
    vi.mocked(f.sink.discard).mockResolvedValue();
    await f.service.discard(started.operation!.jobId);
    await expect(f.service.idle()).resolves.toBeUndefined();
  });

  it("has no exporter capability in facade-only hosts and requires a current connection", () => {
    const unavailable = new RecordExportService({
      scope: (): null => null,
      settings: (): RecordExportSettings => ({
        codecs: { key: "auto", value: "auto" },
        protection: KAFKA_RECORD_PROTECTION_DEFAULTS,
      }),
      changed: (): void => undefined,
    });
    expect(unavailable.snapshot().available).toBe(false);
    expect(() => unavailable.start(input())).toThrow("unavailable");
    const f = fixture();
    f.disconnect();
    expect(() => f.service.start(input())).toThrow("Connect to Kafka");
  });
});
