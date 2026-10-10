import { expect, it, vi, type Mock } from "vitest";

import {
  KAFKA_RECORD_PROTECTION_DEFAULTS,
  type KafkaMessage,
  type KafkaReadCoverage,
} from "../../src/features/kafka/contracts";
import type { KafkaMessageStream } from "../../src/features/kafka/application/types";
import { RepairReconciliationReader } from "../../src/features/kafka/application/repair-reconciliation-reader";
import type { RecordReadScope } from "../../src/features/kafka/application/connection-scope";
import {
  parseRepairJournalDocument,
  type RepairJob,
} from "../../src/features/kafka/contracts/repair-jobs";
import {
  replayBatch,
  UNCHANGED_REPLAY_TRANSFORM,
} from "../../src/features/kafka/contracts/record-replay";

const original = {
  state: "complete" as const,
  encoding: "base64" as const,
  key: "",
  value: null,
  headers: [
    { key: "aA==", value: null },
    { key: "aA==", value: "" },
  ],
};
const input = {
  targetProfile: null,
  topic: "events",
  partition: 0,
  ratePerSecond: 1,
  records: [{ topic: "source", partition: 0, offset: "0", timestampMs: null, original }],
  transform: UNCHANGED_REPLAY_TRANSFORM,
};
const job: RepairJob = parseRepairJournalDocument({
  schemaVersion: 1,
  jobs: [
    {
      id: "parent",
      createdAt: "2026-10-10T00:00:00Z",
      updatedAt: "2026-10-10T00:00:00Z",
      pendingIndex: 0,
      outcomes: [],
      status: "running",
      cleanup: "pending",
      review: {
        planId: "parent",
        sourceName: "Source",
        targetName: "Target",
        expiresAt: "2026-10-10T01:00:00Z",
        input,
        batch: replayBatch(input),
        destination: {
          clusterId: "cluster",
          topicId: "12345678-1234-1234-1234-123456789abc",
          partitions: 1,
        },
      },
    },
  ],
}).jobs[0]!;
const request = { jobId: job.id, targetProfile: null, recordIndex: 0, offset: "7" };
const message: KafkaMessage = {
  id: "events:0:7",
  topic: "events",
  partition: 0,
  offset: "7",
  timestamp: "2026-10-10T00:00:00Z",
  key: "",
  payload: "",
  preview: "",
  headers: {},
  originalByteSize: 8,
  original,
  truncated: false,
  provenance: {
    clusterId: job.review.destination.clusterId,
    topicId: job.review.destination.topicId,
    leaderEpoch: 3,
  },
};
interface Fixture {
  readonly service: RepairReconciliationReader;
  readonly close: Mock<() => Promise<void>>;
  readonly targetClose: Mock<() => Promise<void>>;
  readonly open: Mock<RecordReadScope["openMessageStream"]>;
  readonly reader: KafkaMessageStream;
  readonly settings: {
    readonly codecs: { readonly key: "auto"; readonly value: "auto" };
    readonly protection: typeof KAFKA_RECORD_PROTECTION_DEFAULTS;
  };
  readonly revoke: () => void;
}
function fixture(values: readonly KafkaMessage[] = [message]): Fixture {
  let complete = false,
    current = true;
  const close = vi.fn(() => Promise.resolve()),
    targetClose = vi.fn(() => Promise.resolve());
  const coverage = (): KafkaReadCoverage => ({
    reason: complete ? "range-complete" : "reading",
    scannedRecords: values.length,
    scannedBytes: 8 * values.length,
    matchedRecords: values.length,
    unavailableRecords: 0,
    partitions: [
      { partition: 0, startOffset: "7", endOffset: "8", nextOffset: complete ? "8" : "7" },
    ],
  });
  const reader: KafkaMessageStream = {
    close,
    coverage,
    checkpoint: () => ({
      clusterId: job.review.destination.clusterId,
      topicId: job.review.destination.topicId,
      partitionCount: 1,
      coverage: coverage(),
    }),
    async *[Symbol.asyncIterator]() {
      for (const value of values) {
        complete = true;
        yield await Promise.resolve(value);
      }
      complete = true;
    },
  };
  const open = vi.fn<RecordReadScope["openMessageStream"]>(() => Promise.resolve(reader));
  const target = {
    scope: { connectionName: "Target", isCurrent: (): boolean => current },
    readScope: {
      connectionName: "Target",
      isCurrent: (): boolean => current,
      openMessageStream: open,
    },
    close: targetClose,
  };
  const settings = {
    codecs: { key: "auto" as const, value: "auto" as const },
    protection: KAFKA_RECORD_PROTECTION_DEFAULTS,
  };
  const service = new RepairReconciliationReader(
    () => target,
    () => settings,
  );
  return {
    service,
    close,
    targetClose,
    open,
    reader,
    settings,
    revoke: (): void => {
      current = false;
    },
  };
}
it("observes exact tombstone/key/ordered duplicate-header bytes with actual provenance without inventing an acknowledgement", async () => {
  const f = fixture();
  expect(await f.service.observe(job, request)).toMatchObject({
    recordIndex: 0,
    offset: "7",
    state: "equivalent",
    cleanup: "complete",
  });
  expect(job).toMatchObject({ pendingIndex: 0, outcomes: [] });
  expect(f.open.mock.calls[0]?.[0]).toMatchObject({ search: { offsetExact: "7", partition: 0 } });
  expect(f.close).toHaveBeenCalledOnce();
  expect(f.targetClose).toHaveBeenCalledOnce();
});
it.each([
  ["different", { ...message, original: { ...original, key: null } }],
  [
    "different",
    { ...message, original: { ...original, headers: [...original.headers].reverse() } },
  ],
  [
    "unavailable",
    { ...message, original: { state: "unavailable" as const, reason: "masked" as const } },
  ],
  ["unavailable", { ...message, provenance: { ...message.provenance!, clusterId: "other" } }],
  [
    "unavailable",
    {
      ...message,
      provenance: { ...message.provenance!, topicId: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" },
    },
  ],
  ["unavailable", { ...message, offset: "8" }],
] as const)("records %s without exposing destination values", async (state, candidate) => {
  const f = fixture([candidate]),
    result = await f.service.observe(job, request);
  expect(result.state).toBe(state);
  expect(Object.keys(result).sort()).toEqual([
    "cleanup",
    "id",
    "observedAt",
    "offset",
    "recordIndex",
    "state",
  ]);
  expect(f.close).toHaveBeenCalledOnce();
});
it("reports absence only when actual source identity and the complete single-offset range agree", async () => {
  expect(await fixture([]).service.observe(job, request)).toMatchObject({ state: "not-observed" });
  const f = fixture([]);
  f.reader.checkpoint = (): ReturnType<NonNullable<KafkaMessageStream["checkpoint"]>> => ({
    clusterId: "other",
    topicId: job.review.destination.topicId,
    partitionCount: 1,
    coverage: f.reader.coverage!()!,
  });
  expect(await f.service.observe(job, request)).toMatchObject({ state: "unavailable" });
});
it("retains reader cleanup debt and does not close its destination or admit another observation until the original stop succeeds", async () => {
  const f = fixture();
  f.close.mockRejectedValueOnce(new Error("stop failed"));
  expect(await f.service.observe(job, request)).toMatchObject({ cleanup: "unavailable" });
  expect(f.targetClose).not.toHaveBeenCalled();
  expect(f.service.available()).toBe(false);
  expect(() => f.service.observe(job, request)).toThrow("still owns");
  await f.service.invalidate();
  expect(f.close).toHaveBeenCalledTimes(2);
  expect(f.targetClose).toHaveBeenCalledOnce();
  expect(f.service.available()).toBe(true);
});
it("joins a late-opened reader during revocation before closing the original destination", async () => {
  const f = fixture();
  let resolve!: (reader: KafkaMessageStream) => void;
  f.open.mockImplementation(
    () =>
      new Promise((r) => {
        resolve = r;
      }),
  );
  const task = f.service.observe(job, request);
  await vi.waitFor(() => expect(f.open).toHaveBeenCalledOnce());
  const stop = f.service.invalidate();
  let drained = false;
  void stop.then(() => {
    drained = true;
  });
  await Promise.resolve();
  expect(drained).toBe(false);
  resolve(f.reader);
  await stop;
  expect(await task).toMatchObject({ state: "unavailable", cleanup: "complete" });
  expect(f.close).toHaveBeenCalled();
  expect(f.targetClose).toHaveBeenCalled();
});
