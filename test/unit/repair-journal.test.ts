import { afterEach, expect, it, vi } from "vitest";

import {
  RepairJournal,
  MemoryRepairJobStore,
} from "../../src/features/kafka/application/repair-journal";
import { RecordReplayService } from "../../src/features/kafka/application/record-replay-service";
import type { ReviewedWriteScope } from "../../src/features/kafka/application/connection-scope";
import {
  UNCHANGED_REPLAY_TRANSFORM,
  replayConfirmation,
  type RecordReplayReview,
} from "../../src/features/kafka/contracts/record-replay";
import { parseRepairJournalDocument } from "../../src/features/kafka/contracts/repair-jobs";
import type { KafkaWriteOutcome } from "../../src/features/kafka/contracts/reviewed-writes";
import { RepairRecoveryService } from "../../src/features/kafka/application/repair-recovery-service";
import { RepairReconciliationReader } from "../../src/features/kafka/application/repair-reconciliation-reader";
import { KAFKA_RECORD_PROTECTION_DEFAULTS } from "../../src/features/kafka/contracts";

const services: RecordReplayService[] = [];
afterEach(async () => {
  for (const s of services.splice(0)) await s.invalidate();
});
const ack: KafkaWriteOutcome = {
  state: "acknowledged",
  detail: "Accepted",
  receipt: { topic: "destination", partition: 0, offset: "19" },
  verification: "verified",
};
const input = {
  targetProfile: null,
  topic: "destination",
  partition: 0,
  ratePerSecond: 10,
  records: [0, 1, 2].map((index) => ({
    topic: "source",
    partition: 0,
    offset: String(index),
    timestampMs: null,
    original: {
      state: "complete" as const,
      encoding: "base64" as const,
      key: "",
      value: "c2VjcmV0",
      headers: [
        { key: "aA==", value: null },
        { key: "aA==", value: "" },
      ],
    },
  })),
  transform: UNCHANGED_REPLAY_TRANSFORM,
};
function fixture(store = new MemoryRepairJobStore()): {
  service: RecordReplayService;
  journal: RepairJournal;
  send: ReturnType<typeof vi.fn<() => Promise<KafkaWriteOutcome>>>;
  revoke(): void;
} {
  const journal = new RepairJournal(store);
  let current = true;
  const send = vi.fn((): Promise<KafkaWriteOutcome> => Promise.resolve(ack));
  const scope: ReviewedWriteScope = {
    connectionName: "Target",
    isCurrent: () => current,
    reviewWrite: () => Promise.resolve({ clusterId: "cluster", topicId: "topic", partitions: 1 }),
    tryDispatchWrite: () => (current ? { started: true, result: send() } : { started: false }),
  };
  const service = new RecordReplayService(() => scope, undefined, undefined, journal);
  services.push(service);
  return {
    service,
    journal,
    send,
    revoke: (): void => {
      current = false;
    },
  };
}
it("persists dispatch intent before each send and actual ordered receipts before the next", async () => {
  const store = new MemoryRepairJobStore(),
    f = fixture(store);
  const states: unknown[] = [];
  f.send.mockImplementation(async () => {
    states.push(await store.load());
    return ack;
  });
  const review = await f.service.review(input);
  expect((await store.load()).jobs).toEqual([]);
  const outcome = await f.service.apply(review.planId, replayConfirmation(review));
  expect(outcome).toMatchObject({
    total: 3,
    unsent: 0,
    journal: "confirmed",
    durability: "session",
    cleanup: "complete",
  });
  for (let i = 0; i < states.length; i++)
    expect(states[i]).toMatchObject({ jobs: [{ pendingIndex: i, outcomes: Array(i).fill(ack) }] });
  const list = await f.journal.list();
  expect(list[0]).toMatchObject({
    id: review.planId,
    status: "complete",
    unsent: 0,
    outcomes: [ack, ack, ack],
  });
  expect(JSON.stringify(list)).not.toContain("c2VjcmV0");
  await f.service.apply(review.planId, replayConfirmation(review));
  expect(f.send).toHaveBeenCalledTimes(3);
});
it("fails closed before dispatch when intent persistence fails", async () => {
  const store = new MemoryRepairJobStore(),
    f = fixture(store);
  const normal = store.commit.bind(store);
  const commit = vi.spyOn(store, "commit");
  commit.mockImplementation(async (document) => {
    if (document.jobs[0]?.pendingIndex !== null) throw new Error("disk unavailable before send");
    await normal(document);
  });
  const review = await f.service.review(input);
  expect(await f.service.apply(review.planId, replayConfirmation(review))).toMatchObject({
    unsent: 3,
    outcomes: [],
    stopReason: "journal-unavailable",
    journal: "unavailable",
  });
  expect(f.send).not.toHaveBeenCalled();
});
it("preserves an admitted acknowledgement if receipt persistence fails, stops and recovers uncertainty", async () => {
  const store = new MemoryRepairJobStore(),
    f = fixture(store),
    normal = store.commit.bind(store);
  vi.spyOn(store, "commit").mockImplementation(async (d) => {
    if (d.jobs[0]?.outcomes.length) throw new Error("crash after Kafka acknowledgement");
    await normal(d);
  });
  const review = await f.service.review(input);
  const outcome = await f.service.apply(review.planId, replayConfirmation(review));
  expect(outcome).toMatchObject({
    outcomes: [ack],
    unsent: 2,
    stopReason: "journal-unavailable",
    journal: "unavailable",
  });
  expect(f.send).toHaveBeenCalledTimes(1);
  const reopened = new RepairJournal(store);
  expect(await reopened.list()).toMatchObject([
    { outcomes: [], uncertainIndex: 0, unsent: 2, cleanup: "pending" },
  ]);
  expect(f.send).toHaveBeenCalledTimes(1);
});
it("rechecks revocation after durable intent and clears an unadmitted intent without sending", async () => {
  const store = new MemoryRepairJobStore(),
    f = fixture(store),
    normal = store.commit.bind(store);
  vi.spyOn(store, "commit").mockImplementation(async (d) => {
    await normal(d);
    if (d.jobs[0]?.pendingIndex !== null) f.revoke();
  });
  const review = await f.service.review(input);
  expect(await f.service.apply(review.planId, replayConfirmation(review))).toMatchObject({
    outcomes: [],
    unsent: 3,
    stopReason: "connection-changed",
    journal: "confirmed",
  });
  expect(f.send).not.toHaveBeenCalled();
  expect(await f.journal.list()).toMatchObject([
    { uncertainIndex: null, unsent: 3, status: "stopped" },
  ]);
});
it("keeps the original owner until an in-flight acknowledgement and receipt settle after cancellation", async () => {
  const f = fixture();
  let resolve!: (o: KafkaWriteOutcome) => void;
  f.send.mockImplementation(
    () =>
      new Promise<KafkaWriteOutcome>((r) => {
        resolve = r;
      }),
  );
  const review = await f.service.review(input),
    operation = f.service.apply(review.planId, replayConfirmation(review));
  await vi.waitFor(() => expect(f.send).toHaveBeenCalledTimes(1));
  const cancel = f.service.cancel(review.planId);
  resolve(ack);
  expect(await operation).toMatchObject({ outcomes: [ack], unsent: 2, stopReason: "cancelled" });
  await cancel;
  expect(await f.journal.list()).toMatchObject([
    { outcomes: [ack], uncertainIndex: null, cleanup: "complete" },
  ]);
});
it("rejects inconsistent, duplicate and unknown-version stored jobs before admitting work", async () => {
  const f = fixture(),
    review: RecordReplayReview = await f.service.review(input);
  await f.journal.begin(review);
  const valid = await f.journal.store.load(),
    job = valid.jobs[0]!;
  expect(() => parseRepairJournalDocument({ ...valid, schemaVersion: 3 })).toThrow();
  expect(() => parseRepairJournalDocument({ ...valid, jobs: [job, job] })).toThrow();
  expect(() =>
    parseRepairJournalDocument({ ...valid, jobs: [{ ...job, pendingIndex: 2 }] }),
  ).toThrow();
  expect(() =>
    parseRepairJournalDocument({ ...valid, jobs: [{ ...job, status: "complete" }] }),
  ).toThrow();
});
it("links one fresh continuation and never resends the acknowledged or uncertain prefix", async () => {
  const store = new MemoryRepairJobStore(),
    first = fixture(store);
  const parent = await first.service.review(input);
  await first.journal.begin(parent);
  await first.journal.intent(parent.planId, 0);
  await first.journal.receipt(parent.planId, 0, ack);
  await first.journal.intent(parent.planId, 1); // crash before a durable receipt
  const reopened = fixture(store),
    origin = await reopened.journal.continuation(parent.planId);
  expect(origin.startIndex).toBe(2);
  const child = await reopened.service.review(
    { ...input, records: input.records.slice(2) },
    origin,
  );
  const duplicate = await reopened.service.review(
    { ...input, records: input.records.slice(2) },
    origin,
  );
  expect(await reopened.service.apply(child.planId, replayConfirmation(child))).toMatchObject({
    total: 1,
    outcomes: [ack],
    unsent: 0,
  });
  await expect(
    reopened.service.apply(duplicate.planId, replayConfirmation(duplicate)),
  ).rejects.toThrow("changed");
  expect(reopened.send).toHaveBeenCalledTimes(1);
  const jobs = (await store.load()).jobs;
  expect(jobs[0]).toMatchObject({
    id: parent.planId,
    outcomes: [ack],
    pendingIndex: 1,
    continuationId: child.planId,
  });
  expect(jobs[1]).toMatchObject({
    id: child.planId,
    parentJobId: parent.planId,
    outcomes: [ack],
    status: "complete",
  });
  expect((await reopened.journal.list()).every((j) => !j.canArchive)).toBe(true);
});
it("retains a committed child reservation after an uncertain begin and permits only its unsent suffix after reopening", async () => {
  const store = new MemoryRepairJobStore(),
    first = fixture(store);
  const parent = await first.service.review(input);
  await first.journal.begin(parent);
  const recovered = fixture(store),
    origin = await recovered.journal.continuation(parent.planId);
  const child = await recovered.service.review(input, origin),
    normal = store.commit.bind(store);
  vi.spyOn(store, "commit").mockImplementation(async (d) => {
    await normal(d);
    throw new Error("crash after reserving child before send");
  });
  await expect(recovered.service.apply(child.planId, replayConfirmation(child))).rejects.toThrow(
    "uncertain",
  );
  expect(recovered.send).not.toHaveBeenCalled();
  const reopened = new RepairJournal(store);
  await expect(reopened.continuation(parent.planId)).rejects.toThrow("no available");
  expect((await reopened.continuation(child.planId)).startIndex).toBe(0);
  expect(await reopened.list()).toMatchObject([
    { continuationId: child.planId, canContinue: false },
    { parentJobId: parent.planId, canContinue: true, unsent: 3 },
  ]);
});
it("rejects stale history and changed output bytes before reserving a continuation", async () => {
  const store = new MemoryRepairJobStore(),
    first = fixture(store),
    parent = await first.service.review(input);
  await first.journal.begin(parent);
  const recovered = fixture(store),
    origin = await recovered.journal.continuation(parent.planId);
  const child = await recovered.service.review(input, origin);
  await recovered.journal.recordFinding(origin.parent, {
    id: "observation",
    recordIndex: 0,
    offset: "19",
    observedAt: "2026-10-10T12:00:00Z",
    state: "equivalent",
    cleanup: "complete",
  });
  await expect(recovered.service.apply(child.planId, replayConfirmation(child))).rejects.toThrow(
    "changed",
  );
  const fresh = await recovered.journal.continuation(parent.planId);
  await expect(
    recovered.journal.begin(
      {
        ...child,
        batch: { ...child.batch, records: child.batch.records.map((r) => ({ ...r, value: "" })) },
      },
      fresh,
    ),
  ).rejects.toThrow("differs");
  expect((await store.load()).jobs).toHaveLength(1);
  expect(recovered.send).not.toHaveBeenCalled();
});
it("archives the exact inactive known chain, rejects stale revisions and preserves uncertain chains", async () => {
  const store = new MemoryRepairJobStore(),
    first = fixture(store),
    parent = await first.service.review(input);
  await first.journal.begin(parent);
  await first.journal.finish(parent.planId, false, "complete");
  const origin = await first.journal.continuation(parent.planId),
    child = await first.service.review(input, origin);
  await first.service.apply(child.planId, replayConfirmation(child));
  const jobs = await first.journal.list(),
    chain = jobs.map((j) => ({ id: j.id, revision: j.revision }));
  await expect(
    first.journal.archive({
      jobId: parent.planId,
      confirmation: parent.planId,
      chain: [{ ...chain[0]!, revision: 1 }, chain[1]!],
    }),
  ).rejects.toThrow("changed");
  expect((await store.load()).jobs).toHaveLength(2);
  await first.journal.archive({ jobId: parent.planId, confirmation: parent.planId, chain });
  expect((await store.load()).jobs).toEqual([]);
});
it("normalizes legacy jobs without guessing links and rejects string versions, dangling links and cycles", async () => {
  const f = fixture(),
    review = await f.service.review(input);
  await f.journal.begin(review);
  const modern = (await f.journal.store.load()).jobs[0]!;
  const { revision, parentJobId, continuationId, findings, ...legacy } = modern;
  expect([revision, parentJobId, continuationId, findings]).toEqual([1, null, null, []]);
  expect(parseRepairJournalDocument({ schemaVersion: 1, jobs: [legacy] }).jobs[0]).toEqual(modern);
  expect(() => parseRepairJournalDocument({ schemaVersion: "1", jobs: [legacy] })).toThrow();
  expect(() =>
    parseRepairJournalDocument({
      schemaVersion: 2,
      jobs: [{ ...modern, continuationId: "missing" }],
    }),
  ).toThrow();
  const other = {
    ...modern,
    id: "other",
    review: { ...modern.review, planId: "other" },
    parentJobId: modern.id,
    continuationId: modern.id,
  };
  expect(() =>
    parseRepairJournalDocument({
      schemaVersion: 2,
      jobs: [{ ...modern, parentJobId: other.id, continuationId: other.id }, other],
    }),
  ).toThrow();
});
it("fences a pending recovery lookup during revocation and joins it without admitting a late destination reader", async () => {
  const f = fixture(),
    reader = new RepairReconciliationReader(
      () => null,
      () => ({
        codecs: { key: "auto", value: "auto" },
        protection: KAFKA_RECORD_PROTECTION_DEFAULTS,
      }),
    );
  const review = await f.service.review(input);
  await f.journal.begin(review);
  const snapshot = await f.journal.snapshot(review.planId);
  let resolve!: (job: typeof snapshot) => void;
  const lookup = vi.spyOn(f.journal, "snapshot").mockImplementation(
    () =>
      new Promise((r) => {
        resolve = r;
      }),
  );
  const observe = vi.spyOn(reader, "observe");
  const recovery = new RepairRecoveryService(f.journal, f.service, reader);
  const work = recovery.reconcile({
    jobId: review.planId,
    targetProfile: null,
    recordIndex: 0,
    offset: "19",
  });
  const rejected = expect(work).rejects.toThrow("revoked");
  await vi.waitFor(() => expect(lookup).toHaveBeenCalledOnce());
  const stop = recovery.invalidate();
  expect(recovery.invalidate()).toBe(stop);
  await expect(
    recovery.archive({
      jobId: review.planId,
      confirmation: review.planId,
      chain: [{ id: review.planId, revision: 1 }],
    }),
  ).rejects.toThrow("cleanup");
  resolve(snapshot);
  await rejected;
  await stop;
  expect(observe).not.toHaveBeenCalled();
  expect((await f.journal.store.load()).jobs).toHaveLength(1);
});
