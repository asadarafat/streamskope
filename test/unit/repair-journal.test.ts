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
  expect(() => parseRepairJournalDocument({ ...valid, schemaVersion: 2 })).toThrow();
  expect(() => parseRepairJournalDocument({ ...valid, jobs: [job, job] })).toThrow();
  expect(() =>
    parseRepairJournalDocument({ ...valid, jobs: [{ ...job, pendingIndex: 2 }] }),
  ).toThrow();
  expect(() =>
    parseRepairJournalDocument({ ...valid, jobs: [{ ...job, status: "complete" }] }),
  ).toThrow();
});
