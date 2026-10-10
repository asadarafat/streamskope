import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { expect, vi } from "vitest";

import type { KafkaApplicationSession } from "../../src/features/kafka/application";
import { RepairJournal } from "../../src/features/kafka/application/repair-journal";
import { RecordReplayService } from "../../src/features/kafka/application/record-replay-service";
import { RepairRecoveryService } from "../../src/features/kafka/application/repair-recovery-service";
import { RepairReconciliationReader } from "../../src/features/kafka/application/repair-reconciliation-reader";
import { StructuredReplayService } from "../../src/features/kafka/application/structured-replay-service";
import {
  KAFKA_RECORD_PROTECTION_DEFAULTS,
  type KafkaMessage,
} from "../../src/features/kafka/contracts";
import { inspectConnectDlqEvidence } from "../../src/features/kafka/contracts/connect-dlq";
import {
  UNCHANGED_REPLAY_TRANSFORM,
  replayConfirmation,
  type ReplayRecord,
} from "../../src/features/kafka/contracts/record-replay";
import type { KafkaCompleteRecord } from "../../src/features/kafka/contracts/record-bytes";
import { SchemaRegistryHttpAdapter } from "../../src/features/kafka/engine/schema-registry-http";
import { NodeBoundedJsonHttp } from "../../src/features/kafka/engine/bounded-json-http";
import { AtomicRepairFileStore } from "../../src/platform/node/kafka-repair-file-store";
import { createHostRecordCodec } from "../../src/platform/node/record-codec";
import {
  openPassphraseVault,
  type PassphraseVault,
} from "../../src/platform/node/vault/passphrase-vault";

/** A real converter failure feeds the ordinary structured replay and protected recovery owners. */
export async function qualifyConnectDlqRepair(input: {
  readonly session: KafkaApplicationSession;
  readonly connector: string;
  readonly workerUrl: string;
  connect(): Promise<void>;
}): Promise<void> {
  const { session } = input;
  const dataRoot = await mkdtemp(join(tmpdir(), "streamskope-connect-repair-")),
    file = join(dataRoot, "history", "kafka-repair-jobs.json"),
    passphrase = `disposable-fixture-${randomUUID()}`;
  let vault: PassphraseVault | undefined,
    replay: RecordReplayService | undefined,
    recovery: RepairRecoveryService | undefined;
  const failures: unknown[] = [];
  const b64 = (s: string): string => Buffer.from(s).toString("base64");
  const original = [1, 3].map((amount): KafkaCompleteRecord => ({
    state: "complete",
    encoding: "base64",
    key: b64("original-key"),
    value: b64(JSON.stringify({ amount })),
    headers: [
      { key: b64("trace"), value: b64("first") },
      { key: b64("trace"), value: null },
      { key: b64("trace"), value: b64("last") },
    ],
  }));
  const read = async (topic: string): Promise<KafkaMessage[]> => {
    const owner = session.writeContext();
    if (!owner) throw new Error("Fixture lost its original connected reader.");
    const stream = await owner.connection.openMessageStream(
      { topic, mode: "earliest", maxMessages: 10 },
      AbortSignal.timeout(10_000),
    );
    const messages: KafkaMessage[] = [];
    try {
      for await (const message of stream) messages.push(message);
    } finally {
      await stream.close();
    }
    return messages;
  };
  const sourceOffset = async (): Promise<unknown> => {
    const response = await fetch(`${input.workerUrl}/connectors/${input.connector}/offsets`);
    expect(response.ok).toBe(true);
    return response.json();
  };
  const knownTaskIds = async (): Promise<string[]> => {
    const response = await fetch(`${input.workerUrl}/connectors/${input.connector}/status`);
    expect(response.ok).toBe(true);
    const status: unknown = await response.json();
    if (
      status === null ||
      typeof status !== "object" ||
      !("tasks" in status) ||
      !Array.isArray(status.tasks)
    )
      throw new Error("The actual worker did not report connector tasks.");
    return status.tasks.map((task: unknown): string => {
      if (
        task === null ||
        typeof task !== "object" ||
        !("id" in task) ||
        typeof task.id !== "number" ||
        !Number.isSafeInteger(task.id) ||
        task.id < 0
      )
        throw new Error("The actual worker reported an invalid task identity.");
      return String(task.id);
    });
  };
  const codec = createHostRecordCodec();
  const encoding = new StructuredReplayService(
    () => session.schemaRegistryReviewScope(),
    codec,
    new SchemaRegistryHttpAdapter(new NodeBoundedJsonHttp()),
    codec,
  );
  try {
    for (const record of original) {
      expect(
        await session.writeContext()!.connection.applyWrite!({
          kind: "record",
          topic: "connect-input",
          partition: 0,
          record,
        }),
      ).toMatchObject({ state: "acknowledged" });
    }
    let deadLetters: KafkaMessage[] = [];
    await expect
      .poll(
        async () => {
          deadLetters = await read("connect-dlq");
          return deadLetters.length;
        },
        { timeout: 30_000 },
      )
      .toBe(2);
    const immutable = structuredClone(deadLetters);
    const taskIds = await knownTaskIds();
    expect(taskIds.length).toBeGreaterThan(0);
    deadLetters.forEach((record, index) => {
      const evidence = inspectConnectDlqEvidence(record);
      expect(evidence).toMatchObject({
        state: "reported",
        context: {
          topic: "connect-input",
          partition: "0",
          offset: String(index + 2),
          connector: input.connector,
          stage: "VALUE_CONVERTER",
        },
      });
      if (evidence.state !== "reported") throw new Error("No complete reported DLQ context.");
      // Multiple actual tasks can own partition zero after a rebalance; never infer task ID.
      expect(taskIds).toContain(evidence.context.task);
      expect(record.original).toMatchObject({
        key: original[index]!.key,
        value: original[index]!.value,
      });
      expect(record.original?.state === "complete" && record.original.headers.slice(0, 3)).toEqual(
        original[index]!.headers,
      );
      expect(record.structured?.value).toMatchObject({
        state: "decoded",
        codec: "json",
        json: JSON.stringify({ amount: index === 0 ? 1 : 3 }),
      });
    });
    await expect
      .poll(async () => JSON.stringify(await sourceOffset()), { timeout: 15_000 })
      .toContain('"kafka_offset":4');
    const beforeOffsets = await sourceOffset();
    vault = await openPassphraseVault({ dataRoot, passphrase, mode: "create" });
    const store = new AtomicRepairFileStore(file, vault.protector),
      journal = new RepairJournal(store);
    replay = new RecordReplayService(
      () => session.reviewedWriteScope(),
      undefined,
      undefined,
      journal,
      encoding,
    );
    const records = deadLetters.map((r): ReplayRecord => {
      if (r.original?.state !== "complete") throw new Error("No complete DLQ bytes.");
      return {
        topic: r.topic,
        partition: r.partition,
        offset: r.offset,
        timestampMs: null,
        original: r.original,
      };
    });
    const plan = await replay.review({
      targetProfile: null,
      topic: "connect-replayed",
      partition: 0,
      ratePerSecond: 1,
      records,
      transform: {
        ...UNCHANGED_REPLAY_TRANSFORM,
        structured: {
          key: null,
          value: {
            codec: "json",
            patches: [{ op: "set", path: "/repaired", json: "true" }],
            mappings: [{ format: "json", sourceId: null, target: null }],
          },
        },
      },
    });
    const expected = [1, 3].map((amount, index): KafkaCompleteRecord => ({
      ...records[index]!.original,
      value: b64(JSON.stringify({ amount, repaired: true })),
    }));
    expect(plan.batch.records).toEqual(expected);
    expect(plan.encoding).toEqual([
      { key: null, value: { source: { format: "json", id: null }, target: null } },
      { key: null, value: { source: { format: "json", id: null }, target: null } },
    ]);
    expect(await read("connect-replayed")).toEqual([]);
    const commit = store.commit.bind(store);
    const receiptLoss = vi
      .spyOn(store, "commit")
      .mockImplementation(async (document): Promise<void> => {
        if (document.jobs.find((j) => j.id === plan.planId)?.outcomes.length)
          throw new Error("Injected storage loss after actual broker ACK");
        await commit(document);
      });
    const outcome = await replay
      .apply(plan.planId, replayConfirmation(plan))
      .finally(() => receiptLoss.mockRestore());
    expect(outcome).toMatchObject({
      stopReason: "journal-unavailable",
      unsent: 1,
      cleanup: "complete",
      outcomes: [{ state: "acknowledged" }],
    });
    expect((await read("connect-replayed")).map((r) => r.original)).toEqual([expected[0]]);
    const encrypted = await readFile(file, "utf8");
    expect(JSON.parse(encrypted)).toMatchObject({ schemaVersion: 3 });
    expect(encrypted).not.toContain("original-key");
    expect(encrypted).not.toContain(b64(JSON.stringify({ amount: 1, repaired: true })));
    await replay.invalidate();
    await session.disconnect();
    await vault.lock();
    vault = await openPassphraseVault({ dataRoot, passphrase, mode: "unlock" });
    const reopenedJournal = new RepairJournal(new AtomicRepairFileStore(file, vault.protector));
    expect((await reopenedJournal.snapshot(plan.planId)).pendingIndex).toBe(0);
    expect(session.writeContext()).toBeNull();
    await input.connect();
    replay = new RecordReplayService(
      () => session.reviewedWriteScope(),
      undefined,
      undefined,
      reopenedJournal,
      encoding,
    );
    recovery = new RepairRecoveryService(
      reopenedJournal,
      replay,
      new RepairReconciliationReader(
        () => {
          const scope = session.reviewedWriteScope(),
            readScope = session.recordReadScope();
          return scope && readScope
            ? { scope, readScope, close: (): Promise<void> => Promise.resolve() }
            : null;
        },
        () => ({
          codecs: { key: "auto", value: "auto" },
          protection: KAFKA_RECORD_PROTECTION_DEFAULTS,
        }),
      ),
    );
    expect(
      await recovery.reconcile({
        jobId: plan.planId,
        recordIndex: 0,
        offset: outcome.outcomes[0]!.receipt!.offset,
        targetProfile: null,
      }),
    ).toMatchObject({ state: "equivalent", cleanup: "complete" });
    expect((await reopenedJournal.snapshot(plan.planId)).pendingIndex).toBe(0);
    const continuation = await recovery.review({ jobId: plan.planId, targetProfile: null });
    expect(continuation.skipped).toEqual({ acknowledged: 0, rejected: 0, uncertain: 1 });
    expect(continuation.review.batch.records).toEqual([expected[1]]);
    const completed = await replay.apply(
      continuation.review.planId,
      replayConfirmation(continuation.review),
    );
    expect(completed).toMatchObject({
      stopReason: "complete",
      total: 1,
      unsent: 0,
      cleanup: "complete",
      journal: "confirmed",
    });
    expect(
      await replay.apply(continuation.review.planId, replayConfirmation(continuation.review)),
    ).toEqual(completed);
    expect((await read("connect-replayed")).map((r) => r.original)).toEqual(expected);
    expect((await read("connect-dlq")).map((r) => r.original)).toEqual(
      immutable.map((r) => r.original),
    );
    expect(await sourceOffset()).toEqual(beforeOffsets);
    const root = (await reopenedJournal.list()).find((j) => j.id === plan.planId);
    expect(root).toMatchObject({
      uncertainIndex: 0,
      continuationId: continuation.review.planId,
      canArchive: false,
    });
    await recovery.invalidate();
    await replay.invalidate();
    await vault.lock();
    vault = await openPassphraseVault({ dataRoot, passphrase, mode: "unlock" });
    const restored = new RepairJournal(new AtomicRepairFileStore(file, vault.protector));
    expect((await restored.list()).map((j) => j.id).sort()).toEqual(
      [plan.planId, continuation.review.planId].sort(),
    );
    expect((await restored.snapshot(plan.planId)).findings[0]?.state).toBe("equivalent");
  } catch (error) {
    failures.push(error);
  } finally {
    const cleanup = await Promise.allSettled([recovery?.invalidate(), replay?.invalidate()]);
    cleanup.push(...(await Promise.allSettled([vault?.lock()])));
    for (const result of cleanup) if (result.status === "rejected") failures.push(result.reason);
    if (!failures.length) await rm(dataRoot, { recursive: true, force: true });
  }
  if (failures.length)
    throw new AggregateError(failures, `Connect repair or cleanup failed; preserve ${dataRoot}`, {
      cause: failures[0],
    });
}
