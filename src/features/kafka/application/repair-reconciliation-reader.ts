import type { RecordReadSettings } from "../contracts/finite-record-read";
import { KAFKA_QUERY_LIMITS } from "../contracts/query-search";
import { kafkaRecordLocator } from "../contracts/record-locator";
import { sameKafkaCompleteRecord } from "../contracts/record-bytes";
import type { RepairJob } from "../contracts/repair-jobs";
import type { RepairFinding, RepairReconciliationInput } from "../contracts/repair-recovery";

import { FiniteRecordRead } from "./finite-record-read";
import type {
  ReviewedReplayDestination,
  ReviewedReplayDestinationPort,
} from "./replay-destination";
import { ownedCleanupFailure } from "./session-lifecycle";

interface OwnedObservation {
  readonly controller: AbortController;
  target?: ReviewedReplayDestination | undefined;
  reader?: FiniteRecordRead;
  task?: Promise<RepairFinding> | undefined;
  debt: boolean;
}
/** A destination observation never upgrades an uncertain dispatch into an acknowledgement. */
export class RepairReconciliationReader {
  private owner: OwnedObservation | undefined;
  private closing = false;
  constructor(
    private readonly currentTarget: () => ReviewedReplayDestination | null,
    private readonly settings: () => RecordReadSettings,
    private readonly destinations?: ReviewedReplayDestinationPort,
    private readonly now = Date.now,
  ) {}
  available(): boolean {
    return !this.closing && this.owner === undefined;
  }
  observe(job: RepairJob, input: RepairReconciliationInput): Promise<RepairFinding> {
    if (!this.available()) throw new Error("An observation still owns its reader or destination.");
    const owner: OwnedObservation = { controller: new AbortController(), debt: false };
    this.owner = owner;
    owner.task = this.run(owner, job, input).finally(() => {
      owner.task = undefined;
      if (!owner.debt && this.owner === owner) this.owner = undefined;
    });
    return owner.task;
  }
  async invalidate(): Promise<void> {
    this.closing = true;
    try {
      const owner = this.owner;
      if (!owner) return;
      owner.controller.abort();
      owner.reader?.stop("revoked");
      await owner.task?.catch(() => undefined);
      await owner.reader?.retryCleanup();
      await owner.target?.close();
      if (owner.debt && !owner.reader && !owner.target)
        throw new Error("Observation opening cleanup could not be confirmed.");
      if (this.owner === owner) this.owner = undefined;
    } finally {
      this.closing = false;
    }
  }
  private async run(
    owner: OwnedObservation,
    job: RepairJob,
    input: RepairReconciliationInput,
  ): Promise<RepairFinding> {
    const finding = (
      state: RepairFinding["state"],
      cleanup: RepairFinding["cleanup"],
    ): RepairFinding => ({
      id: crypto.randomUUID(),
      recordIndex: input.recordIndex,
      offset: input.offset,
      observedAt: new Date(this.now()).toISOString(),
      state,
      cleanup,
    });
    let state: RepairFinding["state"] = "unavailable";
    let deadline: ReturnType<typeof setTimeout> | undefined;
    try {
      const authority = this.currentTarget();
      if (!authority || !authority.scope.isCurrent())
        throw new Error("Connect before checking a destination.");
      const settings = JSON.stringify(this.settings());
      owner.target = input.targetProfile
        ? await this.destinations?.openReviewed(
            input.targetProfile.id,
            input.targetProfile.revision,
            AbortSignal.any([owner.controller.signal, AbortSignal.timeout(30_000)]),
          )
        : authority;
      const scope = owner.target?.readScope;
      if (!scope) throw new Error("Destination reads are unavailable.");
      const assertCurrent = (): void => {
        if (
          owner.controller.signal.aborted ||
          !authority.scope.isCurrent() ||
          !scope.isCurrent() ||
          JSON.stringify(this.settings()) !== settings
        )
          throw new Error("Observation authority changed.");
      };
      const reader = new FiniteRecordRead({
        scope,
        input: {
          topic: job.review.input.topic,
          range: { mode: "earliest" },
          maxRecords: 1,
          search: {
            key: "",
            value: "",
            offset: "",
            timestamp: "",
            partition: job.review.input.partition,
            offsetExact: input.offset,
          },
        },
        limits: {
          scanRecords: KAFKA_QUERY_LIMITS.scanRecords,
          scanBytes: KAFKA_QUERY_LIMITS.scanBytes,
          passes: 1,
        },
        deadlineAt: this.now() + 30_000,
        authority: owner.controller.signal,
        assertCurrent,
        changed: (): void => undefined,
        now: this.now,
      });
      owner.reader = reader;
      deadline = setTimeout(() => {
        owner.controller.abort();
        reader.stop("deadline");
      }, 30_000);
      deadline.unref?.();
      const result = await reader.run((message) => {
        const actual = kafkaRecordLocator(message),
          expected = job.review.destination;
        if (
          !actual ||
          actual.topic !== job.review.input.topic ||
          actual.clusterId !== expected.clusterId ||
          actual.topicId !== expected.topicId ||
          actual.partition !== job.review.input.partition ||
          actual.offset !== input.offset ||
          message.original?.state !== "complete"
        )
          throw new Error("Destination identity or bytes are unavailable.");
        state = sameKafkaCompleteRecord(
          message.original,
          job.review.batch.records[input.recordIndex]!,
        )
          ? "equivalent"
          : "different";
        return Promise.resolve("committed");
      });
      await reader.idle();
      assertCurrent();
      if (state === "unavailable") {
        const p = result.coverage?.partitions;
        if (
          result.reason === "range-complete" &&
          result.source.clusterId === job.review.destination.clusterId &&
          result.source.topicId === job.review.destination.topicId &&
          p?.length === 1 &&
          p[0]?.partition === job.review.input.partition &&
          p[0].startOffset === input.offset &&
          p[0].endOffset === String(BigInt(input.offset) + 1n) &&
          p[0].nextOffset === p[0].endOffset
        )
          state = "not-observed";
      }
    } catch (error) {
      state = "unavailable";
      if (ownedCleanupFailure(error) !== undefined && !owner.target && !owner.reader)
        owner.debt = true;
    } finally {
      if (deadline) clearTimeout(deadline);
      try {
        await owner.reader?.idle();
      } catch {
        owner.debt = true;
      }
      // The original reader must stop before its isolated connection can be released.
      if (!owner.debt) {
        try {
          await owner.target?.close();
        } catch {
          owner.debt = true;
        }
      }
    }
    return finding(state, owner.debt ? "unavailable" : "complete");
  }
}
