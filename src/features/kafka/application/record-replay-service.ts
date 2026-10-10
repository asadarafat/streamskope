import {
  parseRecordReplayInput,
  parseRecordReplayReview,
  replayBatch,
  replayConfirmation,
  type RecordReplayInput,
  type RecordReplayReview,
  type RecordReplayOutcome,
} from "../contracts/record-replay";
import type { KafkaWriteDestination, KafkaWriteInput } from "../contracts/reviewed-writes";

import { RecordBatchService } from "./record-batch-service";
import type { ReviewedWriteScope, WriteDispatch } from "./connection-scope";
import type {
  ReviewedReplayDestination,
  ReviewedReplayDestinationPort,
} from "./replay-destination";
import { ownedCleanupFailure } from "./session-lifecycle";
import type { RepairJournal, RepairContinuation } from "./repair-journal";
import type { ReplayEncodingPort } from "./structured-replay-service";

/** Combine source and isolated destination authority without exposing either adapter. */
function replayScope(
  source: ReviewedWriteScope,
  destination: ReviewedWriteScope,
  signal: AbortSignal,
): ReviewedWriteScope {
  const isCurrent = (): boolean => !signal.aborted && source.isCurrent() && destination.isCurrent();
  return {
    connectionName: destination.connectionName,
    isCurrent,
    ...(destination.reviewWrite === undefined
      ? {}
      : {
          reviewWrite: async (input: KafkaWriteInput): Promise<KafkaWriteDestination | void> => {
            if (!isCurrent()) throw new Error("Replay inputs changed during review.");
            const result = await destination.reviewWrite!(input);
            if (!isCurrent()) throw new Error("Replay inputs changed during review.");
            return result;
          },
        }),
    ...(destination.tryDispatchWrite === undefined
      ? {}
      : {
          tryDispatchWrite: (input: KafkaWriteInput): WriteDispatch =>
            isCurrent() ? destination.tryDispatchWrite!(input) : { started: false as const },
        }),
  };
}

interface Plan {
  readonly review: RecordReplayReview;
  readonly batch: RecordBatchService;
  readonly batchId: string;
  readonly target: ReviewedReplayDestination;
  readonly controller: AbortController;
  readonly timer: ReturnType<typeof setTimeout>;
  readonly continuation?: RepairContinuation;
  current(): boolean;
  close?: Promise<void>;
  operation?: Promise<RecordReplayOutcome>;
}
export class RecordReplayService {
  private readonly plans = new Map<string, Plan>();
  private readonly reviews = new Set<AbortController>();
  private readonly pendingReviews = new Set<Promise<RecordReplayReview>>();
  private active = false;
  private invalidating = false;
  private cleanupUnconfirmed = false;
  constructor(
    private readonly scope: () => ReviewedWriteScope | null,
    private readonly destinations?: ReviewedReplayDestinationPort,
    private readonly now = Date.now,
    private readonly journal?: RepairJournal,
    private readonly encoding?: ReplayEncodingPort,
    private readonly currentDestination?: () => ReviewedReplayDestination | null,
  ) {}
  recoveryAvailable(): boolean {
    return (
      !this.invalidating && !this.cleanupUnconfirmed && !this.active && this.reviews.size === 0
    );
  }
  review(input: RecordReplayInput, continuation?: RepairContinuation): Promise<RecordReplayReview> {
    const operation = this.prepare(input, continuation).catch((error: unknown): never => {
      if (ownedCleanupFailure(error) !== undefined) this.cleanupUnconfirmed = true;
      throw error;
    });
    this.pendingReviews.add(operation);
    void operation.finally(() => this.pendingReviews.delete(operation)).catch(() => undefined);
    return operation;
  }
  private async prepare(
    input: RecordReplayInput,
    continuation?: RepairContinuation,
  ): Promise<RecordReplayReview> {
    if (this.cleanupUnconfirmed || this.invalidating)
      throw new Error(
        "A replay destination did not close cleanly. Resolve cleanup before reviewing another replay.",
      );
    const parsed = parseRecordReplayInput(input);
    const source = this.scope();
    if (!source || this.active || this.reviews.size >= 2)
      throw new Error("Connect and wait for active replay operations to finish.");
    const controller = new AbortController();
    this.reviews.add(controller);
    let target: ReviewedReplayDestination | undefined;
    let retained = false;
    try {
      if (parsed.targetProfile) {
        if (!this.destinations) throw new Error("Saved-profile destinations are unavailable.");
        target = await this.destinations.openReviewed(
          parsed.targetProfile.id,
          parsed.targetProfile.revision,
          AbortSignal.any([controller.signal, AbortSignal.timeout(30_000)]),
        );
      } else
        target = this.currentDestination?.() ?? {
          scope: source,
          close: (): Promise<void> => Promise.resolve(),
        };
      const pinned = target;
      if (!source.isCurrent() || !pinned.scope.isCurrent() || controller.signal.aborted)
        throw new Error("Replay authority changed while opening the destination.");
      const encoded = this.encoding
        ? await this.encoding.prepare(
            parsed,
            pinned.registryScope ?? null,
            controller.signal,
            continuation
              ? { review: continuation.parent.review, startIndex: continuation.startIndex }
              : undefined,
          )
        : { batch: replayBatch(parsed), revalidate: (): Promise<boolean> => Promise.resolve(true) };
      const batchInput = encoded.batch;
      const planId = crypto.randomUUID();
      const scope = replayScope(source, pinned.scope, controller.signal);
      const current = (): boolean => scope.isCurrent();
      const first = {
        kind: "record" as const,
        topic: parsed.topic,
        partition: parsed.partition,
        record: batchInput.records[0]!,
      };
      const identity = await scope.reviewWrite?.(first);
      if (
        !identity?.clusterId ||
        !identity.topicId ||
        /^0+$/u.test(identity.topicId.replaceAll("-", ""))
      )
        throw new Error("The broker must report stable cluster and topic identities for replay.");
      if (
        continuation &&
        (identity.clusterId !== continuation.parent.review.destination.clusterId ||
          identity.topicId !== continuation.parent.review.destination.topicId ||
          identity.partitions !== continuation.parent.review.destination.partitions)
      )
        throw new Error("The original repair destination changed. No continuation was prepared.");
      const batch = new RecordBatchService(
        () => scope,
        this.now,
        undefined,
        async (index) => {
          if (!current()) return false;
          if (!(await encoded.revalidate(index)) || !current()) return false;
          const actual = await scope.reviewWrite?.(first);
          return (
            current() &&
            actual?.clusterId === identity.clusterId &&
            actual.topicId === identity.topicId &&
            actual.partitions === identity.partitions
          );
        },
        this.journal
          ? {
              beforeRecord: (index): Promise<void> => this.journal!.intent(planId, index),
              afterRecord: (index, outcome): Promise<void> =>
                this.journal!.receipt(planId, index, outcome),
            }
          : undefined,
      );
      const reviewed = await batch.review(batchInput);
      if (!current()) throw new Error("Replay inputs changed during review.");
      if (this.plans.size >= 4) {
        const oldest = this.plans.values().next().value;
        if (oldest) {
          await this.close(oldest);
          this.plans.delete(oldest.review.planId);
        }
      }
      const review = parseRecordReplayReview({
        planId,
        sourceName: continuation?.parent.review.sourceName ?? source.connectionName,
        targetName: target.scope.connectionName,
        expiresAt: reviewed.expiresAt,
        input: parsed,
        batch: batchInput,
        destination: identity,
        ...(encoded.encoding === undefined ? {} : { encoding: encoded.encoding }),
      });
      const plan: Plan = {
        review: structuredClone(review),
        batch,
        batchId: reviewed.planId,
        target,
        controller,
        current,
        ...(continuation === undefined ? {} : { continuation: structuredClone(continuation) }),
        timer: setTimeout(() => {
          if (!plan.operation) {
            plan.controller.abort();
            void this.close(plan).catch(() => undefined);
          }
        }, 120_000),
      };
      plan.timer.unref?.();
      this.plans.set(review.planId, plan);
      retained = true;
      return review;
    } finally {
      this.reviews.delete(controller);
      if (!retained) {
        controller.abort();
        if (target !== undefined) await this.closeTarget(target);
      }
    }
  }
  cancel(id: string): Promise<void> {
    const plan = this.plans.get(id);
    if (!plan) return Promise.resolve();
    plan.controller.abort();
    plan.batch.cancel(plan.batchId);
    // Let an in-flight write retain its acknowledgement before closing the target.
    return plan.operation
      ? plan.operation.then(
          () => this.close(plan),
          () => this.close(plan),
        )
      : this.close(plan);
  }
  async invalidate(): Promise<void> {
    this.invalidating = true;
    try {
      for (const controller of this.reviews) controller.abort();
      const cancellations = [...this.plans.keys()].map((id) => this.cancel(id));
      await Promise.allSettled(this.pendingReviews);
      const results = await Promise.allSettled(cancellations);
      if (this.cleanupUnconfirmed || results.some((r) => r.status === "rejected"))
        throw new Error("An isolated replay destination did not close cleanly.");
    } finally {
      this.invalidating = false;
    }
  }
  apply(id: string, confirmation: string): Promise<RecordReplayOutcome> {
    const plan = this.plans.get(id);
    if (!plan || confirmation !== replayConfirmation(plan.review))
      return Promise.reject(new Error("Confirm the exact replay destination."));
    if (plan.operation) return plan.operation;
    if (this.cleanupUnconfirmed)
      return Promise.reject(
        new Error(
          "A replay destination did not close cleanly. Resolve cleanup before starting another replay.",
        ),
      );
    if (
      this.invalidating ||
      this.active ||
      this.reviews.size > 0 ||
      !plan.current() ||
      this.now() >= Date.parse(plan.review.expiresAt)
    )
      return Promise.reject(new Error("Replay is busy, expired or stale. Review again."));
    this.active = true;
    clearTimeout(plan.timer);
    plan.operation = Promise.resolve()
      .then(async (): Promise<RecordReplayOutcome> => {
        let result;
        let journalStarted = false;
        try {
          if (this.journal) {
            await this.journal.begin(plan.review, plan.continuation);
            journalStarted = true;
          }
          result = await plan.batch.apply(plan.batchId);
        } catch (error) {
          let cleanup: "complete" | "unavailable" = "complete";
          await this.close(plan).catch(() => {
            cleanup = "unavailable";
          });
          if (journalStarted) await this.journal!.finish(id, false, cleanup).catch(() => undefined);
          throw error;
        }
        let cleanup: RecordReplayOutcome["cleanup"] = "complete";
        try {
          await this.close(plan);
        } catch {
          cleanup = "unavailable";
        }
        if (!this.journal) return { ...result, cleanup };
        let journal: "confirmed" | "unavailable" = "confirmed";
        try {
          await this.journal.finish(id, result.stopReason === "complete", cleanup);
        } catch {
          journal = "unavailable";
        }
        return {
          ...result,
          cleanup,
          jobId: id,
          journal,
          durability: this.journal.store.durability,
        };
      })
      .finally(() => {
        this.active = false;
      });
    return plan.operation;
  }
  private close(plan: Plan): Promise<void> {
    clearTimeout(plan.timer);
    plan.close ??= Promise.resolve().then(() => this.closeTarget(plan.target));
    return plan.close;
  }
  private async closeTarget(target: ReviewedReplayDestination): Promise<void> {
    try {
      await target.close();
    } catch (cleanupCause) {
      this.cleanupUnconfirmed = true;
      throw Object.assign(new Error("An isolated replay destination did not close cleanly."), {
        cleanupCause,
      });
    }
  }
}
