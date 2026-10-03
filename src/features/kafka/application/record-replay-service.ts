import {
  parseRecordReplayInput,
  replayBatch,
  replayConfirmation,
  type RecordReplayInput,
  type RecordReplayReview,
  type RecordReplayOutcome,
} from "../contracts/record-replay";

import { RecordBatchService } from "./record-batch-service";
import type { ReviewContext } from "./connection-plans";
import type { ReplayDestination, ReplayDestinationPort } from "./replay-destination";

interface Plan {
  readonly review: RecordReplayReview;
  readonly batch: RecordBatchService;
  readonly batchId: string;
  readonly target: ReplayDestination;
  readonly controller: AbortController;
  readonly timer: ReturnType<typeof setTimeout>;
  current(): boolean;
  close?: Promise<void>;
  operation?: Promise<RecordReplayOutcome>;
}
export class RecordReplayService {
  private readonly plans = new Map<string, Plan>();
  private readonly reviews = new Set<AbortController>();
  private readonly pendingReviews = new Set<Promise<RecordReplayReview>>();
  private active = false;
  constructor(
    private readonly context: () => ReviewContext | null,
    private readonly destinations?: ReplayDestinationPort,
    private readonly now = Date.now,
  ) {}
  private matches(context: ReviewContext): boolean {
    const actual = this.context();
    return actual?.connection === context.connection && actual.generation === context.generation;
  }
  review(input: RecordReplayInput): Promise<RecordReplayReview> {
    const operation = this.prepare(input);
    this.pendingReviews.add(operation);
    void operation.finally(() => this.pendingReviews.delete(operation)).catch(() => undefined);
    return operation;
  }
  private async prepare(input: RecordReplayInput): Promise<RecordReplayReview> {
    const parsed = parseRecordReplayInput(input),
      batchInput = replayBatch(parsed);
    const source = this.context();
    if (!source || this.active || this.reviews.size >= 2)
      throw new Error("Connect and wait for active replay operations to finish.");
    const controller = new AbortController();
    this.reviews.add(controller);
    let target: ReplayDestination | undefined;
    let retained = false;
    try {
      if (parsed.targetProfile) {
        if (!this.destinations) throw new Error("Saved-profile destinations are unavailable.");
        target = await this.destinations.open(
          parsed.targetProfile.id,
          parsed.targetProfile.revision,
          AbortSignal.any([controller.signal, AbortSignal.timeout(30_000)]),
        );
      } else
        target = {
          connection: source.connection,
          name: source.connectionName,
          current: (): boolean => this.matches(source),
          close: (): Promise<void> => Promise.resolve(),
        };
      const pinned = target;
      const current = (): boolean =>
        !controller.signal.aborted && this.matches(source) && pinned.current();
      const first = {
        kind: "record" as const,
        topic: parsed.topic,
        partition: parsed.partition,
        record: batchInput.records[0]!,
      };
      const identity = await target.connection.reviewWrite?.(first);
      if (
        !identity?.clusterId ||
        !identity.topicId ||
        /^0+$/u.test(identity.topicId.replaceAll("-", ""))
      )
        throw new Error("The broker must report stable cluster and topic identities for replay.");
      const batch = new RecordBatchService(
        () =>
          current()
            ? {
                connection: pinned.connection,
                generation: source.generation,
                connectionName: pinned.name,
              }
            : null,
        this.now,
        undefined,
        async () => {
          if (!current()) return false;
          const actual = await pinned.connection.reviewWrite?.(first);
          return (
            current() &&
            actual?.clusterId === identity.clusterId &&
            actual.topicId === identity.topicId &&
            actual.partitions === identity.partitions
          );
        },
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
      const review: RecordReplayReview = {
        planId: crypto.randomUUID(),
        sourceName: source.connectionName,
        targetName: target.name,
        expiresAt: reviewed.expiresAt,
        input: parsed,
        batch: batchInput,
        destination: identity,
      };
      const plan: Plan = {
        review: structuredClone(review),
        batch,
        batchId: reviewed.planId,
        target,
        controller,
        current,
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
        await target?.close();
      }
    }
  }
  cancel(id: string): Promise<void> {
    const plan = this.plans.get(id);
    if (!plan) return Promise.resolve();
    plan.controller.abort();
    plan.batch.cancel(plan.batchId);
    // Let an in-flight write retain its acknowledgement before closing the target.
    return plan.operation ? plan.operation.then(() => undefined) : this.close(plan);
  }
  async invalidate(): Promise<void> {
    for (const controller of this.reviews) controller.abort();
    const cancellations = [...this.plans.keys()].map((id) => this.cancel(id));
    await Promise.allSettled(this.pendingReviews);
    const results = await Promise.allSettled(cancellations);
    if (results.some((r) => r.status === "rejected"))
      throw new Error("An isolated replay destination did not close cleanly.");
  }
  apply(id: string, confirmation: string): Promise<RecordReplayOutcome> {
    const plan = this.plans.get(id);
    if (!plan || confirmation !== replayConfirmation(plan.review))
      return Promise.reject(new Error("Confirm the exact replay destination."));
    if (plan.operation) return plan.operation;
    if (
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
        try {
          result = await plan.batch.apply(plan.batchId);
        } catch (error) {
          await this.close(plan).catch(() => undefined);
          throw error;
        }
        let cleanup: RecordReplayOutcome["cleanup"] = "complete";
        try {
          await this.close(plan);
        } catch {
          cleanup = "unavailable";
        }
        return { ...result, cleanup };
      })
      .finally(() => {
        this.active = false;
      });
    return plan.operation;
  }
  private close(plan: Plan): Promise<void> {
    clearTimeout(plan.timer);
    plan.close ??= Promise.resolve().then(() => plan.target.close());
    return plan.close;
  }
}
