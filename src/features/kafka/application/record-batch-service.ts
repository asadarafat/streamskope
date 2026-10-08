import {
  SCHEMA_SAMPLE_LIMITS,
  parseRecordBatchInput,
  type RecordBatchInput,
  type RecordBatchReview,
  type RecordBatchOutcome,
} from "../contracts/schema-samples";
import type { KafkaWriteOutcome } from "../contracts/reviewed-writes";

import type { ReviewedWriteScope } from "./connection-scope";

interface Plan {
  readonly scope: ReviewedWriteScope;
  readonly review: RecordBatchReview;
  readonly controller: AbortController;
  operation?: Promise<RecordBatchOutcome>;
}
const pause = (milliseconds: number, signal: AbortSignal): Promise<void> =>
  new Promise((resolve) => {
    const finish = (): void => {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    };
    const timer = setTimeout(finish, milliseconds);
    signal.addEventListener("abort", finish, { once: true });
    if (signal.aborted) finish();
  });
export class RecordBatchService {
  private readonly plans = new Map<string, Plan>();
  private active = false;
  private reviews = 0;
  constructor(
    private readonly scope: () => ReviewedWriteScope | null,
    private readonly now = Date.now,
    private readonly wait = pause,
    private readonly beforeDispatch?: () => Promise<boolean>,
  ) {}
  invalidate(): void {
    for (const plan of this.plans.values()) plan.controller.abort();
  }
  async review(input: RecordBatchInput): Promise<RecordBatchReview> {
    const parsed = parseRecordBatchInput(input);
    const scope = this.scope();
    if (!scope?.reviewWrite || !scope.tryDispatchWrite)
      throw new Error("Connect an adapter supporting reviewed writes.");
    if (this.active || this.reviews >= 2)
      throw new Error("Wait for the current batch or reviews to finish.");
    this.reviews++;
    try {
      // All records share one topic/partition. Validate the destination without producing.
      await scope.reviewWrite({
        kind: "record",
        topic: parsed.topic,
        partition: parsed.partition,
        record: parsed.records[0]!,
      });
      if (!scope.isCurrent()) throw new Error("Connection changed during review.");
      if (this.plans.size >= 4) {
        const oldest = this.plans.keys().next().value;
        if (oldest) this.plans.delete(oldest);
      }
      const review: RecordBatchReview = {
        planId: globalThis.crypto.randomUUID(),
        connectionName: scope.connectionName,
        expiresAt: new Date(this.now() + 120_000).toISOString(),
        input: parsed,
      };
      this.plans.set(review.planId, {
        review: structuredClone(review),
        scope,
        controller: new AbortController(),
      });
      return review;
    } finally {
      this.reviews--;
    }
  }
  cancel(planId: string): void {
    this.plans.get(planId)?.controller.abort();
  }
  apply(planId: string): Promise<RecordBatchOutcome> {
    const plan = this.plans.get(planId);
    if (!plan)
      return Promise.reject(
        new Error("Review is unavailable; never automatically retry an uncertain batch."),
      );
    if (plan.operation) return plan.operation;
    if (this.active || !plan.scope.isCurrent() || this.now() >= Date.parse(plan.review.expiresAt))
      return Promise.reject(
        new Error("Batch is busy, expired or belongs to an old connection. Review again."),
      );
    this.active = true;
    plan.operation = Promise.resolve()
      .then(() => this.run(plan))
      .finally(() => {
        this.active = false;
      });
    return plan.operation;
  }
  private async run(plan: Plan): Promise<RecordBatchOutcome> {
    const started = this.now();
    const input = plan.review.input;
    const outcomes: KafkaWriteOutcome[] = [];
    let stopReason: RecordBatchOutcome["stopReason"] = "complete";
    for (const record of input.records) {
      if (this.now() - started >= SCHEMA_SAMPLE_LIMITS.durationMs) {
        stopReason = "deadline";
        break;
      }
      if (plan.controller.signal.aborted) {
        stopReason = "cancelled";
        break;
      }
      if (!plan.scope.isCurrent()) {
        stopReason = "connection-changed";
        break;
      }
      if (this.beforeDispatch) {
        let valid = false;
        try {
          valid = await this.beforeDispatch();
        } catch {
          /* Fail closed before dispatch. */
        }
        if (plan.controller.signal.aborted) {
          stopReason = "cancelled";
          break;
        }
        if (!plan.scope.isCurrent()) {
          stopReason = "connection-changed";
          break;
        }
        if (!valid) {
          stopReason = "destination-changed";
          break;
        }
        if (this.now() - started >= SCHEMA_SAMPLE_LIMITS.durationMs) {
          stopReason = "deadline";
          break;
        }
      }
      let outcome: KafkaWriteOutcome;
      try {
        const dispatch = plan.scope.tryDispatchWrite!({
          kind: "record",
          topic: input.topic,
          partition: input.partition,
          record,
          ...(input.timestamps?.[outcomes.length] == null
            ? {}
            : { timestamp: input.timestamps[outcomes.length]! }),
        });
        if (!dispatch.started) {
          stopReason = "connection-changed";
          break;
        }
        outcome = await dispatch.result;
      } catch {
        outcome = {
          state: "unknown",
          detail:
            "Dispatch may have reached Kafka. Inspect the destination before any new attempt.",
          receipt: null,
          verification: "unavailable",
        };
      }
      outcomes.push(outcome);
      if (outcome.state !== "acknowledged") {
        stopReason = "write-failed";
        break;
      }
      if (outcomes.length < input.records.length)
        await this.wait(Math.ceil(1_000 / input.ratePerSecond), plan.controller.signal);
    }
    return {
      total: input.records.length,
      unsent: input.records.length - outcomes.length,
      outcomes,
      stopReason,
    };
  }
}
