import {
  SCHEMA_SAMPLE_LIMITS,
  parseRecordBatchInput,
  type RecordBatchInput,
  type RecordBatchReview,
  type RecordBatchOutcome,
} from "../contracts/schema-samples";
import type { KafkaWriteOutcome } from "../contracts/reviewed-writes";

import type { KafkaActiveConnection } from "./types";

interface Context {
  readonly connection: KafkaActiveConnection;
  readonly generation: number;
  readonly connectionName: string;
}
interface Plan {
  readonly context: Context;
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
    private readonly context: () => Context | null,
    private readonly now = Date.now,
    private readonly wait = pause,
  ) {}
  invalidate(): void {
    for (const plan of this.plans.values()) plan.controller.abort();
  }
  async review(input: RecordBatchInput): Promise<RecordBatchReview> {
    const parsed = parseRecordBatchInput(input);
    const context = this.context();
    if (!context?.connection.reviewWrite || !context.connection.applyWrite)
      throw new Error("Connect an adapter supporting reviewed writes.");
    if (this.active || this.reviews >= 2)
      throw new Error("Wait for the current batch or reviews to finish.");
    this.reviews++;
    try {
      // All records share one topic/partition. Validate the destination without producing.
      await context.connection.reviewWrite({
        kind: "record",
        topic: parsed.topic,
        partition: parsed.partition,
        record: parsed.records[0]!,
      });
      if (!this.current(context)) throw new Error("Connection changed during review.");
      if (this.plans.size >= 4) {
        const oldest = this.plans.keys().next().value;
        if (oldest) this.plans.delete(oldest);
      }
      const review: RecordBatchReview = {
        planId: globalThis.crypto.randomUUID(),
        connectionName: context.connectionName,
        expiresAt: new Date(this.now() + 120_000).toISOString(),
        input: parsed,
      };
      this.plans.set(review.planId, {
        review: structuredClone(review),
        context,
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
    if (
      this.active ||
      !this.current(plan.context) ||
      this.now() >= Date.parse(plan.review.expiresAt)
    )
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
  private current(expected: Context): boolean {
    const actual = this.context();
    return actual?.connection === expected.connection && actual.generation === expected.generation;
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
      if (!this.current(plan.context)) {
        stopReason = "connection-changed";
        break;
      }
      if (plan.controller.signal.aborted) {
        stopReason = "cancelled";
        break;
      }
      let outcome: KafkaWriteOutcome;
      try {
        outcome = await plan.context.connection.applyWrite!({
          kind: "record",
          topic: input.topic,
          partition: input.partition,
          record,
        });
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
