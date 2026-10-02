import {
  parseKafkaWriteInput,
  type KafkaWriteInput,
  type KafkaWriteReview,
  type KafkaWriteOutcome,
} from "../contracts";

import type { KafkaActiveConnection } from "./types";

interface WriteContext {
  readonly connection: KafkaActiveConnection;
  readonly generation: number;
  readonly connectionName: string;
}
interface Plan {
  readonly review: KafkaWriteReview;
  readonly context: WriteContext;
  operation?: Promise<KafkaWriteOutcome>;
}

/** A plan authorizes one attempt against one connection generation, never a retry. */
export class KafkaReviewedWriteService {
  private readonly plans = new Map<string, Plan>();
  constructor(
    private readonly context: () => WriteContext | null,
    private readonly now = Date.now,
    private readonly createId = (): string => globalThis.crypto.randomUUID(),
  ) {}

  async review(input: KafkaWriteInput): Promise<KafkaWriteReview> {
    const parsed = parseKafkaWriteInput(input);
    const context = this.context();
    if (context?.connection.reviewWrite === undefined)
      throw new Error("Connect a Kafka adapter that supports reviewed writes.");
    await context.connection.reviewWrite(parsed);
    if (!this.current(context))
      throw new Error("The connection changed. Review the destination again.");
    if (this.plans.size >= 32) {
      // Evicted identifiers are rejected, never reconstructed into another write.
      const oldest = this.plans.keys().next().value;
      if (oldest !== undefined) this.plans.delete(oldest);
    }
    const review = {
      planId: this.createId(),
      connectionName: context.connectionName,
      expiresAt: new Date(this.now() + 120_000).toISOString(),
      input: parsed,
    };
    this.plans.set(review.planId, { review: structuredClone(review), context });
    return review;
  }

  apply(planId: string): Promise<KafkaWriteOutcome> {
    const plan = this.plans.get(planId);
    if (plan === undefined)
      return Promise.reject(
        new Error(
          "The write review is no longer available. Review again; do not retry an uncertain write.",
        ),
      );
    if (plan.operation !== undefined) return plan.operation;
    if (!this.current(plan.context) || this.now() >= Date.parse(plan.review.expiresAt))
      return Promise.reject(
        new Error("The review expired or its connection changed. Review the destination again."),
      );
    const connection = plan.context.connection;
    if (connection.applyWrite === undefined)
      return Promise.reject(new Error("The active adapter cannot write."));
    // Install the promise before invoking the port, so duplicate confirmations coalesce.
    plan.operation = Promise.resolve()
      .then(() => {
        if (!this.current(plan.context))
          return {
            state: "rejected",
            detail: "The connection changed before dispatch. Review the destination again.",
            receipt: null,
            verification: "not-applicable",
          } as const;
        return connection.applyWrite!(plan.review.input);
      })
      .catch((): KafkaWriteOutcome => ({
        state: "unknown",
        detail:
          "The write result is unknown. Inspect the destination before creating a new review; no automatic retry was attempted.",
        receipt: null,
        verification: "unavailable",
      }));
    return plan.operation;
  }

  private current(expected: WriteContext): boolean {
    const current = this.context();
    return (
      current?.connection === expected.connection && current.generation === expected.generation
    );
  }
}
