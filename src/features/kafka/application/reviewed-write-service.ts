import {
  parseKafkaWriteInput,
  type KafkaWriteInput,
  type KafkaWriteReview,
  type KafkaWriteOutcome,
} from "../contracts";

import type { ReviewedWriteScope } from "./connection-scope";

interface Plan {
  readonly review: KafkaWriteReview;
  readonly scope: ReviewedWriteScope;
  operation?: Promise<KafkaWriteOutcome>;
}

/** A plan authorizes one attempt against one connection generation, never a retry. */
export class KafkaReviewedWriteService {
  private readonly plans = new Map<string, Plan>();
  constructor(
    private readonly scope: () => ReviewedWriteScope | null,
    private readonly now = Date.now,
    private readonly createId = (): string => globalThis.crypto.randomUUID(),
  ) {}

  async review(input: KafkaWriteInput): Promise<KafkaWriteReview> {
    const parsed = parseKafkaWriteInput(input);
    const scope = this.scope();
    if (scope?.reviewWrite === undefined)
      throw new Error("Connect a Kafka adapter that supports reviewed writes.");
    await scope.reviewWrite(parsed);
    if (!scope.isCurrent())
      throw new Error("The connection changed. Review the destination again.");
    if (this.plans.size >= 32) {
      // Evicted identifiers are rejected, never reconstructed into another write.
      const oldest = this.plans.keys().next().value;
      if (oldest !== undefined) this.plans.delete(oldest);
    }
    const review = {
      planId: this.createId(),
      connectionName: scope.connectionName,
      expiresAt: new Date(this.now() + 120_000).toISOString(),
      input: parsed,
    };
    this.plans.set(review.planId, { review: structuredClone(review), scope });
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
    if (!plan.scope.isCurrent() || this.now() >= Date.parse(plan.review.expiresAt))
      return Promise.reject(
        new Error("The review expired or its connection changed. Review the destination again."),
      );
    const scope = plan.scope;
    if (scope.tryDispatchWrite === undefined)
      return Promise.reject(new Error("The active adapter cannot write."));
    // Install the promise before invoking the port, so duplicate confirmations coalesce.
    plan.operation = Promise.resolve()
      .then(() => {
        const attempt = scope.tryDispatchWrite!(plan.review.input);
        if (!attempt.started)
          return {
            state: "rejected",
            detail: "The connection changed before dispatch. Review the destination again.",
            receipt: null,
            verification: "not-applicable",
          } as const;
        return attempt.result;
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
}
