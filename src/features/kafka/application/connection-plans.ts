import type { KafkaActiveConnection } from "./types";

export interface ReviewContext {
  readonly connection: KafkaActiveConnection;
  readonly generation: number;
  readonly connectionName: string;
}
export interface ConnectionPlan<T, O> {
  readonly id: string;
  readonly context: ReviewContext;
  readonly expiresAt: string;
  readonly value: T;
  operation?: Promise<O>;
}
/** Bounded connection-pinned reviews. One identifier authorizes at most one attempt. */
export class ConnectionPlans<T, O> {
  private readonly plans = new Map<string, ConnectionPlan<T, O>>();
  constructor(
    readonly context: () => ReviewContext | null,
    private readonly now = Date.now,
  ) {}
  current(expected: ReviewContext): boolean {
    const actual = this.context();
    return actual?.connection === expected.connection && actual.generation === expected.generation;
  }
  add(context: ReviewContext, value: T): ConnectionPlan<T, O> {
    if (!this.current(context)) throw new Error("The connection changed. Review again.");
    if (this.plans.size >= 16) {
      const oldest = this.plans.keys().next().value;
      if (oldest) this.plans.delete(oldest);
    }
    const plan = {
      id: crypto.randomUUID(),
      context,
      value: structuredClone(value),
      expiresAt: new Date(this.now() + 120_000).toISOString(),
    };
    this.plans.set(plan.id, plan);
    return plan;
  }
  apply(
    id: string,
    confirm: (value: T) => boolean,
    run: (plan: ConnectionPlan<T, O>) => Promise<O>,
  ): Promise<O> {
    const plan = this.plans.get(id);
    if (!plan || !confirm(plan.value))
      return Promise.reject(new Error("Confirm the exact reviewed scope."));
    if (plan.operation) return plan.operation;
    if (!this.current(plan.context) || this.now() >= Date.parse(plan.expiresAt))
      return Promise.reject(new Error("The plan expired or its connection changed. Review again."));
    plan.operation = Promise.resolve().then(() => {
      if (!this.current(plan.context)) throw new Error("Connection changed before dispatch.");
      return run(plan);
    });
    return plan.operation;
  }
}
