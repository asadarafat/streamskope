export type FeatureLifecycleReason = "connection-change" | "shutdown";

export interface FeatureLifecycleParticipant {
  readonly owner: string;
  readonly invalidate?: (reason: FeatureLifecycleReason) => void | Promise<void>;
  readonly drain?: () => void | Promise<void>;
}

/** Invokes cancellation before draining, leaving resource ownership with each feature. */
export class FeatureLifecycle {
  private readonly participants: readonly FeatureLifecycleParticipant[];

  constructor(participants: readonly FeatureLifecycleParticipant[]) {
    this.participants = participants.map((participant) => ({ ...participant }));
  }

  invalidate(): void {
    const failures: Error[] = [];
    for (const { owner, invalidate } of this.participants) {
      if (invalidate === undefined) continue;
      // Connection changes start cancellation immediately without waiting for cleanup.
      try {
        void Promise.resolve(invalidate("connection-change")).catch(() => undefined);
      } catch {
        failures.push(new Error(`${owner} invalidation failed.`));
      }
    }
    if (failures.length > 0)
      throw new AggregateError(failures, "Kafka connection state invalidation failed.");
  }

  async shutdown(): Promise<void> {
    const operations: Array<{ readonly owner: string; readonly operation: Promise<void> }> = [];
    for (const { owner, invalidate } of this.participants) {
      if (invalidate !== undefined)
        operations.push({ owner, operation: this.invoke(() => invalidate("shutdown")) });
    }
    for (const { owner, drain } of this.participants) {
      if (drain !== undefined) operations.push({ owner, operation: this.invoke(drain) });
    }
    const results = await Promise.allSettled(operations.map(({ operation }) => operation));
    const failures = results.flatMap((result, index) =>
      result.status === "rejected"
        ? [new Error(`${operations[index]?.owner ?? "Application"} cleanup failed.`)]
        : [],
    );
    if (failures.length > 0)
      throw new AggregateError(failures, "Kafka application resources did not close cleanly.");
  }

  private invoke(run: () => void | Promise<void>): Promise<void> {
    try {
      return Promise.resolve(run());
    } catch {
      return Promise.reject(new Error("Application cleanup invocation failed."));
    }
  }
}
