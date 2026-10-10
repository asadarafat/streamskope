export interface OwnedKafkaResource {
  close(): Promise<void>;
}
export interface OwnedKafkaResult<T> {
  readonly value: T;
  readonly cleaned: boolean;
}

/** Keep original resources and admitted operations until their actual cleanup completes. */
export class OwnedKafkaResources {
  private readonly resources = new Set<OwnedKafkaResource>();
  private readonly pending = new Set<Promise<unknown>>();
  private closed = false;
  private cleanupBlocked = false;
  private closePromise: Promise<void> | undefined;

  constructor(private readonly lifetime: AbortSignal) {}

  get available(): boolean {
    return !this.closed && !this.cleanupBlocked && !this.lifetime.aborted;
  }
  get cleanupUnresolved(): boolean {
    return this.cleanupBlocked;
  }

  run<Resource extends OwnedKafkaResource, Result>(
    create: () => Resource,
    run: (resource: Resource) => Promise<Result>,
  ): Promise<OwnedKafkaResult<Result>> {
    if (!this.available)
      return Promise.reject(
        new Error("Kafka administration was revoked or original cleanup is unresolved."),
      );
    const resource = create();
    this.resources.add(resource);
    const operation = (async (): Promise<OwnedKafkaResult<Result>> => {
      let value: Result;
      let cleaned = false;
      try {
        value = await run(resource);
      } finally {
        try {
          await resource.close();
          this.resources.delete(resource);
          cleaned = true;
        } catch {
          this.cleanupBlocked = true;
        }
      }
      return { value, cleaned };
    })();
    this.pending.add(operation);
    void operation.finally(() => this.pending.delete(operation)).catch(() => undefined);
    return operation;
  }

  close(): Promise<void> {
    this.closed = true;
    this.closePromise ??= this.closeResources();
    return this.closePromise;
  }
  private async closeResources(): Promise<void> {
    await Promise.allSettled([...this.pending]);
    const closed = await Promise.allSettled(
      [...this.resources].map(async (resource) => {
        await resource.close();
        this.resources.delete(resource);
      }),
    );
    if (closed.some((result) => result.status === "rejected"))
      throw new Error(
        "Original Kafka resources did not close cleanly; cleanup remains unresolved.",
      );
  }
}
