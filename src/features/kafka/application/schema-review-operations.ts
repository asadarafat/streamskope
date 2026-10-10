import { SCHEMA_REVIEW_LIMITS } from "../contracts/schema-changes";

import type { SchemaRegistryReviewScope } from "./connection-scope";

/** Shared authority, bounded reads and serial reviewed writes for one Registry connection owner. */
export class SchemaReviewOperations {
  private epoch = 0;
  private reads = 0;
  private writing = false;
  private readonly pending = new Set<AbortController>();
  constructor(private readonly current: () => SchemaRegistryReviewScope | null) {}
  context(): SchemaRegistryReviewScope | null {
    const scope = this.current(),
      epoch = this.epoch;
    return scope
      ? { ...scope, isCurrent: (): boolean => epoch === this.epoch && scope.isCurrent() }
      : null;
  }
  invalidate(): void {
    this.epoch++;
    for (const controller of this.pending) controller.abort();
  }
  private async deadline<T>(run: (signal: AbortSignal) => Promise<T>): Promise<T> {
    const controller = new AbortController();
    this.pending.add(controller);
    try {
      return await run(
        AbortSignal.any([controller.signal, AbortSignal.timeout(SCHEMA_REVIEW_LIMITS.operationMs)]),
      );
    } finally {
      this.pending.delete(controller);
    }
  }
  async read<T>(run: (signal: AbortSignal) => Promise<T>): Promise<T> {
    if (this.reads >= SCHEMA_REVIEW_LIMITS.pendingReads)
      throw new Error("Wait for the current Registry reviews.");
    this.reads++;
    try {
      return await this.deadline(run);
    } finally {
      this.reads--;
    }
  }
  async write<T>(run: (signal: AbortSignal) => Promise<T>): Promise<T> {
    if (this.writing)
      throw new Error("Another reviewed Registry change is active. Wait and review again.");
    this.writing = true;
    try {
      return await this.deadline(run);
    } finally {
      this.writing = false;
    }
  }
}
