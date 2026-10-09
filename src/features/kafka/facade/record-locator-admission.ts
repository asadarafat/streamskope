interface PendingLoad {
  readonly requestId: string;
  readonly settled: Promise<boolean>;
  readonly resolve: (suppressed: boolean) => void;
  cancelled: boolean;
  dispatched: boolean;
}
export interface LocatorLoadAdmission {
  enter(): boolean;
  finish(cleanupConfirmed?: boolean): void;
}
export interface LocatorCancelAdmission {
  readonly suppressed: Promise<boolean>;
  finish(): void;
}

/** Fences pending keyed reloads without serializing cancellation behind the read. */
export class RecordLocatorAdmission {
  private readonly pending = new Set<PendingLoad>();
  private readonly cancelling = new Map<string, number>();
  private readonly rejected = new Set<string>();
  private readonly capacity = 16;

  load(requestId: string): LocatorLoadAdmission | undefined {
    this.rejected.delete(requestId);
    if (this.pending.size >= this.capacity || this.cancelling.has(requestId)) {
      this.reject(requestId);
      return undefined;
    }
    let resolve!: (suppressed: boolean) => void;
    const promise = new Promise<boolean>((settle) => {
      resolve = settle;
    });
    const token: PendingLoad = {
      requestId,
      settled: promise,
      resolve,
      cancelled: false,
      dispatched: false,
    };
    this.pending.add(token);
    return {
      enter: (): boolean => {
        if (token.cancelled) return false;
        token.dispatched = true;
        this.rejected.delete(requestId);
        return true;
      },
      finish: (cleanupConfirmed = false): void => {
        const noReader = !token.dispatched || cleanupConfirmed;
        token.resolve(noReader);
        if (noReader) this.reject(requestId);
        this.pending.delete(token);
      },
    };
  }
  /** Bounded proof that a parsed request owns no remaining reader. */
  reject(requestId: string): void {
    this.rejected.delete(requestId);
    this.rejected.add(requestId);
    if (this.rejected.size > this.capacity)
      this.rejected.delete(this.rejected.values().next().value!);
  }
  cancel(requestId: string): LocatorCancelAdmission | undefined {
    if (!this.cancelling.has(requestId) && this.cancelling.size >= this.capacity) return undefined;
    this.cancelling.set(requestId, (this.cancelling.get(requestId) ?? 0) + 1);
    const matches = [...this.pending].filter((entry) => entry.requestId === requestId);
    const rejected = this.rejected.has(requestId);
    for (const token of matches) if (!token.dispatched) token.cancelled = true;
    let finished = false;
    return {
      suppressed: Promise.all(matches.map((entry) => entry.settled)).then((outcomes) =>
        outcomes.length > 0 ? outcomes.every(Boolean) : rejected,
      ),
      finish: (): void => {
        if (finished) return;
        finished = true;
        const remaining = (this.cancelling.get(requestId) ?? 1) - 1;
        if (remaining === 0) this.cancelling.delete(requestId);
        else this.cancelling.set(requestId, remaining);
      },
    };
  }
  revoke(): void {
    for (const token of this.pending) if (!token.dispatched) token.cancelled = true;
  }
}
