import type { ProviderWireEvent } from "../providers/host";

import type {
  ProviderDeliveryLease,
  ProviderDeliveryQueue,
} from "./accounted-provider-event-queue";

export interface ProviderSseWritable {
  readonly destroyed: boolean;
  readonly writableEnded: boolean;
  write(chunk: string, callback: (error?: Error | null) => void): boolean;
  end(): unknown;
  on(event: "drain" | "close", listener: () => void): unknown;
  on(event: "error", listener: (error: Error) => void): unknown;
  off(event: "drain" | "close", listener: () => void): unknown;
  off(event: "error", listener: (error: Error) => void): unknown;
}

export interface ProviderSseCloseReport {
  readonly cleanupFailures: readonly unknown[];
  readonly deliveryFailure?: unknown;
}

interface LocalWrite {
  readonly lease?: ProviderDeliveryLease<ProviderWireEvent>;
  completed: boolean;
  returned: boolean;
  drained: boolean;
}

/** One local socket write owns its lease until completion; drain separately grants capacity. */
export class ProviderSseDelivery {
  private closed = false;
  private started = false;
  private blocked = false;
  private pumping = false;
  private commentPending = true;
  private currentWrite: LocalWrite | undefined;
  private deadline: ReturnType<typeof setTimeout> | undefined;
  private readonly attached = new Set<"drain" | "close" | "error">();

  constructor(
    private readonly options: {
      readonly queue: ProviderDeliveryQueue<ProviderWireEvent>;
      readonly response: ProviderSseWritable;
      readonly maxEventBytes: number;
      readonly writeTimeoutMs: number;
      readonly onClose: (report: ProviderSseCloseReport) => void;
    },
  ) {
    for (const value of [options.maxEventBytes, options.writeTimeoutMs]) {
      if (!Number.isSafeInteger(value) || value <= 0)
        throw new Error("HTTP event delivery bounds must be positive safe integers.");
    }
  }

  start(): void {
    if (this.started || this.closed) return;
    this.started = true;
    try {
      this.options.response.on("drain", this.onDrain);
      this.attached.add("drain");
      this.options.response.on("close", this.onResponseClose);
      this.attached.add("close");
      this.options.response.on("error", this.onError);
      this.attached.add("error");
      this.pump();
    } catch (error) {
      this.close(error);
    }
  }

  enqueue(event: ProviderWireEvent): void {
    if (this.closed) return;
    try {
      const failure = this.options.queue.enqueue(event);
      if (failure !== undefined) {
        this.close(failure);
        return;
      }
      if (this.started) this.pump();
    } catch (error) {
      this.close(error);
    }
  }

  close(deliveryFailure?: unknown): void {
    if (this.closed) return;
    this.closed = true;
    this.clearDeadline();
    this.currentWrite = undefined;
    const cleanupFailures: unknown[] = [];
    const attempt = (operation: () => unknown): void => {
      try {
        operation();
      } catch (error) {
        cleanupFailures.push(error);
      }
    };
    attempt(() => this.options.queue.close());
    if (this.attached.has("drain")) attempt(() => this.options.response.off("drain", this.onDrain));
    if (this.attached.has("close"))
      attempt(() => this.options.response.off("close", this.onResponseClose));
    if (this.attached.has("error")) attempt(() => this.options.response.off("error", this.onError));
    this.attached.clear();
    attempt(() => {
      if (!this.options.response.destroyed && !this.options.response.writableEnded)
        this.options.response.end();
    });
    // Route removal/confirmed stop must still run when an independent local cleanup throws.
    this.options.onClose({
      cleanupFailures,
      ...(deliveryFailure === undefined ? {} : { deliveryFailure }),
    });
  }

  private readonly onResponseClose = (): void => this.close();
  private readonly onError = (error: Error): void => this.close(error);
  private readonly onDrain = (): void => {
    if (this.closed) return;
    if (this.currentWrite !== undefined) this.currentWrite.drained = true;
    this.blocked = false;
    if (this.currentWrite === undefined) this.clearDeadline();
    this.pump();
  };

  private pump(): void {
    if (this.closed || this.pumping) return;
    this.pumping = true;
    try {
      while (!this.closed && !this.blocked && this.currentWrite === undefined) {
        if (this.commentPending) {
          this.commentPending = false;
          this.write(": streamskope development host ready\n\n");
          continue;
        }
        const next = this.options.queue.begin();
        if (next.kind === "empty") return;
        if (next.kind === "failure") {
          this.close(next.reason);
          return;
        }
        const encoded = `data: ${JSON.stringify(next.lease.event)}\n\n`;
        if (Buffer.byteLength(encoded) > this.options.maxEventBytes) {
          this.close("event-byte-limit");
          return;
        }
        this.write(encoded, next.lease);
      }
    } catch (error) {
      this.close(error);
    } finally {
      this.pumping = false;
    }
  }

  private write(encoded: string, lease?: ProviderDeliveryLease<ProviderWireEvent>): void {
    const write: LocalWrite = {
      completed: false,
      returned: false,
      drained: false,
      ...(lease === undefined ? {} : { lease }),
    };
    this.currentWrite = write;
    // This client owns the deadline until both local completion and required drain, or close.
    this.deadline = setTimeout(() => this.close("write-timeout"), this.options.writeTimeoutMs);
    this.deadline.unref();
    const accepted = this.options.response.write(encoded, (error): void => {
      if (this.closed || this.currentWrite !== write || write.completed) return;
      write.completed = true;
      if (error !== undefined && error !== null) {
        this.close(error);
        return;
      }
      try {
        write.lease?.complete();
        this.finishWrite(write);
      } catch (failure) {
        this.close(failure);
      }
    });
    if (this.closed || this.currentWrite !== write) return;
    write.returned = true;
    this.blocked = !accepted && !write.drained;
    this.finishWrite(write);
  }

  private finishWrite(write: LocalWrite): void {
    if (!write.returned || !write.completed || this.currentWrite !== write || this.closed) return;
    this.currentWrite = undefined;
    if (!this.blocked) this.clearDeadline();
    this.pump();
  }

  private clearDeadline(): void {
    clearTimeout(this.deadline);
    this.deadline = undefined;
  }
}
