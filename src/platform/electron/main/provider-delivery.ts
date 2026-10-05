import type { ProviderWireEvent } from "../../providers/host";
import {
  AccountedProviderEventQueue,
  createControlDeliveryPolicy,
  type DeliveryFailure,
  type ProviderDeliveryLease,
  type ProviderDeliveryLimits,
  type ProviderDeliveryQueue,
} from "../../node/accounted-provider-event-queue";

export interface ElectronProviderDelivery {
  enqueue(event: ProviderWireEvent): void;
  acknowledge(sequence: number): void;
  close(): void;
}

export type ElectronDeliveryFailure = DeliveryFailure | "ack-timeout" | "send-failed";

export interface ElectronProviderDeliveryBinding {
  readonly id: string;
  create(
    send: (event: ProviderWireEvent) => void,
    fail: (reason: string) => void,
  ): ElectronProviderDelivery;
  /** Safe presentation only; the selected endpoint owns actual stream cleanup. */
  recoveryInstruction?(confirmed: boolean, reason: string): string;
}

interface AcknowledgedDeliveryOptions<Event extends ProviderWireEvent> {
  readonly queue: ProviderDeliveryQueue<Event>;
  readonly limits: ProviderDeliveryLimits;
  readonly send: (event: Event) => void;
  readonly onFailure: (reason: ElectronDeliveryFailure) => void;
  readonly onPressure?: (paused: boolean) => void;
}

/** Native ownership is one exact queue lease until its matching renderer ACK. */
export class ElectronAcknowledgedEventDelivery<Event extends ProviderWireEvent> {
  private inflight: ProviderDeliveryLease<Event> | undefined;
  private closed = false;
  private paused = false;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private closeFailed = false;
  private closeFailure: unknown;

  constructor(private readonly options: AcknowledgedDeliveryOptions<Event>) {}

  enqueue(event: Event): void {
    if (this.closed) return;
    try {
      const failure = this.options.queue.enqueue(event);
      if (failure !== undefined) {
        this.fail(failure);
        return;
      }
      this.updatePressure();
      this.flush();
    } catch {
      this.fail("event-validation");
    }
  }

  acknowledge(sequence: number): void {
    if (this.closed || this.inflight?.event.sequence !== sequence) return;
    try {
      const completed = this.inflight;
      this.inflight = undefined;
      clearTimeout(this.timer);
      this.timer = undefined;
      completed.complete();
      this.updatePressure();
      this.flush();
    } catch {
      this.fail("event-validation");
    }
  }

  close(): void {
    if (this.closed) {
      if (this.closeFailed) throw this.closeFailure;
      return;
    }
    this.closed = true;
    clearTimeout(this.timer);
    this.timer = undefined;
    this.inflight = undefined;
    this.options.queue.close();
    if (this.paused) {
      this.paused = false;
      try {
        this.options.onPressure?.(false);
      } catch (cause) {
        this.closeFailed = true;
        this.closeFailure = cause;
        throw cause;
      }
    }
  }

  private updatePressure(): void {
    const { events, serializedBytes, records } = this.options.queue.costs;
    const { maxEvents, maxSerializedBytes, maxRecords } = this.options.limits;
    const next = this.paused
      ? !(
          events <= maxEvents / 4 &&
          serializedBytes <= maxSerializedBytes / 4 &&
          records <= maxRecords / 4
        )
      : events >= maxEvents / 2 ||
        serializedBytes >= maxSerializedBytes / 2 ||
        records >= maxRecords / 2;
    if (next === this.paused) return;
    this.paused = next;
    this.options.onPressure?.(next);
  }

  private fail(reason: ElectronDeliveryFailure): void {
    if (this.closed) return;
    try {
      this.close();
    } catch {
      // Route cleanup receives the retained close failure; stop must still be attempted.
    }
    try {
      this.options.onFailure(reason);
    } catch {
      // Native asynchronous callbacks cannot propagate a failing recovery observer.
    }
  }

  private flush(): void {
    if (this.closed || this.inflight !== undefined) return;
    const next = this.options.queue.begin();
    if (next.kind === "empty") return;
    if (next.kind === "failure") {
      this.fail(next.reason);
      return;
    }
    this.inflight = next.lease;
    this.timer = setTimeout(() => this.fail("ack-timeout"), 30_000);
    try {
      this.options.send(next.lease.event);
    } catch {
      this.fail("send-failed");
    }
  }
}

/** Control providers use the same accounted lease and native ACK lifetime. */
export class ElectronControlEventDelivery extends ElectronAcknowledgedEventDelivery<ProviderWireEvent> {
  constructor(send: (event: ProviderWireEvent) => void, onFailure: (reason: string) => void) {
    const limits = { maxEvents: 64, maxSerializedBytes: 8 * 1024 * 1024, maxRecords: 1000 };
    super({
      queue: new AccountedProviderEventQueue({
        limits,
        overflow: "reject",
        policy: createControlDeliveryPolicy<ProviderWireEvent>(),
      }),
      limits,
      send,
      onFailure: (reason): void =>
        onFailure(
          reason === "event-limit" || reason === "byte-limit" ? "control-event-limit" : reason,
        ),
    });
  }
}
