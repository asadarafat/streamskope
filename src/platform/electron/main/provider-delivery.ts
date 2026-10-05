import type { ProviderWireEvent } from "../../providers/host";

export interface ElectronProviderDelivery {
  enqueue(event: ProviderWireEvent): void;
  acknowledge(sequence: number): void;
  close(): void;
}

export interface ElectronProviderDeliveryBinding {
  readonly id: string;
  create(
    send: (event: ProviderWireEvent) => void,
    fail: (reason: string) => void,
  ): ElectronProviderDelivery;
  failureRecovery(reason: string): Promise<string>;
}

/** Bounded control events for providers without a record-delivery policy. */
export class ElectronControlEventDelivery implements ElectronProviderDelivery {
  private readonly pending: Array<{ event: ProviderWireEvent; bytes: number }> = [];
  private inflight: { event: ProviderWireEvent; bytes: number } | undefined;
  private bytes = 0;
  private closed = false;
  private timer: ReturnType<typeof setTimeout> | undefined;

  constructor(
    private readonly send: (event: ProviderWireEvent) => void,
    private readonly onFailure: (reason: string) => void,
  ) {}

  enqueue(event: ProviderWireEvent): void {
    if (this.closed) return;
    const bytes = Buffer.byteLength(JSON.stringify(event));
    if (
      this.pending.length + Number(this.inflight !== undefined) >= 64 ||
      this.bytes + bytes > 8 * 1024 * 1024
    ) {
      this.fail("control-event-limit");
      return;
    }
    this.pending.push({ event, bytes });
    this.bytes += bytes;
    this.flush();
  }
  acknowledge(sequence: number): void {
    if (this.closed || this.inflight?.event.sequence !== sequence) return;
    this.bytes -= this.inflight.bytes;
    this.inflight = undefined;
    clearTimeout(this.timer);
    this.timer = undefined;
    this.flush();
  }
  close(): void {
    this.closed = true;
    clearTimeout(this.timer);
    this.timer = undefined;
    this.pending.length = 0;
    this.inflight = undefined;
    this.bytes = 0;
  }
  private fail(reason: string): void {
    if (this.closed) return;
    this.close();
    this.onFailure(reason);
  }
  private flush(): void {
    if (this.closed || this.inflight !== undefined) return;
    this.inflight = this.pending.shift();
    if (this.inflight === undefined) return;
    this.timer = setTimeout(() => this.fail("ack-timeout"), 30_000);
    try {
      this.send(this.inflight.event);
    } catch {
      this.fail("send-failed");
    }
  }
}
