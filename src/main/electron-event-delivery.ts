import { KAFKA_MESSAGE_LIMITS, type HostEvent } from "../kafka/contracts";

const MAX_EVENTS = 64;
const MAX_BYTES = 8 * 1024 * 1024;
const ACK_TIMEOUT_MS = 30000;

interface PendingEvent {
  readonly event: HostEvent;
  readonly bytes: number;
  readonly records: number;
}

type DeliveryFailure =
  "event-limit" | "byte-limit" | "record-limit" | "ack-timeout" | "send-failed";

/** One acknowledged IPC event in flight; queued budgets include that event. */
export class ElectronEventDelivery {
  private readonly pending: PendingEvent[] = [];
  private inflight: PendingEvent | undefined;
  private bytes = 0;
  private records = 0;
  private closed = false;
  private paused = false;
  private timer: ReturnType<typeof setTimeout> | undefined;

  constructor(
    private readonly send: (event: HostEvent) => void,
    private readonly onFailure: (reason: DeliveryFailure) => void,
    private readonly onPressure: (paused: boolean) => void = () => undefined,
  ) {}

  enqueue(event: HostEvent): void {
    if (this.closed) return;
    // Serialized-byte budget, not a claim about V8 heap or zero-copy transport.
    const bytes = Buffer.byteLength(JSON.stringify(event));
    const records = event.event === "messages.batch" ? event.payload.messages.length : 0;
    if (
      this.pending.length + Number(this.inflight !== undefined) >= MAX_EVENTS ||
      this.bytes + bytes > MAX_BYTES ||
      this.records + records > KAFKA_MESSAGE_LIMITS.queuedMessages
    ) {
      this.fail(
        this.pending.length + Number(this.inflight !== undefined) >= MAX_EVENTS
          ? "event-limit"
          : this.bytes + bytes > MAX_BYTES
            ? "byte-limit"
            : "record-limit",
      );
      return;
    }
    this.pending.push({ event, bytes, records });
    this.bytes += bytes;
    this.records += records;
    this.updatePressure();
    this.flush();
  }

  acknowledge(sequence: number): void {
    if (this.closed || this.inflight?.event.sequence !== sequence) return;
    this.bytes -= this.inflight.bytes;
    this.records -= this.inflight.records;
    this.inflight = undefined;
    clearTimeout(this.timer);
    this.timer = undefined;
    this.updatePressure();
    this.flush();
  }

  close(): void {
    this.closed = true;
    clearTimeout(this.timer);
    this.timer = undefined;
    this.pending.length = 0;
    this.inflight = undefined;
    this.bytes = 0;
    this.records = 0;
    if (this.paused) {
      this.paused = false;
      this.onPressure(false);
    }
  }

  private updatePressure(): void {
    const events = this.pending.length + Number(this.inflight !== undefined);
    const next = this.paused
      ? !(
          events <= MAX_EVENTS / 4 &&
          this.bytes <= MAX_BYTES / 4 &&
          this.records <= KAFKA_MESSAGE_LIMITS.queuedMessages / 4
        )
      : events >= MAX_EVENTS / 2 ||
        this.bytes >= MAX_BYTES / 2 ||
        this.records >= KAFKA_MESSAGE_LIMITS.queuedMessages / 2;
    if (next === this.paused) return;
    this.paused = next;
    this.onPressure(next);
  }

  private fail(reason: DeliveryFailure): void {
    if (this.closed) return;
    this.close();
    this.onFailure(reason);
  }

  private flush(): void {
    if (this.closed || this.inflight !== undefined) return;
    this.inflight = this.pending.shift();
    if (this.inflight === undefined) return;
    this.timer = setTimeout(() => this.fail("ack-timeout"), ACK_TIMEOUT_MS);
    try {
      this.send(this.inflight.event);
    } catch {
      this.fail("send-failed");
    }
  }
}
