import type { HostEvent } from "../../src/features/kafka/contracts";

/** Bounded accounting for the replay's single consumption operation. */
export class StreamReplayAccounting {
  private accepted = 0;
  private published = 0;
  private dropped = 0;
  private queued = 0;

  observe(event: HostEvent): void {
    if (event.event === "messages.batch") {
      this.published += event.payload.messages.length;
      this.dropped = Math.max(this.dropped, event.payload.droppedMessages);
    } else if (event.event === "consumption.state") {
      this.accepted = Math.max(this.accepted, event.payload.receivedMessages);
      this.dropped = Math.max(this.dropped, event.payload.droppedMessages);
    } else if (event.event === "streamMetrics.changed") {
      this.accepted = Math.max(this.accepted, event.payload.delivery?.receivedMessages ?? 0);
      this.dropped = Math.max(this.dropped, event.payload.queue?.droppedMessages ?? 0);
      this.queued = event.payload.queue?.currentMessages ?? this.queued;
    }
  }

  snapshot(generated: number): {
    readonly accepted: number;
    readonly published: number;
    readonly hostDisplayDrops: number;
    readonly queued: number;
    readonly unacceptedGenerated: number;
    readonly accountingPassed: boolean;
  } {
    // A sequential async iterator can have one yielded record awaiting acceptance
    // when Stop invalidates its owner. Larger gaps are accounting failures.
    const unacceptedGenerated = generated - this.accepted;
    return {
      accepted: this.accepted,
      published: this.published,
      hostDisplayDrops: this.dropped,
      queued: this.queued,
      unacceptedGenerated,
      accountingPassed:
        this.queued === 0 &&
        this.accepted === this.published + this.dropped &&
        unacceptedGenerated >= 0 &&
        unacceptedGenerated <= 1,
    };
  }
}
