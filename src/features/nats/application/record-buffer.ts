import {
  NATS_LIMITS,
  NATS_PROTOCOL_VERSION,
  natsRecordRetainedBytes,
  natsUtf8Bytes,
  type NatsRecord,
  type NatsRecordsBatch,
  type NatsSubscriptionCounters,
} from "../contracts";
import { natsIdentifier } from "../contracts/validation-primitives";

import type { NatsMessageReceipt } from "./engine-port";
import { NatsOperationError } from "./failure";

export function emptyNatsCounters(): NatsSubscriptionCounters {
  return {
    receivedRecords: 0,
    applicationOmittedRecords: 0,
    publishedRecords: 0,
    queuedRecords: 0,
    queuedBytes: 0,
    transportOmittedRecords: 0,
  };
}
interface RetainedRecord {
  readonly record: NatsRecord;
  readonly bytes: number;
}
function increment(value: number, amount = 1): number {
  const next = value + amount;
  if (!Number.isSafeInteger(next))
    throw new NatsOperationError({
      code: "unavailable",
      summary: "NATS capture counters reached their safe limit.",
      recovery: "Start a new subscription after cleanup completes.",
    });
  return next;
}
/** Reserve the largest legal wire envelope, including every counter and context field. */
function batchEnvelopeBytes(generation: string): number {
  const maximum = Number.MAX_SAFE_INTEGER;
  return natsUtf8Bytes(
    JSON.stringify({
      version: NATS_PROTOCOL_VERSION,
      sequence: maximum,
      event: "records.batch",
      operation: "connection.disconnect",
      correlationId: "x".repeat(NATS_LIMITS.identifierCharacters),
      payload: {
        generation,
        records: [],
        counters: {
          receivedRecords: maximum,
          applicationOmittedRecords: maximum,
          publishedRecords: maximum,
          queuedRecords: NATS_LIMITS.queuedRecords,
          queuedBytes: NATS_LIMITS.queuedBytes,
          transportOmittedRecords: maximum,
        },
      },
    }),
  );
}

/** Owns only copied records; local omissions and publication remain separate from delivery. */
export class NatsRecordBuffer {
  private readonly pending: RetainedRecord[] = [];
  private readonly envelopeBytes: number;
  private queuedBytes = 0;
  private receivedRecords = 0;
  private applicationOmittedRecords = 0;
  private publishedRecords = 0;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private closed = false;
  private presenting = false;
  constructor(
    readonly generation: string,
    private readonly publish: (batch: NatsRecordsBatch) => void,
    private readonly fail: (error: unknown) => void,
  ) {
    natsIdentifier(generation);
    if (
      generation.length + 1 + String(Number.MAX_SAFE_INTEGER).length >
      NATS_LIMITS.identifierCharacters
    )
      throw new NatsOperationError({
        code: "validation",
        summary: "The NATS subscription generation is invalid.",
      });
    this.envelopeBytes = batchEnvelopeBytes(generation);
  }
  counters(): NatsSubscriptionCounters {
    return {
      receivedRecords: this.receivedRecords,
      applicationOmittedRecords: this.applicationOmittedRecords,
      publishedRecords: this.publishedRecords,
      queuedRecords: this.pending.length,
      queuedBytes: this.queuedBytes,
      transportOmittedRecords: 0,
    };
  }
  accept(receipt: NatsMessageReceipt): void {
    if (this.closed) return;
    this.receivedRecords = increment(this.receivedRecords);
    if (receipt.kind === "omitted") {
      this.applicationOmittedRecords = increment(this.applicationOmittedRecords);
    } else {
      const record: NatsRecord = {
        ...receipt.record,
        generation: this.generation,
        id: `${this.generation}.${this.receivedRecords}`,
      };
      const bytes = natsRecordRetainedBytes(record);
      if (bytes + this.envelopeBytes > NATS_LIMITS.batchBytes || bytes > NATS_LIMITS.queuedBytes) {
        this.applicationOmittedRecords = increment(this.applicationOmittedRecords);
      } else {
        while (
          this.pending.length >= NATS_LIMITS.queuedRecords ||
          this.queuedBytes + bytes > NATS_LIMITS.queuedBytes
        ) {
          const removed = this.pending.shift();
          if (removed === undefined) break;
          this.queuedBytes -= removed.bytes;
          this.applicationOmittedRecords = increment(this.applicationOmittedRecords);
        }
        this.pending.push({ record, bytes });
        this.queuedBytes += bytes;
      }
    }
    if (this.presenting) this.schedule();
  }
  startPublishing(): void {
    if (this.closed) return;
    this.presenting = true;
    if (this.receivedRecords > 0) this.schedule();
  }
  close(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
    this.applicationOmittedRecords = increment(this.applicationOmittedRecords, this.pending.length);
    this.pending.length = 0;
    this.queuedBytes = 0;
  }
  private schedule(): void {
    if (this.timer !== undefined) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      try {
        this.flush();
      } catch (error) {
        this.fail(error);
      }
    }, 25);
  }
  private flush(): void {
    // An empty receipt still publishes counters through an empty batch.
    do {
      let count = 0;
      let bytes = this.envelopeBytes;
      let retainedBytes = 0;
      for (const entry of this.pending) {
        const addition = entry.bytes + (count === 0 ? 0 : 1);
        if (count >= NATS_LIMITS.batchRecords || bytes + addition > NATS_LIMITS.batchBytes) break;
        count += 1;
        bytes += addition;
        retainedBytes += entry.bytes;
      }
      const records = this.pending.splice(0, count).map((entry) => entry.record);
      this.queuedBytes -= retainedBytes;
      this.publishedRecords = increment(this.publishedRecords, count);
      this.publish({ generation: this.generation, records, counters: this.counters() });
    } while (!this.closed && this.pending.length > 0);
  }
}
