import {
  KAFKA_MESSAGE_LIMITS,
  KAFKA_QUERY_LIMITS,
  compileKafkaSearchFilter,
  type KafkaReadCoverage,
  type KafkaMessage,
  type KafkaReadReason,
} from "../contracts";

import type { KafkaFetchPlan } from "./fetch-plan";
import type { KafkaRawMessage } from "./types";

/** Tracks traversed offsets, not an estimate based on the number of returned matches. */
export class KafkaReadTracker {
  private readonly predicate: ReturnType<typeof compileKafkaSearchFilter> | undefined;
  private reason: KafkaReadReason = "reading";
  private scannedRecords = 0;
  private scannedBytes = 0;
  private matchedRecords = 0;
  private unavailableRecords = 0;
  private readonly next: Map<number, bigint>;
  private pending:
    | {
        readonly message: KafkaRawMessage;
        readonly before: KafkaReadCoverage;
        advancedWithoutAcknowledgement: boolean;
      }
    | undefined;

  constructor(private readonly plan: KafkaFetchPlan) {
    this.predicate =
      plan.request.search === undefined ? undefined : compileKafkaSearchFilter(plan.request.search);
    this.next = new Map(plan.startOffsets);
    this.checkRangeComplete();
  }

  get finished(): boolean {
    return this.reason !== "reading";
  }

  finish(reason: KafkaReadReason): void {
    if (!this.finished || reason === "failed") this.reason = reason;
  }

  snapshot(): KafkaReadCoverage {
    return {
      reason: this.reason,
      scannedRecords: this.scannedRecords,
      scannedBytes: this.scannedBytes,
      matchedRecords: this.matchedRecords,
      unavailableRecords: this.unavailableRecords,
      partitions: [...this.plan.startOffsets].map(([partition, start]) => ({
        partition,
        startOffset: String(start),
        endOffset: String(this.plan.endOffsets?.get(partition) ?? start),
        nextOffset: String(this.next.get(partition) ?? start),
      })),
    };
  }

  /** A yielded record is not a delivered record until the owning session accepts it. */
  acknowledge(message: KafkaRawMessage): void {
    if (this.pending?.message === message && !this.pending.advancedWithoutAcknowledgement)
      this.pending = undefined;
  }

  checkpoint(): KafkaReadCoverage {
    const coverage = this.pending?.before ?? this.snapshot();
    return {
      ...coverage,
      reason:
        this.pending !== undefined && this.reason === "range-complete" ? "cancelled" : this.reason,
    };
  }

  accept(message: KafkaRawMessage, prepared?: KafkaMessage): boolean {
    if (this.finished || message.topic !== this.plan.request.topic) return false;
    const next = this.next.get(message.partition);
    const end = this.plan.endOffsets?.get(message.partition);
    if (next === undefined || end === undefined || message.offset < next) return false;
    // The owner acknowledges before requesting another item. Late acknowledgements
    // cannot release a checkpoint past later records that have not been accepted.
    if (this.pending !== undefined) this.pending.advancedWithoutAcknowledgement = true;
    if (message.offset >= end) {
      this.next.set(message.partition, end);
      this.checkRangeComplete();
      return false;
    }
    const bytes =
      (message.key?.byteLength ?? 0) +
      (message.value?.byteLength ?? 0) +
      (message.headerEntries ?? [...message.headers]).reduce(
        (total, [key, value]) => total + key.byteLength + (value?.byteLength ?? 0),
        0,
      );
    if (this.scannedBytes + bytes > KAFKA_QUERY_LIMITS.scanBytes) {
      this.finish("byte-limit");
      return false;
    }
    const request = this.plan.request;
    const inTime =
      request.mode !== "time-window" ||
      (message.timestamp >= BigInt(request.startTimeMs) &&
        message.timestamp < BigInt(request.endTimeMs));
    let match = inTime;
    if (match && request.search !== undefined) {
      const filter = request.search;
      // An omitted large field is unknown, never proof that the record did not match.
      if (
        ((filter.value.trim().length > 0 || (filter.expression?.trim().length ?? 0) > 0) &&
          (message.value?.byteLength ?? 0) > KAFKA_MESSAGE_LIMITS.messageBytes) ||
        (filter.key.trim().length > 0 &&
          (message.key?.byteLength ?? 0) > KAFKA_MESSAGE_LIMITS.messageBytes)
      ) {
        this.unavailableRecords += 1;
        match = false;
      } else {
        const result = this.predicate?.(
          prepared ?? {
            key: filter.key.trim().length === 0 ? null : (message.key?.toString("utf8") ?? null),
            payload:
              filter.value.trim().length === 0 && (filter.expression?.trim().length ?? 0) === 0
                ? null
                : (message.value?.toString("utf8") ?? null),
            offset: String(message.offset),
            partition: message.partition,
            timestamp: new Date(Number(message.timestamp)).toISOString(),
          },
        );
        match = result === "matched";
        if (result === "unavailable") this.unavailableRecords += 1;
      }
    }
    if (match) {
      if (this.pending === undefined)
        this.pending = { message, before: this.snapshot(), advancedWithoutAcknowledgement: false };
      this.matchedRecords += 1;
    }
    this.next.set(message.partition, message.offset + 1n);
    this.scannedRecords += 1;
    this.scannedBytes += bytes;
    this.checkRangeComplete();
    if (this.matchedRecords >= this.plan.maxMessages) this.finish("result-limit");
    if (this.scannedRecords >= KAFKA_QUERY_LIMITS.scanRecords) this.finish("scan-limit");
    return match;
  }

  private checkRangeComplete(): void {
    if (
      this.plan.endOffsets !== null &&
      [...this.plan.endOffsets].every(
        ([partition, end]) => (this.next.get(partition) ?? -1n) >= end,
      )
    )
      this.finish("range-complete");
  }
}
