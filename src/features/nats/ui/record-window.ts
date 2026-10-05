import { natsRecordRetainedBytes, type NatsRecord } from "../contracts";

/** Viewer retention is independent of the host queue and transport loss accounting. */
export const NATS_VIEWER_LIMITS = { records: 1000, bytes: 8 * 1024 * 1024 } as const;
export interface NatsRecordWindow {
  readonly records: readonly NatsRecord[];
  readonly bytes: number;
  readonly evictedRecords: number;
}
export function emptyNatsRecordWindow(): NatsRecordWindow {
  return { records: [], bytes: 0, evictedRecords: 0 };
}
export function appendNatsRecords(
  window: NatsRecordWindow,
  incoming: readonly NatsRecord[],
  limits: { readonly records: number; readonly bytes: number } = NATS_VIEWER_LIMITS,
): NatsRecordWindow {
  const combined = [...window.records, ...incoming];
  let bytes =
    window.bytes + incoming.reduce((total, record) => total + natsRecordRetainedBytes(record), 0);
  let first = 0;
  while (
    first < combined.length &&
    (combined.length - first > limits.records || bytes > limits.bytes)
  ) {
    bytes -= natsRecordRetainedBytes(combined[first]!);
    first += 1;
  }
  return { records: combined.slice(first), bytes, evictedRecords: window.evictedRecords + first };
}
