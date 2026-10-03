import { expect, it, vi } from "vitest";

import type { KafkaMessage } from "../../src/features/kafka/contracts";
import type { KafkaActiveConnection } from "../../src/features/kafka/application";
import { sampleObservationRecords } from "../../src/features/kafka/application/observation-record-sample";
import { parseObservationRecords } from "../../src/features/kafka/contracts/observation-records";
import { analyzeObservations } from "../../src/features/kafka/contracts/observation-analysis";
import { observation, observationSeries, OBSERVED_AT } from "../support/observation-fixture";
const end = OBSERVED_AT;
function record(index: number): KafkaMessage {
  return {
    id: String(index),
    topic: "events",
    partition: index % 2,
    offset: String(index),
    key: "private key",
    payload: "private payload",
    preview: "private preview",
    headers: {},
    timestamp: new Date(end - 1000).toISOString(),
    originalByteSize: 100,
    truncated: false,
    original: {
      state: "complete",
      encoding: "base64",
      key: "c2VjcmV0",
      value: "dmFsdWU=",
      headers: [],
    },
  };
}
function source(
  messages: readonly KafkaMessage[],
  complete = true,
): {
  connection: Pick<KafkaActiveConnection, "openMessageStream">;
  close: ReturnType<typeof vi.fn>;
} {
  const close = vi.fn(() => Promise.resolve());
  return {
    close,
    connection: {
      openMessageStream: () =>
        Promise.resolve({
          close,
          coverage: () => ({
            reason: complete ? "range-complete" : "result-limit",
            scannedRecords: messages.length,
            scannedBytes: messages.length * 100,
            matchedRecords: messages.length,
            unavailableRecords: 0,
            partitions: [],
          }),
          async *[Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
            for (const message of messages) yield await Promise.resolve(message);
          },
        }),
    },
  };
}
it("aggregates seeded hot keys and sizes without retaining protected bytes or payload previews", async () => {
  const f = source(Array.from({ length: 40 }, (_, i) => record(i)));
  const sample = await sampleObservationRecords(
    f.connection,
    "events",
    end,
    new AbortController().signal,
  );
  expect(sample).toMatchObject({
    state: "complete",
    count: 40,
    bytes: 4000,
    meanBytes: 100,
    p95Bytes: 100,
    knownKeys: 40,
    distinctKeys: 1,
    topKeys: [{ count: 40, partition: 0, offset: "0" }],
    partitions: [
      { partition: 0, count: 20 },
      { partition: 1, count: 20 },
    ],
  });
  expect(JSON.stringify(sample)).not.toMatch(/private|c2VjcmV0|dmFsdWU/);
  expect(f.close).toHaveBeenCalledOnce();
  const s = observation(0, { records: sample });
  expect(analyzeObservations(observationSeries([s]), s.observedAt).hotKey).toMatchObject({
    share: 1,
    known: 40,
    suspected: true,
  });
});
it("does not infer key identity from masked previews and separates null, unknown and available keys", async () => {
  const withoutOriginal = { ...record(3) };
  Reflect.deleteProperty(withoutOriginal, "original");
  const messages = [
    record(0),
    { ...record(1), original: { state: "unavailable" as const, reason: "masked" as const } },
    {
      ...record(2),
      original: {
        state: "complete" as const,
        encoding: "base64" as const,
        key: null,
        value: null,
        headers: [],
      },
    },
    withoutOriginal,
  ];
  const sample = await sampleObservationRecords(
    source(messages).connection,
    "events",
    end,
    new AbortController().signal,
  );
  expect(sample).toMatchObject({
    count: 4,
    knownKeys: 1,
    nullKeys: 1,
    unavailableKeys: 2,
    distinctKeys: 1,
  });
  expect(() => parseObservationRecords({ ...sample, rawKeys: ["secret"] })).toThrow();
});
it("enforces record and byte bounds, excludes duplicates/out-of-window records, and never labels a capped sample complete", async () => {
  const capped = await sampleObservationRecords(
    source(Array.from({ length: 201 }, (_, i) => record(i))).connection,
    "events",
    end,
    new AbortController().signal,
  );
  expect(capped).toMatchObject({ count: 200, state: "partial", reason: "limit" });
  const bytes = await sampleObservationRecords(
    source([
      { ...record(0), originalByteSize: 1_500_000 },
      { ...record(1), originalByteSize: 1_500_000 },
    ]).connection,
    "events",
    end,
    new AbortController().signal,
  );
  expect(bytes).toMatchObject({ count: 1, bytes: 1_500_000, state: "partial", reason: "limit" });
  const deduped = await sampleObservationRecords(
    source([
      record(0),
      record(0),
      { ...record(1), timestamp: new Date(end).toISOString() },
      { ...record(2), topic: "other" },
    ]).connection,
    "events",
    end,
    new AbortController().signal,
  );
  expect(deduped.count).toBe(1);
});
it("closes its own reader on cancellation and does not return a successful partial cancellation result", async () => {
  const controller = new AbortController();
  const close = vi.fn(() => Promise.resolve());
  const connection: Pick<KafkaActiveConnection, "openMessageStream"> = {
    openMessageStream: () =>
      Promise.resolve({
        close,
        async *[Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
          yield await Promise.resolve(record(0));
          controller.abort();
          yield await Promise.resolve(record(1));
        },
      }),
  };
  await expect(
    sampleObservationRecords(connection, "events", end, controller.signal),
  ).rejects.toThrow();
  expect(close).toHaveBeenCalled();
  const unavailable = await sampleObservationRecords(
    {
      openMessageStream: () => Promise.reject(new Error("private endpoint error")),
    },
    "events",
    end,
    new AbortController().signal,
  );
  expect(unavailable).toMatchObject({ state: "unavailable", count: 0, reason: "read-failed" });
  expect(JSON.stringify(unavailable)).not.toContain("private endpoint");
});
