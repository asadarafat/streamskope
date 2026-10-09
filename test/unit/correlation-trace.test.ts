import { expect, it } from "vitest";

import { traceCorrelation } from "../../src/features/kafka/application/correlation-trace-service";
import { matchCorrelation } from "../../src/features/kafka/application/correlation-selector";
import {
  parseCorrelationTraceInput,
  parseCorrelationTraceResult,
  type CorrelationTraceInput,
} from "../../src/features/kafka/contracts/correlation-trace";
import type { KafkaMessage, KafkaReadCoverage } from "../../src/features/kafka/contracts";
import type { KafkaMessageStream } from "../../src/features/kafka/application/types";
import { RecordingActiveConnection } from "../support/kafka-backend-facade-fixture";

const request: CorrelationTraceInput = {
  traceId: "trace",
  topics: ["a", "b"],
  startTimeMs: 1_000,
  endTimeMs: 2_000,
  value: "id-1",
  selector: { source: "header", path: "cid", format: "json" },
};
const message = (topic: string, offset: string, value = "id-1"): KafkaMessage => ({
  id: `${topic}:0:${offset}`,
  topic,
  partition: 0,
  offset,
  timestamp: new Date(1_500).toISOString(),
  key: value,
  payload: JSON.stringify({ cid: value }),
  preview: JSON.stringify({ cid: value }),
  headers: { cid: value },
  originalByteSize: 30,
  truncated: false,
  original: {
    state: "complete",
    encoding: "base64",
    key: btoa(value),
    value: btoa(JSON.stringify({ cid: value })),
    headers: [
      { key: btoa("cid"), value: btoa("wrong") },
      { key: btoa("cid"), value: btoa(value) },
    ],
  },
});
function stream(messages: readonly KafkaMessage[]): KafkaMessageStream & { closed: boolean } {
  let next = 0;
  const result = {
    closed: false,
    close: (): Promise<void> => {
      result.closed = true;
      return Promise.resolve();
    },
    coverage: (): KafkaReadCoverage => ({
      reason: next === messages.length ? "range-complete" : result.closed ? "cancelled" : "reading",
      scannedRecords: next,
      scannedBytes: next * 30,
      matchedRecords: next,
      unavailableRecords: 0,
      partitions: [
        {
          partition: 0,
          startOffset: "0",
          endOffset: String(messages.length),
          nextOffset: String(next),
        },
      ],
    }),
    async *[Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
      await Promise.resolve();
      for (const entry of messages) {
        next++;
        yield entry;
      }
    },
  };
  return result;
}
it("retains duplicate values at distinct source offsets and reports denied and fully searched empty topics separately", async () => {
  const a = stream([message("a", "0"), message("a", "1"), message("a", "2", "other")]);
  const c = stream([]);
  const connection = Object.assign(new RecordingActiveConnection(), {
    openMessageStream: ({ topic }: { topic: string }): Promise<KafkaMessageStream> => {
      if (topic === "b")
        return Promise.reject(Object.assign(new Error("Denied"), { code: "AUTHORIZATION_DENIED" }));
      return Promise.resolve(topic === "a" ? a : c);
    },
  });
  const result = parseCorrelationTraceResult(
    await traceCorrelation(
      connection,
      "Fixture",
      { ...request, topics: ["a", "b", "c"] },
      new AbortController().signal,
    ),
  );
  expect(result.matches.map((entry) => [entry.topic, entry.offset])).toEqual([
    ["a", "0"],
    ["a", "1"],
  ]);
  expect(result.topics.map((entry) => [entry.state, entry.matches])).toEqual([
    ["searched", 2],
    ["denied", 0],
    ["searched", 0],
  ]);
  expect(a.closed && c.closed).toBe(true);
});
it("matches exact keys and payload pointers, preserves numeric precision, and distinguishes unreadable input", async () => {
  const signal = new AbortController().signal;
  expect(
    await matchCorrelation(
      message("a", "0"),
      { ...request, selector: { source: "key", path: "", format: "json" } },
      undefined,
      null,
      signal,
    ),
  ).toBe("matched");
  const original = {
    state: "complete" as const,
    encoding: "base64" as const,
    key: null,
    value: btoa('{"a/b":{"~id":9223372036854775807}}'),
    headers: [],
  };
  const input = {
    ...request,
    value: "9223372036854775807",
    selector: { source: "payload" as const, path: "/a~1b/~0id", format: "json" as const },
  };
  expect(
    await matchCorrelation({ ...message("a", "0"), original }, input, undefined, null, signal),
  ).toBe("matched");
  expect(
    await matchCorrelation(
      { ...message("a", "0"), original: { ...original, value: btoa("invalid") } },
      input,
      undefined,
      null,
      signal,
    ),
  ).toBe("unavailable");
  expect(
    await matchCorrelation(
      { ...message("a", "0"), original: { state: "unavailable", reason: "size-limit" } },
      input,
      undefined,
      null,
      signal,
    ),
  ).toBe("unavailable");
  expect(
    await matchCorrelation(
      { ...message("a", "0"), original: { ...original, value: btoa('{"cid":"id-1suffix"}') } },
      { ...request, selector: { source: "payload", path: "/cid", format: "json" } },
      undefined,
      null,
      signal,
    ),
  ).toBe("different");
});
it("labels malformed records and match-budget exhaustion as partial and keeps unsearched topics visible", async () => {
  const many = stream(Array.from({ length: 205 }, (_, index) => message("a", String(index))));
  const connection = Object.assign(new RecordingActiveConnection(), {
    openMessageStream: (): Promise<KafkaMessageStream> => Promise.resolve(many),
  });
  const result = await traceCorrelation(
    connection,
    "Fixture",
    request,
    new AbortController().signal,
  );
  expect(result.matches).toHaveLength(200);
  expect(result.topics).toMatchObject([
    { state: "partial", reason: "match-limit", matches: 200 },
    { state: "not-searched", reason: "match-limit", matches: 0 },
  ]);
  expect(many.closed).toBe(true);
  const incomplete = stream([
    {
      ...message("a", "0"),
      original: { state: "unavailable" as const, reason: "size-limit" as const },
    },
  ]);
  connection.openMessageStream = (): Promise<KafkaMessageStream> => Promise.resolve(incomplete);
  expect(
    (
      await traceCorrelation(
        connection,
        "Fixture",
        { ...request, topics: ["a"] },
        new AbortController().signal,
      )
    ).topics,
  ).toMatchObject([{ state: "partial", unavailable: 1, reason: "records-unavailable" }]);
});
it("closes a pending stream on cancellation and accounts for topics never started", async () => {
  const controller = new AbortController();
  let stopped = false;
  let wake!: () => void;
  const waiting = new Promise<void>((resolve) => {
    wake = resolve;
  });
  const pending: KafkaMessageStream = {
    close: (): Promise<void> => {
      stopped = true;
      wake();
      return Promise.resolve();
    },
    async *[Symbol.asyncIterator](): AsyncIterator<KafkaMessage> {
      await waiting;
      if (!stopped) yield message("a", "0");
    },
  };
  const connection = Object.assign(new RecordingActiveConnection(), {
    openMessageStream: (): Promise<KafkaMessageStream> => Promise.resolve(pending),
  });
  const result = traceCorrelation(connection, "Fixture", request, controller.signal);
  await Promise.resolve();
  controller.abort();
  expect((await result).topics).toMatchObject([
    { state: "partial" },
    { state: "not-searched", reason: "cancelled" },
  ]);
  expect(stopped).toBe(true);
});
it("bounds aggregate trace work including duplicate header bytes across topics", async () => {
  const header = btoa("x".repeat(8_192));
  const heavy = (topic: string, index: number): KafkaMessage => ({
    ...message(topic, String(index), "other"),
    originalByteSize: 0,
    original: {
      state: "complete",
      encoding: "base64",
      key: null,
      value: null,
      headers: Array.from({ length: 24 }, () => ({ key: btoa("repeated"), value: header })),
    },
  });
  const opened: string[] = [];
  const connection = Object.assign(new RecordingActiveConnection(), {
    openMessageStream: ({ topic }: { topic: string }): Promise<KafkaMessageStream> => {
      opened.push(topic);
      return Promise.resolve(stream(Array.from({ length: 100 }, (_, i) => heavy(topic, i))));
    },
  });
  const result = await traceCorrelation(
    connection,
    "Fixture",
    { ...request, topics: ["a", "b", "c"] },
    new AbortController().signal,
  );
  expect(opened).toEqual(["a", "b"]);
  expect(result.topics).toMatchObject([
    { state: "searched", evaluated: 100 },
    { state: "partial", reason: "evaluation-byte-limit" },
    { state: "not-searched", reason: "evaluation-byte-limit" },
  ]);
});
it("keeps omitted oversized headers in the trace byte budget", async () => {
  const entries = Array.from({ length: 100 }, (_, index): KafkaMessage => ({
    ...message("a", String(index)),
    originalByteSize: 0,
    recordByteSize: 524_288,
    original: { state: "unavailable", reason: "size-limit" },
  }));
  const connection = Object.assign(new RecordingActiveConnection(), {
    openMessageStream: (): Promise<KafkaMessageStream> => Promise.resolve(stream(entries)),
  });
  const result = await traceCorrelation(
    connection,
    "Fixture",
    request,
    new AbortController().signal,
  );
  expect(result.topics).toMatchObject([
    { state: "partial", reason: "evaluation-byte-limit", unavailable: 64 },
    { state: "not-searched", reason: "evaluation-byte-limit" },
  ]);
});
it("rejects unbounded or ambiguous requests and fabricated complete coverage", () => {
  expect(() => parseCorrelationTraceInput({ ...request, topics: Array(9).fill("a") })).toThrow();
  expect(() => parseCorrelationTraceInput({ ...request, topics: ["a", "a"] })).toThrow();
  expect(() => parseCorrelationTraceInput({ ...request, startTimeMs: 2_000 })).toThrow();
  expect(() =>
    parseCorrelationTraceInput({
      ...request,
      selector: { source: "payload", path: "/bad~2", format: "json" },
    }),
  ).toThrow();
  expect(() =>
    parseCorrelationTraceResult({
      input: { ...request, topics: ["a"] },
      connectionName: "Fixture",
      matches: [],
      topics: [
        {
          topic: "a",
          state: "searched",
          reason: "fake",
          evaluated: 0,
          unavailable: 0,
          matches: 0,
          coverage: null,
        },
      ],
    }),
  ).toThrow();
});
