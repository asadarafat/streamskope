import { parseKafkaTimestamp } from "./fetch-validation";
import { parseKafkaReadCoverage, type KafkaReadCoverage } from "./query-search";
import { HostContractValidationError } from "./validation-error";
import {
  boundedText,
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  record,
  text,
} from "./validation-primitives";

export const CORRELATION_TRACE_LIMITS = {
  topics: 8,
  recordsPerTopic: 1_000,
  matches: 200,
  durationMs: 30_000,
  bytes: 32 * 1_048_576,
  preview: 512,
} as const;
export interface CorrelationTraceInput {
  readonly traceId: string;
  readonly topics: readonly string[];
  readonly startTimeMs: number;
  readonly endTimeMs: number;
  readonly value: string;
  readonly selector: {
    readonly source: "header" | "key" | "payload";
    readonly path: string;
    readonly format: "auto" | "json" | "avro" | "protobuf";
  };
}
export interface CorrelationTraceMatch {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  readonly timestamp: string;
  readonly preview: string;
}
export interface CorrelationTopicEvidence {
  readonly topic: string;
  readonly state: "searched" | "partial" | "denied" | "failed" | "not-searched";
  readonly reason: string;
  readonly evaluated: number;
  readonly unavailable: number;
  readonly matches: number;
  readonly coverage: KafkaReadCoverage | null;
}
export interface CorrelationTraceResult {
  readonly input: CorrelationTraceInput;
  readonly connectionName: string;
  readonly matches: readonly CorrelationTraceMatch[];
  readonly topics: readonly CorrelationTopicEvidence[];
}
export function parseCorrelationTraceInput(value: unknown): CorrelationTraceInput {
  const input = record(value, "trace");
  exactKeys(input, ["traceId", "topics", "startTimeMs", "endTimeMs", "value", "selector"], "trace");
  if (
    !Array.isArray(input.topics) ||
    input.topics.length === 0 ||
    input.topics.length > CORRELATION_TRACE_LIMITS.topics
  )
    throw new HostContractValidationError("trace.topics", "select one to eight topics");
  const topics = input.topics.map((item: unknown) => text(item, "trace.topic", 249));
  if (
    new Set(topics).size !== topics.length ||
    topics.some((topic) => !/^[a-zA-Z0-9._-]+$/u.test(topic) || topic === "." || topic === "..")
  )
    throw new HostContractValidationError("trace.topics", "requires distinct Kafka topic names");
  const startTimeMs = parseKafkaTimestamp(input.startTimeMs, "trace.startTimeMs");
  const endTimeMs = parseKafkaTimestamp(input.endTimeMs, "trace.endTimeMs");
  if (startTimeMs >= endTimeMs)
    throw new HostContractValidationError("trace.endTimeMs", "must follow start time");
  const selector = record(input.selector, "trace.selector");
  exactKeys(selector, ["source", "path", "format"], "trace.selector");
  const source = declaredValue(
    selector.source,
    ["header", "key", "payload"] as const,
    "trace.selector.source",
  );
  const path = boundedText(selector.path, "trace.selector.path", 256);
  if (
    (source === "header" && !path) ||
    (source === "key" && path !== "") ||
    (source === "payload" &&
      ((path !== "" && !path.startsWith("/")) ||
        /~(?:[^01]|$)/u.test(path) ||
        path.split("/").length > 33))
  )
    throw new HostContractValidationError(
      "trace.selector.path",
      "use a header name, an empty key path, or a JSON Pointer up to 32 levels",
    );
  return {
    traceId: text(input.traceId, "trace.traceId", 128),
    topics,
    startTimeMs,
    endTimeMs,
    value: text(input.value, "trace.value", 256),
    selector: {
      source,
      path,
      format: declaredValue(
        selector.format,
        ["auto", "json", "avro", "protobuf"] as const,
        "trace.selector.format",
      ),
    },
  };
}
export function parseCorrelationTraceResult(value: unknown): CorrelationTraceResult {
  const result = record(value, "traceResult");
  exactKeys(result, ["input", "connectionName", "matches", "topics"], "traceResult");
  const input = parseCorrelationTraceInput(result.input);
  if (
    !Array.isArray(result.matches) ||
    result.matches.length > CORRELATION_TRACE_LIMITS.matches ||
    !Array.isArray(result.topics) ||
    result.topics.length !== input.topics.length
  )
    throw new HostContractValidationError("traceResult", "invalid bounded result inventory");
  const matches = result.matches.map((value: unknown): CorrelationTraceMatch => {
    const item = record(value, "traceMatch");
    exactKeys(item, ["topic", "partition", "offset", "timestamp", "preview"], "traceMatch");
    const offset = text(item.offset, "traceMatch.offset", 20);
    const timestamp = text(item.timestamp, "traceMatch.timestamp", 40);
    const topic = text(item.topic, "traceMatch.topic", 249);
    if (
      !/^(0|[1-9]\d*)$/u.test(offset) ||
      BigInt(offset) > 9_223_372_036_854_775_807n ||
      !Number.isFinite(Date.parse(timestamp)) ||
      !input.topics.includes(topic)
    )
      throw new HostContractValidationError("traceMatch", "invalid source identity");
    return {
      topic,
      offset,
      timestamp,
      partition: nonNegativeInteger(item.partition, "traceMatch.partition"),
      preview: boundedText(item.preview, "traceMatch.preview", CORRELATION_TRACE_LIMITS.preview),
    };
  });
  const topics = result.topics.map((value: unknown, index): CorrelationTopicEvidence => {
    const item = record(value, "traceTopic");
    exactKeys(
      item,
      ["topic", "state", "reason", "evaluated", "unavailable", "matches", "coverage"],
      "traceTopic",
    );
    if (item.topic !== input.topics[index])
      throw new HostContractValidationError(
        "traceTopic.topic",
        "must account for each selected topic in order",
      );
    const state = declaredValue(
      item.state,
      ["searched", "partial", "denied", "failed", "not-searched"] as const,
      "traceTopic.state",
    );
    const coverage =
      item.coverage === null ? null : parseKafkaReadCoverage(item.coverage, "traceTopic.coverage");
    const evaluated = nonNegativeInteger(item.evaluated, "traceTopic.evaluated");
    const unavailable = nonNegativeInteger(item.unavailable, "traceTopic.unavailable");
    const count = nonNegativeInteger(item.matches, "traceTopic.matches");
    if (
      count !== matches.filter((entry) => entry.topic === item.topic).length ||
      count > evaluated ||
      evaluated + unavailable > CORRELATION_TRACE_LIMITS.recordsPerTopic ||
      (state === "searched" && (coverage?.reason !== "range-complete" || unavailable !== 0))
    )
      throw new HostContractValidationError("traceTopic", "contains inconsistent coverage");
    return {
      topic: input.topics[index]!,
      state,
      reason: text(item.reason, "traceTopic.reason", 128),
      evaluated,
      unavailable,
      matches: count,
      coverage,
    };
  });
  if (
    new Set(matches.map((item) => JSON.stringify([item.topic, item.partition, item.offset])))
      .size !== matches.length
  )
    throw new HostContractValidationError(
      "traceResult.matches",
      "must not repeat a source identity",
    );
  return {
    input,
    connectionName: text(result.connectionName, "traceResult.connectionName", 256),
    matches,
    topics,
  };
}
