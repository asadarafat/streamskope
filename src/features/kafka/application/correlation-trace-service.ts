import {
  CORRELATION_TRACE_LIMITS as limits,
  parseCorrelationTraceInput,
  type CorrelationTraceInput,
  type CorrelationTraceResult,
  type CorrelationTopicEvidence,
  type CorrelationTraceMatch,
} from "../contracts/correlation-trace";
import { kafkaRawMessageRetainedBytes } from "../contracts/message-limits";

import type { KafkaActiveConnection, KafkaMessageStream } from "./types";
import type { RecordCodecService } from "./record-codec-service";
import { matchCorrelation } from "./correlation-selector";

export async function traceCorrelation(
  connection: KafkaActiveConnection,
  connectionName: string,
  request: CorrelationTraceInput,
  signal: AbortSignal,
  codec?: RecordCodecService,
): Promise<CorrelationTraceResult> {
  const input = parseCorrelationTraceInput(request);
  const matches: CorrelationTraceMatch[] = [];
  const topics: CorrelationTopicEvidence[] = [];
  const identities = new Set<string>();
  let bytes = 0;
  let stopReason: string | undefined;
  const cancelled = (): string =>
    signal.reason instanceof DOMException && signal.reason.name === "TimeoutError"
      ? "deadline"
      : "cancelled";
  for (const topic of input.topics) {
    if (signal.aborted) stopReason = cancelled();
    if (stopReason) {
      topics.push({
        topic,
        state: "not-searched",
        reason: stopReason,
        evaluated: 0,
        unavailable: 0,
        matches: 0,
        coverage: null,
      });
      continue;
    }
    let stream: KafkaMessageStream | undefined;
    let evaluated = 0;
    let unavailable = 0;
    let state: CorrelationTopicEvidence["state"] = "partial";
    let reason = "coverage-unavailable";
    let closeFailure = false;
    const close = (): void => {
      void stream?.close().catch(() => {
        closeFailure = true;
      });
    };
    signal.addEventListener("abort", close, { once: true });
    try {
      stream = await connection.openMessageStream(
        {
          mode: "time-window",
          topic,
          startTimeMs: input.startTimeMs,
          endTimeMs: input.endTimeMs,
          maxMessages: limits.recordsPerTopic,
        },
        signal,
      );
      if (signal.aborted) close();
      for await (const message of stream) {
        if (signal.aborted) {
          stopReason = cancelled();
          break;
        }
        if (evaluated + unavailable >= limits.recordsPerTopic) {
          reason = "record-limit";
          break;
        }
        const evaluatedBytes = Math.max(
          message.recordByteSize ?? message.originalByteSize,
          kafkaRawMessageRetainedBytes(message),
        );
        if (bytes + evaluatedBytes > limits.bytes) {
          stopReason = "evaluation-byte-limit";
          break;
        }
        bytes += evaluatedBytes;
        const timestamp = Date.parse(message.timestamp);
        if (message.topic !== topic || !Number.isFinite(timestamp)) {
          unavailable++;
          continue;
        }
        if (timestamp < input.startTimeMs || timestamp >= input.endTimeMs) continue;
        const identity = JSON.stringify([topic, message.partition, message.offset]);
        if (identities.has(identity)) continue;
        const match = await matchCorrelation(
          message,
          input,
          codec,
          connection.clusterServiceContext?.("schemaRegistry") ?? null,
          signal,
        );
        if (signal.aborted) {
          unavailable++;
          stopReason = cancelled();
          break;
        }
        if (match === "unavailable") unavailable++;
        else evaluated++;
        if (match === "matched") {
          identities.add(identity);
          matches.push({
            topic,
            partition: message.partition,
            offset: message.offset,
            timestamp: message.timestamp,
            preview: message.preview.slice(0, limits.preview),
          });
          if (matches.length >= limits.matches) {
            stopReason = "match-limit";
            break;
          }
        }
      }
    } catch (error) {
      if (signal.aborted) stopReason = cancelled();
      else {
        state =
          error !== null &&
          typeof error === "object" &&
          "code" in error &&
          error.code === "AUTHORIZATION_DENIED"
            ? "denied"
            : "failed";
        reason = state === "denied" ? "permission-denied" : "read-failed";
      }
    } finally {
      signal.removeEventListener("abort", close);
      try {
        await stream?.close();
      } catch {
        closeFailure = true;
      }
    }
    if (signal.aborted) stopReason = cancelled();
    const coverage = stream?.coverage?.() ?? null;
    if (state !== "denied" && state !== "failed") {
      reason = closeFailure
        ? "cleanup-failed"
        : (stopReason ?? (unavailable > 0 ? "records-unavailable" : (coverage?.reason ?? reason)));
      state =
        !closeFailure && !stopReason && unavailable === 0 && coverage?.reason === "range-complete"
          ? "searched"
          : "partial";
    }
    topics.push({
      topic,
      state,
      reason,
      evaluated,
      unavailable,
      matches: matches.filter((item) => item.topic === topic).length,
      coverage,
    });
  }
  return { input, connectionName, matches, topics };
}
