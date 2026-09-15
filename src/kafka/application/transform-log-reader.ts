import {
  KAFKA_MESSAGE_LIMITS,
  REDPANDA_TRANSFORM_LIMITS,
  REDPANDA_TRANSFORM_LOG_TOPIC,
  type RedpandaTransformLogEntry,
} from "../contracts";

import type { KafkaActiveConnection } from "./types";

export interface RedpandaTransformLogResult {
  readonly logs: readonly RedpandaTransformLogEntry[];
  readonly omittedLogs: number;
}

interface ParsedLogContent {
  readonly level: string;
  readonly message: string;
  readonly timestamp: string | null;
}

function eventTimestamp(value: unknown): string | null {
  if (typeof value !== "number" && typeof value !== "string") return null;
  try {
    const nanoseconds = BigInt(value);
    const milliseconds = nanoseconds / 1_000_000n;
    if (milliseconds < 0n || milliseconds > BigInt(8_640_000_000_000_000)) return null;
    const timestamp = new Date(Number(milliseconds));
    return Number.isNaN(timestamp.valueOf()) ? null : timestamp.toISOString();
  } catch {
    return null;
  }
}

function parseLogContent(message: string): ParsedLogContent {
  try {
    const parsed: unknown = JSON.parse(message);
    if (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)) {
      const item = parsed as Record<string, unknown>;
      const body = item.body;
      if (body !== null && typeof body === "object" && !Array.isArray(body)) {
        const stringValue = (body as Record<string, unknown>).stringValue;
        if (typeof stringValue === "string" && stringValue.length <= 32_768) {
          return {
            level:
              typeof item.severityNumber === "number" && item.severityNumber > 9 ? "warn" : "info",
            message: stringValue,
            timestamp: eventTimestamp(item.timeUnixNano),
          };
        }
      }
      return {
        level: typeof item.level === "string" && item.level.length <= 64 ? item.level : "log",
        message:
          typeof item.message === "string" && item.message.length <= 32_768
            ? item.message
            : message,
        timestamp: null,
      };
    }
  } catch {
    // Raw transform log records are valid evidence and remain unchanged.
  }
  return { level: "log", message, timestamp: null };
}

export async function readRedpandaTransformLogs(
  connection: KafkaActiveConnection,
  name: string,
  signal: AbortSignal,
): Promise<RedpandaTransformLogResult> {
  const stream = await connection.openMessageStream(
    {
      maxMessages: KAFKA_MESSAGE_LIMITS.retainedMessages,
      mode: "newest",
      topic: REDPANDA_TRANSFORM_LOG_TOPIC,
    },
    signal,
  );
  const logs: RedpandaTransformLogEntry[] = [];
  try {
    for await (const message of stream) {
      if (message.key !== name) continue;
      const content = parseLogContent(message.payload ?? message.preview);
      logs.push({
        level: content.level,
        message: content.message.slice(0, 32_768),
        offset: message.offset,
        partition: message.partition,
        timestamp: content.timestamp ?? message.timestamp,
      });
    }
  } finally {
    await stream.close();
  }
  return {
    logs: logs.slice(-REDPANDA_TRANSFORM_LIMITS.logs),
    omittedLogs: Math.max(0, logs.length - REDPANDA_TRANSFORM_LIMITS.logs),
  };
}
