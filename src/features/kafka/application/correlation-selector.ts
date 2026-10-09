import type { CorrelationTraceInput } from "../contracts/correlation-trace";
import type { KafkaMessage } from "../contracts";
import { RECORD_CODEC_LIMITS } from "../contracts/record-codec";

import type { RecordCodecService } from "./record-codec-service";
import type { KafkaClusterServiceContext } from "./types";

const utf8 = (bytes: string): string =>
  new TextDecoder("utf-8", { fatal: true }).decode(
    Uint8Array.from(atob(bytes), (character) => character.charCodeAt(0)),
  );
function jsonPointer(json: string, path: string): unknown {
  if (json.length > RECORD_CODEC_LIMITS.outputCharacters) throw new Error("JSON limit");
  let nodes = 0;
  let value: unknown = JSON.parse(
    json,
    (_key: string, value: unknown, context?: { source?: string }): unknown => {
      if (++nodes > RECORD_CODEC_LIMITS.jsonNodes) throw new Error("JSON node limit");
      // Correlation compares scalar text. Numeric source text avoids precision loss.
      if (typeof value === "number" && context?.source === undefined)
        throw new Error("Numeric source unavailable");
      return typeof value === "number" ? context?.source : value;
    },
  );
  for (const token of path === "" ? [] : path.slice(1).split("/")) {
    const key = token.replaceAll("~1", "/").replaceAll("~0", "~");
    if (!value || typeof value !== "object" || !Object.hasOwn(value, key)) return undefined;
    value = (value as Record<string, unknown>)[key];
  }
  return value;
}
export async function matchCorrelation(
  message: KafkaMessage,
  input: CorrelationTraceInput,
  codec: RecordCodecService | undefined,
  context: KafkaClusterServiceContext | null,
  signal: AbortSignal,
): Promise<"matched" | "different" | "unavailable"> {
  const structured = message.structured;
  if (structured) {
    signal.throwIfAborted();
    if (input.selector.source === "header") {
      if (structured.headersState === "unavailable") return "unavailable";
      let unavailable = false;
      for (const header of structured.headers) {
        if (header.error) {
          unavailable = true;
          continue;
        }
        if (header.key === input.selector.path && header.value === input.value) return "matched";
      }
      return unavailable ? "unavailable" : "different";
    }
    const field = input.selector.source === "key" ? structured.key : structured.value;
    if (field.state === "null") return "different";
    if (field.state === "error" || field.state === "masked") return "unavailable";
    if (input.selector.source === "key")
      return field.text === input.value ? "matched" : "different";
    if (field.json === null) return "unavailable";
    try {
      const value = jsonPointer(field.json, input.selector.path);
      return (typeof value === "string" || typeof value === "boolean") &&
        String(value) === input.value
        ? "matched"
        : "different";
    } catch {
      return "unavailable";
    }
  }
  const original = message.original;
  if (original?.state !== "complete") return "unavailable";
  try {
    if (input.selector.source === "key")
      return original.key !== null && utf8(original.key) === input.value ? "matched" : "different";
    if (input.selector.source === "header") {
      let unavailable = false;
      for (const header of original.headers) {
        try {
          if (
            utf8(header.key) === input.selector.path &&
            header.value !== null &&
            utf8(header.value) === input.value
          )
            return "matched";
        } catch {
          unavailable = true;
        }
      }
      return unavailable ? "unavailable" : "different";
    }
    if (original.value === null) return "different";
    let json: string;
    if (input.selector.format === "json" || input.selector.format === "auto")
      json = utf8(original.value);
    else {
      if (!codec) return "unavailable";
      const decoded = await codec.decode(
        { format: input.selector.format, bytes: original.value },
        context,
        signal,
      );
      if (decoded.state !== "decoded") return "unavailable";
      json = decoded.json;
    }
    const value = jsonPointer(json, input.selector.path);
    return (typeof value === "string" || typeof value === "boolean") &&
      String(value) === input.value
      ? "matched"
      : "different";
  } catch {
    return "unavailable";
  }
}
