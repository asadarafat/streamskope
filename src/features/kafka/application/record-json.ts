import { RECORD_CODEC_LIMITS } from "../contracts/record-codec";

/** One bounded JSON projection for decoding, display, filtering and protection. */
export function boundedRecordJson(value: unknown): string {
  const pending: { value: unknown; depth: number }[] = [{ value, depth: 0 }];
  let nodes = 0;
  while (pending.length > 0) {
    const entry = pending.pop()!;
    if (++nodes > RECORD_CODEC_LIMITS.jsonNodes || entry.depth > RECORD_CODEC_LIMITS.jsonDepth)
      throw new RangeError("Decoded structure limit");
    if (entry.value !== null && typeof entry.value === "object") {
      const children = Object.values(entry.value);
      if (children.length + pending.length > RECORD_CODEC_LIMITS.jsonNodes)
        throw new RangeError("Decoded structure limit");
      for (const child of children) pending.push({ value: child, depth: entry.depth + 1 });
    }
    if (typeof entry.value === "number" && !Number.isFinite(entry.value))
      throw new Error("Non-finite value is not JSON");
  }
  const json = JSON.stringify(value);
  if (json.length > RECORD_CODEC_LIMITS.outputCharacters)
    throw new RangeError("Decoded output limit");
  return json;
}

export function normalizeRecordJson(text: string): string {
  if (text.length > RECORD_CODEC_LIMITS.outputCharacters) throw new RangeError("JSON input limit");
  const value: unknown = JSON.parse(
    text,
    (_key: string, item: unknown, context?: { source: string }): unknown => {
      if (
        typeof item === "number" &&
        (!Number.isFinite(item) || (Number.isInteger(item) && !Number.isSafeInteger(item)))
      ) {
        if (!context) throw new Error("Unsafe JSON number");
        return context.source;
      }
      return item;
    },
  );
  return boundedRecordJson(value);
}
