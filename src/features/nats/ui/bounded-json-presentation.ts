import { NATS_LIMITS } from "../contracts";

export const NATS_JSON_PRESENTATION_LIMITS = {
  depth: 32,
  bytes: 1024 * 1024,
} as const;

export type NatsJsonPresentation =
  | { readonly state: "ready"; readonly text: string }
  | { readonly state: "invalid" }
  | { readonly state: "limited"; readonly reason: "input" | "depth" | "output" };

/** Count JSON string escaping without allocating a serialized copy of a payload or key. */
function quotedBytes(value: string): number {
  let bytes = 2;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code === 0x22 || code === 0x5c) bytes += 2;
    else if (code < 0x20) bytes += [8, 9, 10, 12, 13].includes(code) ? 2 : 6;
    else if (code < 0x80) bytes += 1;
    else if (code < 0x800) bytes += 2;
    else if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4;
        index += 1;
      } else bytes += 6;
    } else bytes += code >= 0xdc00 && code <= 0xdfff ? 6 : 3;
  }
  return bytes;
}

/** Reject deep input before JSON.parse, ignoring brackets within escaped string values. */
function exceedsDepth(source: string): boolean {
  let depth = 0;
  let quoted = false;
  let escaped = false;
  for (const character of source) {
    if (quoted) {
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === '"') quoted = false;
    } else if (character === '"') quoted = true;
    else if (character === "[" || character === "{") {
      depth += 1;
      if (depth > NATS_JSON_PRESENTATION_LIMITS.depth) return true;
    } else if (character === "]" || character === "}") depth -= 1;
  }
  return false;
}

/** Measure the exact two-space JSON presentation, stopping as soon as its byte budget is spent. */
function fitsPresentation(value: unknown): boolean {
  let bytes = 0;
  function add(count: number): boolean {
    bytes += count;
    return bytes <= NATS_JSON_PRESENTATION_LIMITS.bytes;
  }
  function visit(item: unknown, depth: number): boolean {
    if (item === null) return add(4);
    if (typeof item === "string") return add(quotedBytes(item));
    if (typeof item === "number") return add(Number.isFinite(item) ? String(item).length : 4);
    if (typeof item === "boolean") return add(item ? 4 : 5);
    if (Array.isArray(item)) {
      if (item.length === 0) return add(2);
      if (!add(2 + item.length * (2 * (depth + 1) + 2) + 2 * depth)) return false;
      return item.every((child: unknown): boolean => visit(child, depth + 1));
    }
    if (typeof item === "object") {
      const entries = Object.entries(item as Record<string, unknown>);
      if (entries.length === 0) return add(2);
      if (!add(2 + entries.length * (2 * (depth + 1) + 2) + 2 * depth)) return false;
      return entries.every(
        ([key, child]): boolean => add(quotedBytes(key) + 2) && visit(child, depth + 1),
      );
    }
    return false;
  }
  return visit(value, 0);
}

/** Formatting is optional and bounded before any expanded presentation is allocated. */
export function prepareNatsJsonPresentation(source: string): NatsJsonPresentation {
  if (
    source.length > NATS_LIMITS.payloadBytes ||
    new TextEncoder().encode(source).length > NATS_LIMITS.payloadBytes
  ) {
    return { state: "limited", reason: "input" };
  }
  if (exceedsDepth(source)) return { state: "limited", reason: "depth" };
  let value: unknown;
  try {
    value = JSON.parse(source);
  } catch {
    return { state: "invalid" };
  }
  if (!fitsPresentation(value)) return { state: "limited", reason: "output" };
  return { state: "ready", text: JSON.stringify(value, null, 2) ?? "null" };
}
