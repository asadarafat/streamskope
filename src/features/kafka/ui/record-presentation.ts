import type { RecordCodecSelection, RecordField } from "../contracts/structured-record";

export const recordCodecLabels: Record<RecordCodecSelection, string> = {
  auto: "Detect encoding",
  utf8: "UTF-8 text",
  json: "UTF-8 JSON",
  avro: "Confluent Avro",
  protobuf: "Confluent Protobuf",
  bytes: "Bytes (Base64)",
};

/** Indent JSON tokens without rounding numbers or rewriting strings. */
export function formatRecordJson(value: string | null): string | null {
  if (value === null) return null;
  try {
    JSON.parse(value);
  } catch {
    return null;
  }
  let result = "";
  let depth = 0;
  let quoted = false;
  let escaped = false;
  const newline = (): string => `\n${"  ".repeat(depth)}`;
  // Walk the original string; whitespace is only removed outside quoted tokens.
  for (let index = 0; index < value.length; index++) {
    const character = value[index]!;
    if (quoted) {
      result += character;
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === '"') quoted = false;
      continue;
    }
    if (/\s/u.test(character)) continue;
    if (character === '"') {
      quoted = true;
      result += character;
    } else if (character === "{" || character === "[") {
      result += character;
      depth++;
      if (depth > 64) return value;
      let lookahead = index + 1;
      while (lookahead < value.length && /\s/u.test(value[lookahead]!)) lookahead++;
      const next = value[lookahead];
      if (next !== "}" && next !== "]") result += newline();
    } else if (character === "}" || character === "]") {
      depth--;
      if (!result.endsWith("{") && !result.endsWith("[")) result += newline();
      result += character;
    } else if (character === ",") result += `${character}${newline()}`;
    else if (character === ":") result += ": ";
    else result += character;
    if (result.length > 512 * 1_024) return value;
  }
  return result;
}

export function recordFieldComparison(field: RecordField): string | null {
  if (field.state === "error") return null;
  if (field.state === "null") return '{"kind":"Kafka null"}';
  if (field.state === "masked") return '{"kind":"masked","value":"[MASKED]"}';
  return field.json === null
    ? JSON.stringify({ kind: "text", value: field.text })
    : `{"kind":"decoded","value":${field.json}}`;
}
