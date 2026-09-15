import { HOST_ACTIVITY_DETAIL_CHARACTER_LIMIT } from "../../kafka/contracts";

export const ACTIVITY_DETAIL_CHARACTER_LIMIT = HOST_ACTIVITY_DETAIL_CHARACTER_LIMIT;

const MAXIMUM_SCAN_CHARACTERS = 65_536;
const TRUNCATION_SUFFIX = "… [truncated]";
const PRIVATE_KEY_PATTERN =
  /-----BEGIN ([A-Z0-9 ]*PRIVATE KEY)-----[\s\S]*?(?:-----END \1-----|$)/giu;
const AUTHORIZATION_PATTERN =
  /(\bauthorization\b\s*[:=]\s*)(?!\[REDACTED\])(?:basic|bearer)\s+[^\s,;]+/giu;
const SENSITIVE_FIELD_PATTERN =
  /(\b(?:access_token|refresh_token|id_token|client_secret)\b\s*(?:=|:)\s*)(?!\[REDACTED\])(?:"[^"]*"|'[^']*'|[^&,\s}]+)/giu;

function escapeRegularExpression(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

function bounded(value: string, maximum: number, alreadyTruncated: boolean): string {
  if (!alreadyTruncated && value.length <= maximum) {
    return value;
  }
  if (maximum <= TRUNCATION_SUFFIX.length) {
    return TRUNCATION_SUFFIX.slice(0, maximum);
  }
  return `${value.slice(0, maximum - TRUNCATION_SUFFIX.length)}${TRUNCATION_SUFFIX}`;
}

export function redactSensitiveText(
  value: string,
  sensitiveValues: readonly string[] = [],
  maximumCharacters: number = ACTIVITY_DETAIL_CHARACTER_LIMIT,
): string {
  const maximum = Math.max(1, Math.floor(maximumCharacters));
  const scanLimit = Math.min(MAXIMUM_SCAN_CHARACTERS, Math.max(maximum, maximum * 4));
  let redacted = value.slice(0, scanLimit);

  const exactValues = [...new Set(sensitiveValues)]
    .filter((secret) => secret.length > 0)
    .sort((left, right) => right.length - left.length);
  for (const secret of exactValues) {
    redacted = redacted.replace(new RegExp(escapeRegularExpression(secret), "gu"), "[REDACTED]");
  }

  redacted = redacted
    .replace(PRIVATE_KEY_PATTERN, "[REDACTED PRIVATE KEY]")
    .replace(AUTHORIZATION_PATTERN, "$1[REDACTED]")
    .replace(SENSITIVE_FIELD_PATTERN, "$1[REDACTED]");

  return bounded(redacted, maximum, value.length > scanLimit || redacted.length > maximum);
}
