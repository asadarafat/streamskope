const CANONICAL_UTC_TIMESTAMP = /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(\.\d{3})?Z$/u;

function timestampParts(value: string): readonly [string, string, string] | null {
  const match = CANONICAL_UTC_TIMESTAMP.exec(value);
  if (match === null) {
    return null;
  }
  return [match[1] ?? "", match[2] ?? "", match[3] ?? ""];
}

export function formatUtcTimestamp(value: string): string {
  const parts = timestampParts(value);
  return parts === null ? value : `${parts[0]} · ${parts[1]} UTC`;
}

export function formatUtcTimestampStacked(value: string): string {
  const parts = timestampParts(value);
  return parts === null ? value : `${parts[0]}\n${parts[1]} UTC`;
}

export function formatUtcClock(value: string): string {
  const parts = timestampParts(value);
  return parts === null ? value : `${parts[1]}${parts[2]}`;
}

export function formatUtcClockSeconds(value: string): string {
  const parts = timestampParts(value);
  return parts === null ? value : parts[1];
}
