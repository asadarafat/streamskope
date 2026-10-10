/** Shared finite bounds for capture inputs, individual measurements and retained summaries. */
export function observationNumber(value: unknown, maximum: number): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0 || value > maximum)
    throw new Error("Invalid bounded observation value.");
  return value;
}
export function nullableObservationNumber(value: unknown, maximum: number): number | null {
  return value === null ? null : observationNumber(value, maximum);
}
export function observationArray(value: unknown, maximum: number): unknown[] {
  if (!Array.isArray(value) || value.length > maximum)
    throw new Error("Observation collection exceeds its limit.");
  return value as unknown[];
}
