import { describe, expect, it } from "vitest";

import {
  formatUtcClock,
  formatUtcClockSeconds,
  formatUtcTimestamp,
  formatUtcTimestampStacked,
} from "../../src/kafka/ui/timestamp-presentation";

describe("technical timestamp presentation", () => {
  it("renders canonical UTC evidence without ISO punctuation noise", () => {
    const timestamp = "2026-08-01T18:01:01.125Z";

    expect(formatUtcTimestamp(timestamp)).toBe("2026-08-01 · 18:01:01 UTC");
    expect(formatUtcTimestampStacked(timestamp)).toBe("2026-08-01\n18:01:01 UTC");
    expect(formatUtcClock(timestamp)).toBe("18:01:01.125");
    expect(formatUtcClockSeconds(timestamp)).toBe("18:01:01");
  });

  it("retains unknown timestamp evidence instead of inventing a value", () => {
    expect(formatUtcTimestamp("timestamp unavailable")).toBe("timestamp unavailable");
    expect(formatUtcTimestampStacked("timestamp unavailable")).toBe("timestamp unavailable");
    expect(formatUtcClock("timestamp unavailable")).toBe("timestamp unavailable");
    expect(formatUtcClockSeconds("timestamp unavailable")).toBe("timestamp unavailable");
  });
});
