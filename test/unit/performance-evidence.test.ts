import { describe, expect, it } from "vitest";

import { createPerformanceEvidence } from "../../tools/performance-evidence";

describe("performance evidence", () => {
  it("wraps raw measurements with reproducible command and runtime context", () => {
    expect(
      createPerformanceEvidence({
        capturedAt: "2026-07-29T06:00:00.000Z",
        check: "bounded-message-retention",
        command: "npm run performance:electron-message-retention",
        evidence: { retainedMessages: 1_000 },
        outcome: "passed",
        runtime: {
          arch: "arm64",
          node: "v24.12.0",
          platform: "linux",
        },
        sampleMethod: "Production reducer with exposed garbage collection.",
      }),
    ).toEqual({
      capturedAt: "2026-07-29T06:00:00.000Z",
      check: "bounded-message-retention",
      command: "npm run performance:electron-message-retention",
      evidence: { retainedMessages: 1_000 },
      outcome: "passed",
      runtime: {
        arch: "arm64",
        node: "v24.12.0",
        platform: "linux",
      },
      sampleMethod: "Production reducer with exposed garbage collection.",
      schemaVersion: 1,
    });
  });
});
