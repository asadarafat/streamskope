import { describe, expect, it } from "vitest";

import {
  HOST_ACTIVITY_HISTORY_LIMIT,
  type ActivityEntry,
} from "../../src/features/kafka/contracts";
import { activitySupportDocument } from "../../src/features/kafka/ui/activity-support-report";
import { STREAMSKOPE_RELEASE } from "../../src/plugins/host-release";

const entry: ActivityEntry = {
  correlationId: "2aecf118-dacc-4e45-83dc-d11fab0b5fa4",
  timestamp: "2026-10-08T08:00:00.000Z",
  severity: "error",
  outcome: "failed",
  id: "private-profile-name",
  detail: "password=private-secret; upstream=https://private.example.test",
  object: "private-topic",
  operation: "private-operation",
};

describe("safe activity support evidence", () => {
  it("retains useful correlation metadata while excluding every free-form legacy field", () => {
    const document = activitySupportDocument([entry], 14, true);
    expect(document.content).not.toMatch(/private-|private\.example/u);
    const report: unknown = JSON.parse(document.content);
    expect(report).toMatchObject({
      format: "streamskope.support/v1",
      release: STREAMSKOPE_RELEASE,
      scope: {
        provider: "kafka",
        source: "retained-ui-activity-metadata",
        retainedCount: 14,
        visibleCount: 1,
        exportedCount: 1,
        filtersActive: true,
        completeHistory: false,
        omittedMetadataFields: 0,
      },
      entries: [
        {
          correlationId: entry.correlationId,
          timestamp: entry.timestamp,
          severity: "error",
          outcome: "failed",
        },
      ],
    });
    expect(document.byteSize).toBe(new TextEncoder().encode(document.content).byteLength);
    expect(document.fileName).toMatch(/^streamskope-support-[\dTZ-]+\.json$/u);
  });

  it("does not treat malformed metadata as safe text or invent a replacement correlation", () => {
    const document = activitySupportDocument(
      [
        {
          ...entry,
          correlationId: "private-secret",
          timestamp: "private-time",
          severity: "private-severity",
          outcome: "private-outcome",
        } as unknown as ActivityEntry,
      ],
      1,
      false,
    );
    expect(document.content).not.toContain("private-");
    const report: unknown = JSON.parse(document.content);
    expect(report).toMatchObject({
      scope: { omittedMetadataFields: 4 },
      entries: [{ correlationId: null, timestamp: null, severity: null, outcome: null }],
    });
  });

  it("bounds a large visible view, retains the newest metadata and states its incomplete scope", () => {
    const entries = Array.from({ length: HOST_ACTIVITY_HISTORY_LIMIT + 20 }, (_, index) => ({
      ...entry,
      timestamp: new Date(index * 1000).toISOString(),
    }));
    const document = activitySupportDocument(entries, entries.length, false);
    const report = JSON.parse(document.content) as {
      entries: { timestamp: string }[];
      scope: unknown;
    };
    expect(report.entries).toHaveLength(HOST_ACTIVITY_HISTORY_LIMIT);
    expect(report.entries[0]!.timestamp).toBe(entries[20]!.timestamp);
    expect(report.scope).toMatchObject({
      retainedCount: entries.length,
      visibleCount: entries.length,
      exportedCount: HOST_ACTIVITY_HISTORY_LIMIT,
      completeHistory: false,
    });
    expect(document.byteSize).toBeLessThan(32 * 1024);
  });
});
