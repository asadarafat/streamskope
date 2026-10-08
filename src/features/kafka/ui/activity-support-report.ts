import {
  HOST_ACTIVITY_HISTORY_LIMIT,
  type ActivityEntry,
  type HostTextDocument,
} from "../contracts";
import { STREAMSKOPE_RELEASE } from "../../../plugins/host-release";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;

/** Reconstruct metadata; legacy activity text is never safe merely because it was redacted. */
export function activitySupportDocument(
  visible: readonly ActivityEntry[],
  retainedCount: number,
  filtersActive: boolean,
): HostTextDocument {
  let omittedMetadataFields = 0;
  const safe = <T>(value: T, valid: boolean): T | null => {
    if (valid) return value;
    omittedMetadataFields += 1;
    return null;
  };
  const entries = visible.slice(-HOST_ACTIVITY_HISTORY_LIMIT).map((entry) => ({
    timestamp: safe(
      entry.timestamp,
      typeof entry.timestamp === "string" &&
        Number.isFinite(Date.parse(entry.timestamp)) &&
        new Date(entry.timestamp).toISOString() === entry.timestamp,
    ),
    correlationId: safe(
      entry.correlationId,
      typeof entry.correlationId === "string" && UUID.test(entry.correlationId),
    ),
    severity: safe(entry.severity, ["info", "warning", "error"].includes(entry.severity)),
    outcome: safe(
      entry.outcome,
      ["started", "succeeded", "cancelled", "failed"].includes(entry.outcome),
    ),
  }));
  const createdAt = new Date().toISOString();
  const content = `${JSON.stringify(
    {
      format: "streamskope.support/v1",
      release: STREAMSKOPE_RELEASE,
      createdAt,
      scope: {
        provider: "kafka",
        source: "retained-ui-activity-metadata",
        retainedCount,
        visibleCount: visible.length,
        exportedCount: entries.length,
        limit: HOST_ACTIVITY_HISTORY_LIMIT,
        filtersActive,
        completeHistory: false,
        omittedMetadataFields,
      },
      omissions: [
        "Free-form activity detail, object, operation and filter text",
        "Profiles, credentials, endpoints, configuration and message data",
        "Host logs, startup diagnostics and activity no longer retained by this view",
      ],
      entries,
    },
    null,
    2,
  )}\n`;
  return {
    content,
    byteSize: new TextEncoder().encode(content).byteLength,
    fileName: `streamskope-support-${createdAt.replaceAll(":", "-").replaceAll(".", "-")}.json`,
    mediaType: "application/json",
  };
}
