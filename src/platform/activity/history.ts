import { HOST_ACTIVITY_HISTORY_LIMIT, type ActivityEntry } from "../../kafka/contracts";

import { ACTIVITY_DETAIL_CHARACTER_LIMIT, redactSensitiveText } from "./redaction";

export const ACTIVITY_HISTORY_LIMIT = HOST_ACTIVITY_HISTORY_LIMIT;

export class ActivityHistory {
  private readonly activity: ActivityEntry[] = [];

  constructor(private readonly maximumEntries: number = ACTIVITY_HISTORY_LIMIT) {
    if (!Number.isSafeInteger(maximumEntries) || maximumEntries < 1) {
      throw new RangeError("Activity history limit must be a positive safe integer.");
    }
  }

  entries(): readonly ActivityEntry[] {
    return [...this.activity];
  }

  record(entry: ActivityEntry, sensitiveValues: readonly string[] = []): ActivityEntry {
    const safeEntry: ActivityEntry = {
      ...entry,
      correlationId: redactSensitiveText(entry.correlationId, sensitiveValues, 128),
      detail: redactSensitiveText(entry.detail, sensitiveValues, ACTIVITY_DETAIL_CHARACTER_LIMIT),
      id: redactSensitiveText(entry.id, sensitiveValues, 128),
      object: redactSensitiveText(entry.object, sensitiveValues, 512),
      operation: redactSensitiveText(entry.operation, sensitiveValues, 512),
      timestamp: redactSensitiveText(entry.timestamp, sensitiveValues, 128),
    };
    this.activity.push(safeEntry);
    const excess = this.activity.length - this.maximumEntries;
    if (excess > 0) {
      this.activity.splice(0, excess);
    }
    return safeEntry;
  }
}
