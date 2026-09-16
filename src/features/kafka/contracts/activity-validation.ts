import { HOST_ACTIVITY_DETAIL_CHARACTER_LIMIT, type ActivityEntry } from "./types";
import { declaredValue, exactKeys, record, text } from "./validation-primitives";

export function parseActivity(value: unknown, path: string): ActivityEntry {
  const activity = record(value, path);
  exactKeys(
    activity,
    ["correlationId", "detail", "id", "object", "operation", "outcome", "severity", "timestamp"],
    path,
  );
  return {
    correlationId: text(activity.correlationId, `${path}.correlationId`, 128),
    detail: text(activity.detail, `${path}.detail`, HOST_ACTIVITY_DETAIL_CHARACTER_LIMIT),
    id: text(activity.id, `${path}.id`, 128),
    object: text(activity.object, `${path}.object`, 512),
    operation: text(activity.operation, `${path}.operation`, 512),
    outcome: declaredValue(
      activity.outcome,
      ["started", "succeeded", "cancelled", "failed"],
      `${path}.outcome`,
    ),
    severity: declaredValue(activity.severity, ["info", "warning", "error"], `${path}.severity`),
    timestamp: text(activity.timestamp, `${path}.timestamp`, 128),
  };
}
