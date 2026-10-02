import { KAFKA_FETCH_LIMITS, KAFKA_FETCH_MODES, type KafkaFetchRequest } from "./types";
import { HostContractValidationError } from "./validation-error";
import {
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  positiveBoundedInteger,
  record,
  text,
} from "./validation-primitives";

export const KAFKA_MAX_TIMESTAMP_MS = 8_640_000_000_000_000;

export function parseKafkaTimestamp(value: unknown, path: string): number {
  const timestamp = nonNegativeInteger(value, path);
  if (timestamp > KAFKA_MAX_TIMESTAMP_MS) {
    throw new HostContractValidationError(path, "must be a representable timestamp");
  }
  return timestamp;
}

export function parseKafkaFetchRequest(value: unknown, path = "fetch"): KafkaFetchRequest {
  const request = record(value, path);
  const mode = declaredValue(request.mode, KAFKA_FETCH_MODES, `${path}.mode`);
  const common = {
    maxMessages: positiveBoundedInteger(
      request.maxMessages,
      `${path}.maxMessages`,
      KAFKA_FETCH_LIMITS.maxMessages,
    ),
    topic: text(request.topic, `${path}.topic`, 512),
  };
  if (common.topic.trim().length === 0) {
    throw new HostContractValidationError(`${path}.topic`, "must name a topic");
  }
  if (mode !== "time-window") {
    exactKeys(request, ["maxMessages", "mode", "topic"], path);
    return { ...common, mode };
  }
  exactKeys(request, ["endTimeMs", "maxMessages", "mode", "startTimeMs", "topic"], path);
  const startTimeMs = parseKafkaTimestamp(request.startTimeMs, `${path}.startTimeMs`);
  const endTimeMs = parseKafkaTimestamp(request.endTimeMs, `${path}.endTimeMs`);
  if (startTimeMs >= endTimeMs) {
    throw new HostContractValidationError(`${path}.endTimeMs`, "must be greater than startTimeMs");
  }
  return { ...common, endTimeMs, mode, startTimeMs };
}
