import { HOST_ERROR_CODES, HOST_ERROR_STAGES, type HostError } from "./host-errors";
import {
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  optionalText,
  record,
  text,
  truth,
} from "./validation-primitives";

export function parseHostError(value: unknown, path: string): HostError {
  const error = record(value, path);
  exactKeys(
    error,
    [
      "activeStateChanged",
      "code",
      "correlationId",
      "recovery",
      "retryable",
      "retryAfterMs",
      "stage",
      "summary",
      "target",
    ],
    path,
  );
  const target = optionalText(error, "target", path, 2_048);
  const base = {
    activeStateChanged: truth(error.activeStateChanged, `${path}.activeStateChanged`),
    code: declaredValue(error.code, HOST_ERROR_CODES, `${path}.code`),
    correlationId: text(error.correlationId, `${path}.correlationId`, 128),
    recovery: text(error.recovery, `${path}.recovery`, 2_048),
    retryable: truth(error.retryable, `${path}.retryable`),
    stage: declaredValue(error.stage, HOST_ERROR_STAGES, `${path}.stage`),
    summary: text(error.summary, `${path}.summary`, 2_048),
    ...(error.retryAfterMs === undefined
      ? {}
      : { retryAfterMs: nonNegativeInteger(error.retryAfterMs, `${path}.retryAfterMs`) }),
  };
  return target === undefined ? base : { ...base, target };
}
