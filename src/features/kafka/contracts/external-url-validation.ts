import type { ExternalUrlOpenRequest, ExternalUrlOpenResult } from "./external-url-types";
import { parseKafkaRunbookUrl } from "./operational-preference-validation";
import { parseProtocolVersion } from "./protocol-validation";
import { declaredValue, exactKeys, record } from "./validation-primitives";

export function parseExternalUrlOpenRequest(
  value: unknown,
  path = "externalUrlOpenRequest",
): ExternalUrlOpenRequest {
  const request = record(value, path);
  exactKeys(request, ["url", "version"], path);
  return {
    url: parseKafkaRunbookUrl(request.url, `${path}.url`),
    version: parseProtocolVersion(request.version, `${path}.version`),
  };
}

export function parseExternalUrlOpenResult(
  value: unknown,
  path = "externalUrlOpenResult",
): ExternalUrlOpenResult {
  const result = record(value, path);
  exactKeys(result, ["state", "version"], path);
  return {
    state: declaredValue(result.state, ["accepted"] as const, `${path}.state`),
    version: parseProtocolVersion(result.version, `${path}.version`),
  };
}
