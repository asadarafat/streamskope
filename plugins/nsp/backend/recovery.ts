import type { JsonValue } from "../../../src/plugins/contracts";
import { parseNspCredentials } from "../contracts";

export interface NspRecovery {
  readonly version: 1;
  readonly apiUrl: string;
  readonly username: string;
  readonly requestId: string;
  readonly executionId?: string;
}

/** Recovery is deliberately limited to identifiers, never passwords or execution output. */
export function parseRecovery(value: JsonValue | null): NspRecovery | undefined {
  if (value === null) return undefined;
  if (typeof value !== "object" || Array.isArray(value))
    throw new Error("Invalid NSP recovery record.");
  const input = value as Record<string, JsonValue>;
  if (
    input.version !== 1 ||
    Object.keys(input).some(
      (key) => !["version", "apiUrl", "username", "requestId", "executionId"].includes(key),
    ) ||
    typeof input.requestId !== "string" ||
    !/^[a-zA-Z0-9-]{1,128}$/u.test(input.requestId) ||
    (input.executionId !== undefined &&
      (typeof input.executionId !== "string" || !/^[a-zA-Z0-9-]{1,128}$/u.test(input.executionId)))
  )
    throw new Error("Invalid NSP recovery record.");
  const credentials = parseNspCredentials({
    apiUrl: input.apiUrl,
    username: input.username,
    password: "unused",
    verifyCertificate: true,
  });
  return {
    version: 1,
    apiUrl: credentials.apiUrl,
    username: credentials.username,
    requestId: input.requestId,
    ...(input.executionId === undefined ? {} : { executionId: input.executionId }),
  };
}
