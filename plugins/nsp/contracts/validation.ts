import {
  HOST_ERROR_CODES,
  HOST_ERROR_STAGES,
  type HostError,
} from "../../../src/features/kafka/contracts";
import {
  declaredValue,
  emptyRecord,
  exactKeys,
  optionalText,
  parseBoundedBrokers,
  record,
  text,
  truth,
} from "../../../src/features/kafka/contracts/validation-primitives";
import { HostContractValidationError } from "../../../src/features/kafka/contracts/validation-error";
import type { PluginProfileSource } from "../../../src/plugins/contracts";

import {
  NSP_CAPTURE_STEPS,
  NSP_PLUGIN_ID,
  type NspConnectInput,
  type NspCredentials,
  type NspProfileSource,
  type NspProgress,
  type NspResult,
  type NspStatus,
} from "./types";

export function normalizeNspApiUrl(value: unknown): string {
  const input = text(value, "apiUrl", 2_048).trim();
  let url: URL;
  try {
    url = new URL(input);
  } catch {
    throw new HostContractValidationError("apiUrl", "must be an HTTPS URL");
  }
  if (
    url.protocol !== "https:" ||
    !url.hostname ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    url.pathname !== "/"
  )
    throw new HostContractValidationError(
      "apiUrl",
      "must be an HTTPS server URL without credentials, paths, queries or fragments",
    );
  return url.origin;
}

function credentials(input: Record<string, unknown>): NspCredentials {
  return {
    apiUrl: normalizeNspApiUrl(input.apiUrl),
    username: text(input.username, "username", 256),
    password: text(input.password, "password", 4_096),
    verifyCertificate: truth(input.verifyCertificate, "verifyCertificate"),
  };
}

export function parseNspCredentials(value: unknown): NspCredentials {
  const input = record(value, "input");
  exactKeys(input, ["apiUrl", "username", "password", "verifyCertificate"], "input");
  return credentials(input);
}

function brokers(value: unknown): readonly string[] {
  return parseBoundedBrokers(value, "brokers", 32, 512).map((entry) => {
    const broker = entry.trim();
    const match = /^(\[[\da-fA-F:]+\]|[a-zA-Z\d](?:[a-zA-Z\d.-]*[a-zA-Z\d])?):(\d{1,5})$/u.exec(
      broker,
    );
    if (match === null || Number(match[2]) < 1 || Number(match[2]) > 65_535)
      throw new HostContractValidationError("brokers", "must contain host:port endpoints");
    return broker;
  });
}

export function parseNspConnectInput(value: unknown): NspConnectInput {
  const input = record(value, "input");
  exactKeys(
    input,
    [
      "apiUrl",
      "username",
      "password",
      "verifyCertificate",
      "profileId",
      "brokers",
      "authentication",
    ],
    "input",
  );
  const profileId = optionalText(input, "profileId", "input", 128);
  return {
    ...credentials(input),
    ...(profileId === undefined ? {} : { profileId }),
    ...(input.brokers === undefined ? {} : { brokers: brokers(input.brokers) }),
    ...(input.authentication === undefined
      ? {}
      : {
          authentication: declaredValue(
            input.authentication,
            ["auto", "tls", "oauth"] as const,
            "input.authentication",
          ),
        }),
  };
}

export function parseNspCancelInput(value: unknown): { readonly requestId: string } {
  const input = record(value, "input");
  exactKeys(input, ["requestId"], "input");
  return { requestId: text(input.requestId, "input.requestId", 128) };
}

export function parseNspStatusInput(value: unknown): Readonly<Record<string, never>> {
  return emptyRecord(value, "input");
}

function status(value: unknown): NspStatus {
  const input = record(value, "status");
  exactKeys(input, ["state", "requestId", "message"], "status");
  const requestId = optionalText(input, "requestId", "status", 128);
  const message = optionalText(input, "message", "status", 2_048);
  return {
    state: declaredValue(input.state, ["idle", "running", "cleanup-required"], "status.state"),
    ...(requestId === undefined ? {} : { requestId }),
    ...(message === undefined ? {} : { message }),
  };
}

function hostError(value: unknown): HostError {
  const input = record(value, "error");
  exactKeys(
    input,
    [
      "activeStateChanged",
      "code",
      "correlationId",
      "recovery",
      "retryable",
      "stage",
      "summary",
      "target",
    ],
    "error",
  );
  const target = optionalText(input, "target", "error", 2_048);
  return {
    activeStateChanged: truth(input.activeStateChanged, "error.activeStateChanged"),
    code: declaredValue(input.code, HOST_ERROR_CODES, "error.code"),
    correlationId: text(input.correlationId, "error.correlationId", 128),
    recovery: text(input.recovery, "error.recovery", 2_048),
    retryable: truth(input.retryable, "error.retryable"),
    stage: declaredValue(input.stage, HOST_ERROR_STAGES, "error.stage"),
    summary: text(input.summary, "error.summary", 2_048),
    ...(target === undefined ? {} : { target }),
  };
}

/** Reject undeclared data so workflow outputs and retrieved secrets cannot enter the view. */
export function parseNspResult(value: unknown): NspResult {
  const input = record(value, "result");
  const ok = truth(input.ok, "result.ok");
  exactKeys(input, ok ? ["ok", "profileId", "status", "cancelled"] : ["ok", "error"], "result");
  if (!ok) return { ok: false, error: hostError(input.error) };
  const profileId = optionalText(input, "profileId", "result", 128);
  return {
    ok: true,
    ...(profileId === undefined ? {} : { profileId }),
    ...(input.status === undefined ? {} : { status: status(input.status) }),
    ...(input.cancelled === undefined
      ? {}
      : { cancelled: truth(input.cancelled, "result.cancelled") }),
  };
}

export function parseNspProgress(value: unknown): NspProgress {
  const input = record(value, "progress");
  exactKeys(input, ["requestId", "step", "message"], "progress");
  return {
    requestId: text(input.requestId, "progress.requestId", 128),
    step: declaredValue(input.step, NSP_CAPTURE_STEPS, "progress.step"),
    message: text(input.message, "progress.message", 2_048),
  };
}

export function parseNspProfileSource(value: unknown): NspProfileSource {
  const input = record(value, "source");
  exactKeys(input, ["apiUrl", "brokers", "workflowName", "authentication"], "source");
  return {
    apiUrl: normalizeNspApiUrl(input.apiUrl),
    brokers: brokers(input.brokers),
    workflowName: text(input.workflowName, "source.workflowName", 256),
    authentication: declaredValue(input.authentication, ["tls", "oauth"], "source.authentication"),
  };
}

export function toPluginProfileSource(source: NspProfileSource): PluginProfileSource {
  const parsed = parseNspProfileSource(source);
  return {
    kind: "plugin",
    pluginId: NSP_PLUGIN_ID,
    version: 1,
    data: { ...parsed },
  };
}

export function fromPluginProfileSource(
  source: PluginProfileSource | undefined,
): NspProfileSource | undefined {
  if (source?.pluginId !== NSP_PLUGIN_ID || source.version !== 1) return undefined;
  try {
    return parseNspProfileSource(source.data);
  } catch {
    return undefined;
  }
}
