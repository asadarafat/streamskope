import type {
  PluginAcquisitionProgress,
  PluginNetworkSnapshot,
  PluginNetworkTestResult,
} from "../../../plugins/contracts";
import {
  parsePluginNetworkConfiguration,
  parsePluginNetworkUpdateInput,
  PLUGIN_NETWORK_LIMITS,
} from "../../../plugins/network-validation";

import type {
  HostCommand,
  HostCommandName,
  HostCommandResponse,
  HostEvent,
  HostEventName,
  HOST_PROTOCOL_VERSION,
} from "./types";
import { HostContractValidationError } from "./validation-error";
import {
  canonicalIsoTimestamp,
  declaredValue,
  exactKeys,
  nonNegativeInteger,
  optionalText,
  record,
  text,
  truth,
} from "./validation-primitives";

function networkValue<T>(parse: () => T, path: string): T {
  try {
    return parse();
  } catch (error) {
    throw new HostContractValidationError(
      path,
      error instanceof Error ? error.message : "invalid plugin networking data",
    );
  }
}

function snapshot(value: unknown): PluginNetworkSnapshot {
  const input = record(value, "pluginNetwork");
  exactKeys(
    input,
    [
      "revision",
      "configuration",
      "credentialsConfigured",
      "credentialStorage",
      "nativeAvailable",
      "supportedProxyProtocols",
      "error",
    ],
    "pluginNetwork",
  );
  const configuration =
    input.configuration === null
      ? null
      : networkValue(
          () => parsePluginNetworkConfiguration(input.configuration),
          "pluginNetwork.configuration",
        );
  const error = optionalText(input, "error", "pluginNetwork", 2048);
  if (configuration === null && error === undefined)
    throw new HostContractValidationError(
      "pluginNetwork.error",
      "must explain why saved settings are unavailable",
    );
  const credentialsConfigured = truth(
    input.credentialsConfigured,
    "pluginNetwork.credentialsConfigured",
  );
  const credentialStorage = declaredValue(
    input.credentialStorage,
    ["encrypted", "session", "unavailable"] as const,
    "pluginNetwork.credentialStorage",
  );
  const nativeAvailable = truth(input.nativeAvailable, "pluginNetwork.nativeAvailable");
  if (!Array.isArray(input.supportedProxyProtocols) || input.supportedProxyProtocols.length > 2)
    throw new HostContractValidationError(
      "pluginNetwork.supportedProxyProtocols",
      "must contain at most two supported protocols",
    );
  const supportedProxyProtocols = input.supportedProxyProtocols.map((protocol: unknown) =>
    declaredValue(protocol, ["http", "https"] as const, "pluginNetwork.supportedProxyProtocols"),
  );
  if (
    new Set(supportedProxyProtocols).size !== supportedProxyProtocols.length ||
    (!nativeAvailable && supportedProxyProtocols.length !== 0)
  )
    throw new HostContractValidationError(
      "pluginNetwork.supportedProxyProtocols",
      "must declare distinct native proxy capabilities",
    );
  if (
    credentialsConfigured &&
    (!nativeAvailable || configuration?.proxyUrl == null || credentialStorage === "unavailable")
  )
    throw new HostContractValidationError(
      "pluginNetwork.credentialsConfigured",
      "requires a scoped proxy credential store",
    );
  if (configuration?.mode === "custom" && !nativeAvailable)
    throw new HostContractValidationError(
      "pluginNetwork.configuration",
      "custom proxy settings require the native host",
    );
  return {
    revision: nonNegativeInteger(input.revision, "pluginNetwork.revision"),
    configuration,
    credentialsConfigured,
    credentialStorage,
    nativeAvailable,
    supportedProxyProtocols,
    ...(error === undefined ? {} : { error }),
  };
}

function testResult(value: unknown): PluginNetworkTestResult {
  const input = record(value, "pluginNetworkTest");
  exactKeys(input, ["settingsRevision", "checkedAt", "scope", "detail"], "pluginNetworkTest");
  const scope = declaredValue(
    input.scope,
    ["catalog-only", "catalog-and-assets"] as const,
    "pluginNetworkTest.scope",
  );
  const detail = optionalText(input, "detail", "pluginNetworkTest", 2048);
  if (scope === "catalog-only" && detail === undefined)
    throw new HostContractValidationError(
      "pluginNetworkTest.detail",
      "must explain why the asset route was not tested",
    );
  return {
    settingsRevision: nonNegativeInteger(
      input.settingsRevision,
      "pluginNetworkTest.settingsRevision",
    ),
    checkedAt: canonicalIsoTimestamp(input.checkedAt, "pluginNetworkTest.checkedAt"),
    scope,
    ...(detail === undefined ? {} : { detail }),
  };
}

function progress(value: unknown): PluginAcquisitionProgress {
  const input = record(value, "pluginNetworkProgress");
  exactKeys(
    input,
    ["requestId", "operation", "phase", "state", "receivedBytes", "totalBytes"],
    "pluginNetworkProgress",
  );
  const bytes = (value: unknown, path: string): number => {
    const parsed = nonNegativeInteger(value, path);
    if (parsed > PLUGIN_NETWORK_LIMITS.transferBytes)
      throw new HostContractValidationError(path, "exceeds the plugin transfer bound");
    return parsed;
  };
  const receivedBytes =
    input.receivedBytes === undefined
      ? undefined
      : bytes(input.receivedBytes, "pluginNetworkProgress.receivedBytes");
  const totalBytes =
    input.totalBytes === undefined
      ? undefined
      : bytes(input.totalBytes, "pluginNetworkProgress.totalBytes");
  if (receivedBytes !== undefined && totalBytes !== undefined && totalBytes < receivedBytes)
    throw new HostContractValidationError(
      "pluginNetworkProgress.totalBytes",
      "must not be smaller than received bytes",
    );
  return {
    requestId: text(input.requestId, "pluginNetworkProgress.requestId", 128),
    operation: declaredValue(
      input.operation,
      ["catalog", "inspect", "test"] as const,
      "pluginNetworkProgress.operation",
    ),
    phase: declaredValue(
      input.phase,
      ["catalog", "download", "verify"] as const,
      "pluginNetworkProgress.phase",
    ),
    state: declaredValue(
      input.state,
      ["running", "succeeded", "cancelled", "failed"] as const,
      "pluginNetworkProgress.state",
    ),
    ...(receivedBytes === undefined ? {} : { receivedBytes }),
    ...(totalBytes === undefined ? {} : { totalBytes }),
  };
}

export function parsePluginNetworkCommand(
  command: HostCommandName,
  requestId: string,
  value: unknown,
  version: typeof HOST_PROTOCOL_VERSION,
): HostCommand | undefined {
  if (!command.startsWith("plugins.network.")) return undefined;
  const payload = record(value, "command.payload");
  if (command === "plugins.network.get" || command === "plugins.network.test") {
    exactKeys(payload, [], "command.payload");
    return { command, id: requestId, version, payload: {} };
  }
  if (command === "plugins.network.update")
    return {
      command,
      id: requestId,
      version,
      payload: networkValue(() => parsePluginNetworkUpdateInput(payload), "command.payload"),
    };
  if (command === "plugins.network.cancel") {
    exactKeys(payload, ["requestId"], "command.payload");
    return {
      command,
      id: requestId,
      version,
      payload: { requestId: text(payload.requestId, "command.payload.requestId", 128) },
    };
  }
  return undefined;
}

export function parsePluginNetworkResponse(
  command: HostCommandName,
  requestId: string,
  value: unknown,
  version: typeof HOST_PROTOCOL_VERSION,
): HostCommandResponse | undefined {
  if (
    command !== "plugins.network.get" &&
    command !== "plugins.network.update" &&
    command !== "plugins.network.test"
  )
    return undefined;
  const result = record(value, "response.result");
  const correlationId = text(result.correlationId, "response.result.correlationId", 128);
  const base = { id: requestId, version, ok: true as const };
  if (command === "plugins.network.test") {
    exactKeys(result, ["correlationId", "pluginNetworkTest"], "response.result");
    return {
      ...base,
      command,
      result: { correlationId, pluginNetworkTest: testResult(result.pluginNetworkTest) },
    };
  }
  exactKeys(result, ["correlationId", "pluginNetwork"], "response.result");
  return {
    ...base,
    command,
    result: { correlationId, pluginNetwork: snapshot(result.pluginNetwork) },
  };
}

export function parsePluginNetworkEvent(
  event: HostEventName,
  value: unknown,
  sequence: number,
  version: typeof HOST_PROTOCOL_VERSION,
): HostEvent | undefined {
  if (event !== "plugins.network.progress") return undefined;
  return { event, sequence, version, payload: progress(value) };
}
