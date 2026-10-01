import type {
  PluginCatalogSnapshot,
  PluginChangePrompt,
  PluginExitPrompt,
  PluginInstallation,
  PluginSnapshot,
} from "../../../plugins/contracts";
import { parsePluginId, parsePluginJson, parsePluginManifest } from "../../../plugins/validation";

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
  declaredValue,
  exactKeys,
  optionalText,
  nonNegativeInteger,
  record,
  text,
  truth,
} from "./validation-primitives";

function pluginValue<T>(parse: () => T, path: string): T {
  try {
    return parse();
  } catch (error) {
    throw new HostContractValidationError(
      path,
      error instanceof Error ? error.message : "invalid plugin data",
    );
  }
}
function id(value: unknown, path: string): string {
  return pluginValue(() => parsePluginId(value), path);
}
function list<T>(value: unknown, path: string, parse: (value: unknown) => T): readonly T[] {
  if (!Array.isArray(value) || value.length > 64)
    throw new HostContractValidationError(path, "must contain at most 64 entries");
  return value.map(parse);
}
function installation(value: unknown): PluginInstallation {
  const input = record(value, "plugin");
  exactKeys(
    input,
    [
      "id",
      "activationId",
      "installed",
      "active",
      "previous",
      "pending",
      "restartRequired",
      "error",
      "rendererUrl",
      "stylesUrl",
    ],
    "plugin",
  );
  const error = optionalText(input, "error", "plugin", 4096);
  const activationId = optionalText(input, "activationId", "plugin", 128);
  const rendererUrl = optionalText(input, "rendererUrl", "plugin", 4096);
  const stylesUrl = optionalText(input, "stylesUrl", "plugin", 4096);
  return {
    id: id(input.id, "plugin.id"),
    ...(activationId === undefined ? {} : { activationId }),
    pending:
      input.pending === null
        ? null
        : declaredValue(input.pending, ["install", "remove"] as const, "plugin.pending"),
    restartRequired: truth(input.restartRequired, "plugin.restartRequired"),
    ...(input.installed === undefined
      ? {}
      : { installed: pluginValue(() => parsePluginManifest(input.installed), "plugin.installed") }),
    ...(input.active === undefined
      ? {}
      : { active: pluginValue(() => parsePluginManifest(input.active), "plugin.active") }),
    ...(input.previous === undefined
      ? {}
      : { previous: pluginValue(() => parsePluginManifest(input.previous), "plugin.previous") }),
    ...(error === undefined ? {} : { error }),
    ...(rendererUrl === undefined ? {} : { rendererUrl }),
    ...(stylesUrl === undefined ? {} : { stylesUrl }),
  };
}
function snapshot(value: unknown): PluginSnapshot {
  const input = record(value, "pluginSnapshot");
  exactKeys(input, ["plugins", "error", "revision"], "pluginSnapshot");
  const error = optionalText(input, "error", "pluginSnapshot", 4096);
  return {
    revision: nonNegativeInteger(input.revision, "pluginSnapshot.revision"),
    plugins: list(input.plugins, "pluginSnapshot.plugins", installation),
    ...(error === undefined ? {} : { error }),
  };
}
function changePrompt(value: unknown): PluginChangePrompt | null {
  if (value === null) return null;
  const input = record(value, "pluginChange");
  exactKeys(
    input,
    ["pluginId", "token", "title", "message", "detail", "confirmLabel"],
    "pluginChange",
  );
  return {
    pluginId: id(input.pluginId, "pluginChange.pluginId"),
    token: text(input.token, "pluginChange.token", 128),
    title: text(input.title, "pluginChange.title", 256),
    message: text(input.message, "pluginChange.message", 2048),
    detail: text(input.detail, "pluginChange.detail", 8192),
    confirmLabel: text(input.confirmLabel, "pluginChange.confirmLabel", 256),
  };
}
function catalog(value: unknown): PluginCatalogSnapshot {
  const input = record(value, "pluginCatalog");
  exactKeys(input, ["plugins", "error"], "pluginCatalog");
  const error = optionalText(input, "error", "pluginCatalog", 4096);
  return {
    plugins: list(input.plugins, "pluginCatalog.plugins", (entry) =>
      pluginValue(() => parsePluginManifest(entry), "pluginCatalog.plugins"),
    ),
    ...(error === undefined ? {} : { error }),
  };
}
function exitPrompt(value: unknown): PluginExitPrompt | null {
  if (value === null) return null;
  const input = record(value, "pluginExit");
  exactKeys(
    input,
    ["pluginId", "title", "message", "detail", "actions", "cancelAction"],
    "pluginExit",
  );
  return {
    pluginId: id(input.pluginId, "pluginExit.pluginId"),
    title: text(input.title, "pluginExit.title", 256),
    message: text(input.message, "pluginExit.message", 2048),
    detail: text(input.detail, "pluginExit.detail", 8192),
    cancelAction: text(input.cancelAction, "pluginExit.cancelAction", 128),
    actions: list(input.actions, "pluginExit.actions", (value) => {
      const action = record(value, "action");
      exactKeys(action, ["id", "label"], "action");
      return {
        id: text(action.id, "action.id", 128),
        label: text(action.label, "action.label", 256),
      };
    }),
  };
}
export function parsePluginHostCommand(
  command: HostCommandName,
  requestId: string,
  value: unknown,
  version: typeof HOST_PROTOCOL_VERSION,
): HostCommand | undefined {
  if (command !== "plugin.execute" && !command.startsWith("plugins.")) return undefined;
  const payload = record(value, "command.payload");
  switch (command) {
    case "plugins.list":
    case "plugins.catalog":
    case "plugins.restart":
    case "plugins.exit.prepare":
      exactKeys(payload, [], "command.payload");
      return { command, id: requestId, version, payload: {} };
    case "plugins.install":
    case "plugins.remove":
      exactKeys(payload, ["pluginId", "confirmationToken"], "command.payload");
      return {
        command,
        id: requestId,
        version,
        payload: {
          pluginId: id(payload.pluginId, "command.payload.pluginId"),
          ...(payload.confirmationToken === undefined
            ? {}
            : {
                confirmationToken: text(
                  payload.confirmationToken,
                  "command.payload.confirmationToken",
                  128,
                ),
              }),
        },
      };
    case "plugins.change.prepare":
      exactKeys(payload, ["pluginId", "operation"], "command.payload");
      return {
        command,
        id: requestId,
        version,
        payload: {
          pluginId: id(payload.pluginId, "command.payload.pluginId"),
          operation: declaredValue(
            payload.operation,
            ["install", "remove"] as const,
            "command.payload.operation",
          ),
        },
      };
    case "plugins.renderer.failed":
      exactKeys(payload, ["pluginId", "activationId", "error"], "command.payload");
      return {
        command,
        id: requestId,
        version,
        payload: {
          pluginId: id(payload.pluginId, "command.payload.pluginId"),
          activationId: text(payload.activationId, "command.payload.activationId", 128),
          error: text(payload.error, "command.payload.error", 4096),
        },
      };
    case "plugins.exit.resolve":
      exactKeys(payload, ["pluginId", "action"], "command.payload");
      return {
        command,
        id: requestId,
        version,
        payload: {
          pluginId: id(payload.pluginId, "command.payload.pluginId"),
          action: text(payload.action, "command.payload.action", 128),
        },
      };
    case "plugin.execute":
      exactKeys(payload, ["pluginId", "method", "input", "activationId"], "command.payload");
      return {
        command,
        id: requestId,
        version,
        payload: {
          pluginId: id(payload.pluginId, "command.payload.pluginId"),
          ...(payload.activationId === undefined
            ? {}
            : { activationId: text(payload.activationId, "command.payload.activationId", 128) }),
          method: text(payload.method, "command.payload.method", 128),
          input: pluginValue(() => parsePluginJson(payload.input), "command.payload.input"),
        },
      };
  }
  return undefined;
}
export function parsePluginHostResponse(
  command: HostCommandName,
  requestId: string,
  value: unknown,
  version: typeof HOST_PROTOCOL_VERSION,
): HostCommandResponse | undefined {
  if (
    command !== "plugin.execute" &&
    (!command.startsWith("plugins.") || command === "plugins.restart")
  )
    return undefined;
  const result = record(value, "response.result");
  const correlationId = text(result.correlationId, "response.result.correlationId", 128);
  const base = { id: requestId, version, ok: true as const };
  switch (command) {
    case "plugins.list":
    case "plugins.install":
    case "plugins.remove":
    case "plugins.renderer.failed":
      exactKeys(result, ["correlationId", "pluginSnapshot"], "response.result");
      return {
        ...base,
        command,
        result: { correlationId, pluginSnapshot: snapshot(result.pluginSnapshot) },
      };
    case "plugins.change.prepare":
      exactKeys(result, ["correlationId", "pluginChange"], "response.result");
      return {
        ...base,
        command,
        result: { correlationId, pluginChange: changePrompt(result.pluginChange) },
      };
    case "plugins.catalog":
      exactKeys(result, ["correlationId", "pluginCatalog"], "response.result");
      return {
        ...base,
        command,
        result: { correlationId, pluginCatalog: catalog(result.pluginCatalog) },
      };
    case "plugins.exit.prepare":
      exactKeys(result, ["correlationId", "pluginExit"], "response.result");
      return {
        ...base,
        command,
        result: { correlationId, pluginExit: exitPrompt(result.pluginExit) },
      };
    case "plugins.exit.resolve":
      exactKeys(result, ["correlationId", "allowed"], "response.result");
      return {
        ...base,
        command,
        result: { correlationId, allowed: truth(result.allowed, "response.result.allowed") },
      };
    case "plugin.execute":
      exactKeys(result, ["correlationId", "output"], "response.result");
      return {
        ...base,
        command,
        result: {
          correlationId,
          output: pluginValue(() => parsePluginJson(result.output), "response.result.output"),
        },
      };
  }
  return undefined;
}
export function parsePluginHostEvent(
  event: HostEventName,
  value: unknown,
  sequence: number,
  version: typeof HOST_PROTOCOL_VERSION,
): HostEvent | undefined {
  if (event === "plugins.changed") return { event, sequence, version, payload: snapshot(value) };
  if (event !== "plugin.event") return undefined;
  const payload = record(value, "event.payload");
  exactKeys(payload, ["pluginId", "name", "data"], "event.payload");
  return {
    event,
    sequence,
    version,
    payload: {
      pluginId: id(payload.pluginId, "event.payload.pluginId"),
      name: text(payload.name, "event.payload.name", 128),
      data: pluginValue(() => parsePluginJson(payload.data), "event.payload.data"),
    },
  };
}
