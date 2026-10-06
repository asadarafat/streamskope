import type {
  PluginCatalogSnapshot,
  PluginChangePrompt,
  PluginCachedPackage,
  PluginDeliverySnapshot,
  PluginExitPrompt,
  PluginInstallation,
  PluginPackageInspection,
  PluginPackagePublisher,
  PluginPackageReference,
  PluginSnapshot,
} from "../../../plugins/contracts";
import {
  comparePluginVersions,
  parsePluginId,
  parsePluginJson,
  parsePluginManifest,
} from "../../../plugins/validation";

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
  canonicalIsoTimestamp,
  exactKeys,
  optionalText,
  nonNegativeInteger,
  record,
  text,
  truth,
} from "./validation-primitives";
import {
  parsePluginNetworkCommand,
  parsePluginNetworkEvent,
  parsePluginNetworkResponse,
} from "./plugin-network-protocol";

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
function list<T>(
  value: unknown,
  path: string,
  parse: (value: unknown) => T,
  maximum = 64,
): readonly T[] {
  if (!Array.isArray(value) || value.length > maximum)
    throw new HostContractValidationError(path, `must contain at most ${maximum} entries`);
  return value.map(parse);
}

function packageVersion(value: unknown, path: string): string {
  const parsed = text(value, path, 256);
  return pluginValue(() => {
    comparePluginVersions(parsed, parsed);
    return parsed;
  }, path);
}

function digest(value: unknown, path: string): string {
  const parsed = text(value, path, 64);
  if (!/^[a-f0-9]{64}(?![\s\S])/u.test(parsed))
    throw new HostContractValidationError(path, "must be a canonical SHA256 digest");
  return parsed;
}

function packageReference(value: unknown): PluginPackageReference {
  const input = record(value, "pluginPackageReference");
  exactKeys(input, ["pluginId", "version", "sha256"], "pluginPackageReference");
  return {
    pluginId: id(input.pluginId, "pluginPackageReference.pluginId"),
    version: packageVersion(input.version, "pluginPackageReference.version"),
    sha256: digest(input.sha256, "pluginPackageReference.sha256"),
  };
}

function publisher(value: unknown): PluginPackagePublisher {
  const input = record(value, "pluginPublisher");
  exactKeys(input, ["keyId", "name"], "pluginPublisher");
  const keyId = text(input.keyId, "pluginPublisher.keyId", 80);
  if (!/^[a-z][a-z0-9]*(?:-[a-z0-9]+)*(?![\s\S])/u.test(keyId))
    throw new HostContractValidationError(
      "pluginPublisher.keyId",
      "must be a canonical publisher identifier",
    );
  return { keyId, name: text(input.name, "pluginPublisher.name", 256) };
}

function trust(
  input: Record<string, unknown>,
  path: string,
): {
  readonly trust: PluginCachedPackage["trust"];
  readonly publisher?: PluginPackagePublisher;
} {
  const parsed = declaredValue(
    input.trust,
    ["publisher", "official", "development"] as const,
    `${path}.trust`,
  );
  const verifiedPublisher = input.publisher === undefined ? undefined : publisher(input.publisher);
  if ((parsed === "publisher") !== (verifiedPublisher !== undefined))
    throw new HostContractValidationError(
      `${path}.publisher`,
      "must be present only for publisher-verified packages",
    );
  return {
    trust: parsed,
    ...(verifiedPublisher === undefined ? {} : { publisher: verifiedPublisher }),
  };
}

function cachedPackage(value: unknown): PluginCachedPackage {
  const input = record(value, "pluginCachedPackage");
  exactKeys(input, ["manifest", "sha256", "cachedAt", "publisher", "trust"], "pluginCachedPackage");
  return {
    manifest: pluginValue(
      () => parsePluginManifest(input.manifest),
      "pluginCachedPackage.manifest",
    ),
    sha256: digest(input.sha256, "pluginCachedPackage.sha256"),
    cachedAt: canonicalIsoTimestamp(input.cachedAt, "pluginCachedPackage.cachedAt"),
    ...trust(input, "pluginCachedPackage"),
  };
}

function delivery(value: unknown): PluginDeliverySnapshot {
  const input = record(value, "pluginDelivery");
  exactKeys(input, ["fileInstallationAvailable", "cachedPackages"], "pluginDelivery");
  const cachedPackages = list(
    input.cachedPackages,
    "pluginDelivery.cachedPackages",
    cachedPackage,
    4,
  );
  const identities = cachedPackages.map(
    (entry) => `${entry.manifest.id}:${entry.manifest.version}:${entry.sha256}`,
  );
  if (new Set(identities).size !== identities.length)
    throw new HostContractValidationError(
      "pluginDelivery.cachedPackages",
      "must not contain duplicate packages",
    );
  return {
    fileInstallationAvailable: truth(
      input.fileInstallationAvailable,
      "pluginDelivery.fileInstallationAvailable",
    ),
    cachedPackages,
  };
}

function inspection(value: unknown): PluginPackageInspection | null {
  if (value === null) return null;
  const input = record(value, "pluginPackage");
  exactKeys(
    input,
    [
      "candidateId",
      "manifest",
      "sha256",
      "source",
      "publisher",
      "trust",
      "expiresAt",
      "installedVersion",
      "status",
      "reason",
    ],
    "pluginPackage",
  );
  const parsedTrust = trust(input, "pluginPackage");
  const source = declaredValue(
    input.source,
    ["file", "catalog", "cache"] as const,
    "pluginPackage.source",
  );
  if (source === "file" && parsedTrust.trust !== "publisher")
    throw new HostContractValidationError(
      "pluginPackage.trust",
      "file installation requires a verified publisher",
    );
  const reason = optionalText(input, "reason", "pluginPackage", 4096);
  const status = declaredValue(
    input.status,
    ["install", "update", "already-installed", "blocked"] as const,
    "pluginPackage.status",
  );
  if (status === "blocked" && reason === undefined)
    throw new HostContractValidationError(
      "pluginPackage.reason",
      "must explain why the package cannot be installed",
    );
  return {
    candidateId: text(input.candidateId, "pluginPackage.candidateId", 128),
    manifest: pluginValue(() => parsePluginManifest(input.manifest), "pluginPackage.manifest"),
    sha256: digest(input.sha256, "pluginPackage.sha256"),
    source,
    ...parsedTrust,
    expiresAt: canonicalIsoTimestamp(input.expiresAt, "pluginPackage.expiresAt"),
    ...(input.installedVersion === undefined
      ? {}
      : {
          installedVersion: packageVersion(
            input.installedVersion,
            "pluginPackage.installedVersion",
          ),
        }),
    status,
    ...(reason === undefined ? {} : { reason }),
  };
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
  exactKeys(input, ["plugins", "packages", "error", "source", "checkedAt"], "pluginCatalog");
  const error = optionalText(input, "error", "pluginCatalog", 4096);
  const checkedAt = optionalText(input, "checkedAt", "pluginCatalog", 32);
  if (
    checkedAt !== undefined &&
    (!Number.isFinite(Date.parse(checkedAt)) || new Date(checkedAt).toISOString() !== checkedAt)
  )
    throw new HostContractValidationError(
      "pluginCatalog.checkedAt",
      "must be an ISO UTC timestamp",
    );
  const plugins = list(input.plugins, "pluginCatalog.plugins", (entry) =>
    pluginValue(() => parsePluginManifest(entry), "pluginCatalog.plugins"),
  );
  const packages =
    input.packages === undefined
      ? undefined
      : list(input.packages, "pluginCatalog.packages", packageReference, 2);
  if (
    packages !== undefined &&
    (new Set(packages.map((entry) => entry.pluginId)).size !== packages.length ||
      packages.some(
        (entry) =>
          !plugins.some(
            (manifest) => manifest.id === entry.pluginId && manifest.version === entry.version,
          ),
      ))
  )
    throw new HostContractValidationError(
      "pluginCatalog.packages",
      "must reference distinct declared manifests",
    );
  return {
    plugins,
    ...(packages === undefined ? {} : { packages }),
    ...(error === undefined ? {} : { error }),
    ...(checkedAt === undefined ? {} : { checkedAt }),
    ...(input.source === undefined
      ? {}
      : {
          source: declaredValue(
            input.source,
            ["live", "cache", "unavailable"] as const,
            "pluginCatalog.source",
          ),
        }),
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
  const networkCommand = parsePluginNetworkCommand(command, requestId, value, version);
  if (networkCommand !== undefined) return networkCommand;
  if (command !== "plugin.execute" && !command.startsWith("plugins.")) return undefined;
  const payload = record(value, "command.payload");
  switch (command) {
    case "plugins.list":
    case "plugins.delivery":
    case "plugins.restart":
    case "plugins.exit.prepare":
      exactKeys(payload, [], "command.payload");
      return { command, id: requestId, version, payload: {} };
    case "plugins.package.inspect": {
      const source = declaredValue(
        payload.source,
        ["file", "catalog", "cache"] as const,
        "command.payload.source",
      );
      if (source === "file") {
        exactKeys(payload, ["source"], "command.payload");
        return { command, id: requestId, version, payload: { source } };
      }
      exactKeys(payload, ["source", "pluginId", "version", "sha256"], "command.payload");
      return {
        command,
        id: requestId,
        version,
        payload: {
          source,
          pluginId: id(payload.pluginId, "command.payload.pluginId"),
          version: packageVersion(payload.version, "command.payload.version"),
          sha256: digest(payload.sha256, "command.payload.sha256"),
        },
      };
    }
    case "plugins.package.change.prepare":
    case "plugins.package.discard":
      exactKeys(payload, ["candidateId"], "command.payload");
      return {
        command,
        id: requestId,
        version,
        payload: { candidateId: text(payload.candidateId, "command.payload.candidateId", 128) },
      };
    case "plugins.package.install": {
      exactKeys(payload, ["candidateId", "confirmationToken"], "command.payload");
      const confirmationToken = optionalText(payload, "confirmationToken", "command.payload", 128);
      return {
        command,
        id: requestId,
        version,
        payload: {
          candidateId: text(payload.candidateId, "command.payload.candidateId", 128),
          ...(confirmationToken === undefined ? {} : { confirmationToken }),
        },
      };
    }
    case "plugins.catalog":
      exactKeys(payload, ["refresh"], "command.payload");
      return {
        command,
        id: requestId,
        version,
        payload: {
          ...(payload.refresh === undefined
            ? {}
            : { refresh: truth(payload.refresh, "command.payload.refresh") }),
        },
      };
    case "plugins.install":
    case "plugins.retry":
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
            ["install", "remove", "retry"] as const,
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
  const networkResponse = parsePluginNetworkResponse(command, requestId, value, version);
  if (networkResponse !== undefined) return networkResponse;
  if (
    command !== "plugin.execute" &&
    (!command.startsWith("plugins.") ||
      command === "plugins.restart" ||
      command === "plugins.network.cancel" ||
      command === "plugins.package.discard")
  )
    return undefined;
  const result = record(value, "response.result");
  const correlationId = text(result.correlationId, "response.result.correlationId", 128);
  const base = { id: requestId, version, ok: true as const };
  switch (command) {
    case "plugins.list":
    case "plugins.install":
    case "plugins.package.install":
    case "plugins.retry":
    case "plugins.remove":
    case "plugins.renderer.failed":
      exactKeys(result, ["correlationId", "pluginSnapshot"], "response.result");
      return {
        ...base,
        command,
        result: { correlationId, pluginSnapshot: snapshot(result.pluginSnapshot) },
      };
    case "plugins.change.prepare":
    case "plugins.package.change.prepare":
      exactKeys(result, ["correlationId", "pluginChange"], "response.result");
      return {
        ...base,
        command,
        result: { correlationId, pluginChange: changePrompt(result.pluginChange) },
      };
    case "plugins.delivery":
      exactKeys(result, ["correlationId", "pluginDelivery"], "response.result");
      return {
        ...base,
        command,
        result: { correlationId, pluginDelivery: delivery(result.pluginDelivery) },
      };
    case "plugins.package.inspect":
      exactKeys(result, ["correlationId", "pluginPackage"], "response.result");
      return {
        ...base,
        command,
        result: { correlationId, pluginPackage: inspection(result.pluginPackage) },
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
  const networkEvent = parsePluginNetworkEvent(event, value, sequence, version);
  if (networkEvent !== undefined) return networkEvent;
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
