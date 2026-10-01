import {
  formatPluginVersion,
  parsePluginCompatibility,
  parseReleaseVersion,
  parseSemanticPluginVersion,
} from "./compatibility";
import {
  PLUGIN_API_VERSION,
  type JsonObject,
  type JsonValue,
  type PluginApiVersion,
  type PluginManifest,
  type PluginResource,
  type PluginProfileSource,
} from "./contracts";

export {
  compareDesktopReleases,
  comparePluginVersions,
  comparePluginManifests,
  formatPluginVersion,
  isPluginCompatibleWithHost,
  isTargetVersionCompatible,
  isPrereleaseVersion,
} from "./compatibility";

const identifier = /^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*(?![\s\S])/u;

export function parsePluginId(value: unknown): string {
  if (typeof value !== "string" || value.length > 128 || !identifier.test(value)) {
    throw new Error("Invalid plugin identifier.");
  }
  return value;
}

function record(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Expected a plugin object.");
  }
  return value as Record<string, unknown>;
}

function text(value: unknown, name: string, maximum = 256): string {
  if (typeof value !== "string" || value.trim().length === 0 || value.length > maximum) {
    throw new Error(`Invalid plugin ${name}.`);
  }
  return value;
}

export function isSupportedPluginApiVersion(value: unknown): value is PluginApiVersion {
  return value === 2 || value === 3 || value === PLUGIN_API_VERSION;
}

function parseResources(value: unknown): readonly PluginResource[] {
  if (!Array.isArray(value) || value.length > 8) {
    throw new Error("Plugin resources must be an array with at most eight entries.");
  }
  const paths = new Set<string>();
  return value.map((item: unknown) => {
    const resource = record(item);
    if (Object.keys(resource).some((key) => key !== "path" && key !== "sha256")) {
      throw new Error("Plugin resource contains unsupported fields.");
    }
    if (
      typeof resource.path !== "string" ||
      resource.path.length > 128 ||
      !/^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*\.(?:yaml|json|md)(?![\s\S])/u.test(resource.path) ||
      resource.path === "manifest.json" ||
      paths.has(resource.path)
    ) {
      throw new Error("Plugin resource paths must be unique, flat, canonical data filenames.");
    }
    if (typeof resource.sha256 !== "string" || !/^[a-f0-9]{64}(?![\s\S])/u.test(resource.sha256)) {
      throw new Error("Plugin resource requires a canonical SHA-256 digest.");
    }
    paths.add(resource.path);
    return { path: resource.path, sha256: resource.sha256 };
  });
}

export function parsePluginManifest(value: unknown): PluginManifest {
  const input = record(value);
  if (!isSupportedPluginApiVersion(input.apiVersion)) {
    throw new Error(
      "This plugin requires a different StreamSkope plugin API. Update StreamSkope or select a compatible plugin.",
    );
  }
  const allowed = new Set([
    "id",
    "name",
    "description",
    "version",
    "apiVersion",
    "backend",
    "renderer",
    "styles",
    ...(input.apiVersion === 2 ? ["targetEdaVersion"] : ["compatibility", "resources"]),
    ...(input.apiVersion === 3 ? ["revision"] : []),
  ]);
  if (Object.keys(input).some((key) => !allowed.has(key))) {
    throw new Error("Plugin manifest contains unsupported fields.");
  }
  if (
    input.backend !== "backend.cjs" ||
    input.renderer !== "renderer.js" ||
    (input.styles !== undefined && input.styles !== "renderer.css")
  ) {
    throw new Error("Plugin entrypoints must use the supported package filenames.");
  }
  const compatibility =
    input.apiVersion === 2
      ? undefined
      : parsePluginCompatibility(input.compatibility, input.apiVersion);
  let packageVersion: string;
  if (compatibility === undefined) {
    packageVersion = parseSemanticPluginVersion(input.version);
  } else if (input.apiVersion === 4) {
    packageVersion = parseReleaseVersion(input.version);
  } else {
    if (typeof input.revision !== "number")
      throw new Error("Plugin revision must be a positive safe integer.");
    packageVersion = formatPluginVersion(compatibility, input.revision);
    if (input.version !== packageVersion) {
      throw new Error(
        "Plugin version must match its canonical compatibility identity and revision.",
      );
    }
  }
  return {
    id: parsePluginId(input.id),
    name: text(input.name, "name"),
    version: packageVersion,
    apiVersion: input.apiVersion,
    backend: "backend.cjs",
    renderer: "renderer.js",
    ...(input.description === undefined
      ? {}
      : { description: text(input.description, "description", 1_024) }),
    ...(input.apiVersion === 2 && input.targetEdaVersion !== undefined
      ? { targetEdaVersion: parseSemanticPluginVersion(input.targetEdaVersion) }
      : {}),
    ...(compatibility === undefined ? {} : { compatibility }),
    ...(input.apiVersion === 3 ? { revision: input.revision as number } : {}),
    ...(input.resources === undefined ? {} : { resources: parseResources(input.resources) }),
    ...(input.styles === undefined ? {} : { styles: "renderer.css" as const }),
  };
}

/** A bounded JSON boundary, shared by the renderer and host; never accepts executable values. */
export function parsePluginJson(value: unknown): JsonValue {
  let nodes = 0;
  let characters = 0;
  const seen = new Set<object>();
  function visit(input: unknown, depth: number): JsonValue {
    if (++nodes > 20_000 || depth > 24)
      throw new Error("Plugin data exceeds its structural limit.");
    if (input === null || typeof input === "boolean") return input;
    if (typeof input === "number" && Number.isFinite(input)) return input;
    if (typeof input === "string") {
      characters += input.length;
      if (characters > 262_144) throw new Error("Plugin data exceeds its text limit.");
      return input;
    }
    if (typeof input !== "object" || input === null || seen.has(input)) {
      throw new Error("Plugin data must contain only finite, acyclic JSON values.");
    }
    seen.add(input);
    try {
      if (Array.isArray(input)) return input.map((item: unknown) => visit(item, depth + 1));
      if (
        Object.getPrototypeOf(input) !== Object.prototype &&
        Object.getPrototypeOf(input) !== null
      ) {
        throw new Error("Plugin data must contain plain objects.");
      }
      const output: Record<string, JsonValue> = {};
      for (const [key, item] of Object.entries(input)) {
        characters += key.length;
        if (characters > 262_144 || ["__proto__", "prototype", "constructor"].includes(key)) {
          throw new Error("Plugin data contains an unsupported key or exceeds its text limit.");
        }
        output[key] = visit(item, depth + 1);
      }
      return output;
    } finally {
      seen.delete(input);
    }
  }
  return visit(value, 0);
}

export function parsePluginProfileSource(value: unknown): PluginProfileSource {
  const input = record(value);
  if (input.kind !== "plugin" || input.version !== 1)
    throw new Error("Unsupported plugin profile source.");
  const data = parsePluginJson(input.data);
  if (data === null || typeof data !== "object" || Array.isArray(data)) {
    throw new Error("Plugin profile data must be an object.");
  }
  return {
    kind: "plugin",
    pluginId: parsePluginId(input.pluginId),
    version: 1,
    data: data as JsonObject,
  };
}
