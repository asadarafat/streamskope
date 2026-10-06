import type {
  PluginNetworkConfiguration,
  PluginNetworkUpdateInput,
  PluginProxyCredentialsChange,
} from "./contracts";

export const PLUGIN_NETWORK_LIMITS = {
  proxyUrlCharacters: 2048,
  usernameCharacters: 512,
  passwordCharacters: 4096,
  transferBytes: 48 * 1024 * 1024,
} as const;

function record(value: unknown, keys: readonly string[]): Record<string, unknown> {
  if (
    value === null ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    Object.keys(value).some((key) => !keys.includes(key))
  )
    throw new Error("Invalid plugin network settings fields.");
  return value as Record<string, unknown>;
}

/** Return only a canonical HTTP/HTTPS origin; proxy secrets never belong in a URL. */
function proxyOrigin(value: unknown): string | null {
  if (value === null) return null;
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > PLUGIN_NETWORK_LIMITS.proxyUrlCharacters ||
    !/^https?:\/\//iu.test(value) ||
    /[\p{Cc}\s\\?#]/u.test(value)
  )
    throw new Error("Proxy URL must be an HTTP or HTTPS origin without credentials or a path.");
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error("Proxy URL must identify a valid HTTP or HTTPS host and port.");
  }
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.hostname.length === 0 ||
    url.username.length > 0 ||
    url.password.length > 0 ||
    url.pathname !== "/" ||
    url.search.length > 0 ||
    url.hash.length > 0
  )
    throw new Error("Proxy URL must be an HTTP or HTTPS origin without credentials or a path.");
  return url.origin;
}

export function parsePluginNetworkConfiguration(value: unknown): PluginNetworkConfiguration {
  const input = record(value, ["mode", "offline", "proxyUrl"]);
  if (input.mode !== "system" && input.mode !== "custom")
    throw new Error("Plugin network mode must be system or custom.");
  if (typeof input.offline !== "boolean") throw new Error("Plugin offline mode must be a boolean.");
  const proxyUrl = proxyOrigin(input.proxyUrl);
  if (input.mode === "custom" && proxyUrl === null)
    throw new Error("Custom proxy mode requires a proxy origin.");
  return { mode: input.mode, offline: input.offline, proxyUrl };
}

function credentialsChange(value: unknown): PluginProxyCredentialsChange {
  const input = record(value, ["action", "username", "password"]);
  if (input.action === "unchanged" || input.action === "clear") {
    record(value, ["action"]);
    return { action: input.action };
  }
  if (input.action !== "replace") throw new Error("Invalid proxy credentials action.");
  if (
    typeof input.username !== "string" ||
    input.username.trim().length === 0 ||
    input.username.length > PLUGIN_NETWORK_LIMITS.usernameCharacters ||
    /\p{Cc}/u.test(input.username) ||
    typeof input.password !== "string" ||
    input.password.length === 0 ||
    input.password.length > PLUGIN_NETWORK_LIMITS.passwordCharacters ||
    /\p{Cc}/u.test(input.password)
  )
    throw new Error("Proxy credentials must use bounded username and password fields.");
  return { action: "replace", username: input.username, password: input.password };
}

export function parsePluginNetworkUpdateInput(value: unknown): PluginNetworkUpdateInput {
  const input = record(value, ["configuration", "credentials"]);
  const configuration = parsePluginNetworkConfiguration(input.configuration);
  const credentials = credentialsChange(input.credentials);
  if (credentials.action === "replace" && configuration.proxyUrl === null)
    throw new Error("Proxy credentials require a remembered proxy origin.");
  return { configuration, credentials };
}
