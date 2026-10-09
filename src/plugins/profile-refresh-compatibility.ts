import type { HostCommand, ProfileSummary } from "../features/kafka/contracts";

export const PLUGIN_PROFILE_REFRESH_UPGRADE =
  "This plugin refresh does not preserve the profile's authentication or certificate settings. " +
  "The saved profile is unchanged. Update the plugin in Preferences > Plugins and retry. " +
  "To remove these settings deliberately, use the connection profile editor first.";

type Fields = Record<string, unknown>;
function fields(value: unknown): Fields | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Fields)
    : undefined;
}
function protectedValue(value: unknown): boolean {
  const input = fields(value);
  return (
    input?.mode === "retain" ||
    input?.mode === "clear" ||
    (input?.mode === "replace" && typeof input.value === "string")
  );
}
function identity(value: unknown): boolean {
  const input = fields(value);
  return (
    input !== undefined &&
    protectedValue(input.certificatePem) &&
    protectedValue(input.privateKeyPem) &&
    protectedValue(input.passphrase)
  );
}
function endpoint(value: unknown): boolean {
  const input = fields(value);
  if (input === undefined) return false;
  if ("bearerPresent" in input) return false;
  if (input.authentication === "basic" && !protectedValue(fields(input.basic)?.password))
    return false;
  if (input.authentication === "bearer" && !protectedValue(input.bearer)) return false;
  if (input.authentication === "oauth-client" && !protectedValue(fields(input.oauth)?.clientSecret))
    return false;
  const trust = fields(input.trust);
  if (
    trust?.mode === "custom" &&
    (!protectedValue(trust.material) || !protectedValue(trust.password))
  )
    return false;
  return input.clientIdentity === undefined || identity(input.clientIdentity);
}

/** Reject old summary-copy/omission behavior; never infer credential intent or rewrite requests. */
export function compatiblePluginProfileRefresh(value: unknown, current?: ProfileSummary): boolean {
  const input = fields(value);
  if (input === undefined) return true; // The host contract owns malformed unrelated inputs.
  if (input.sasl !== undefined && !protectedValue(fields(input.sasl)?.password)) return false;
  if (input.clientIdentity !== undefined && !identity(input.clientIdentity)) return false;
  const services = fields(input.services);
  if (services !== undefined && Object.values(services).some((service) => !endpoint(service)))
    return false;
  if (current?.sasl !== undefined && input.sasl === undefined) return false;
  if (current?.clientIdentity !== undefined && input.clientIdentity === undefined) return false;
  for (const key of ["schemaRegistry", "connect", "redpandaAdmin"] as const) {
    const saved = current?.services?.[key];
    if (saved === undefined) continue;
    const expanded =
      saved.trust !== undefined ||
      saved.clientIdentity !== undefined ||
      !["none", "oauth"].includes(saved.authentication);
    if (!expanded) continue;
    const incoming = fields(services?.[key]);
    if (incoming === undefined || incoming.authentication !== saved.authentication) return false;
    if (saved.trust !== undefined && fields(incoming.trust)?.mode !== saved.trust.mode)
      return false;
    if (saved.clientIdentity !== undefined && !identity(incoming.clientIdentity)) return false;
  }
  return true;
}

/** Common dispatch check for the backend SDK and renderer host adapters. */
export function compatiblePluginProfileCommand(
  command: HostCommand,
  profiles: readonly ProfileSummary[] = [],
): boolean {
  if (command.command === "profiles.update") {
    const profileId = command.payload.profileId;
    return compatiblePluginProfileRefresh(
      command.payload.profile,
      profiles.find((profile) => profile.id === profileId),
    );
  }
  if (command.command === "profiles.test") {
    const profileId = command.payload.mode === "update" ? command.payload.profileId : undefined;
    return compatiblePluginProfileRefresh(
      command.payload.profile,
      profiles.find((profile) => profile.id === profileId),
    );
  }
  return (
    command.command !== "profiles.create" || compatiblePluginProfileRefresh(command.payload.profile)
  );
}
