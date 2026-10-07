import type { NatsProfileStoreCapability, NatsProfileSummary } from "../contracts";

export function natsProfileStorageLabel(
  capability: NatsProfileStoreCapability | undefined,
): string {
  if (capability === undefined) return "Profile storage capability is not yet available.";
  if (capability.state === "unavailable")
    return `Profile storage unavailable. ${capability.recovery ?? "Restore protected storage and retry."}`;
  if (capability.durability === "session" && capability.protection === "memory")
    return "Session profiles · credentials held in memory. Profiles are lost when this development host restarts.";
  if (capability.protection === "passphrase-protected")
    return "Durable profiles · credentials encrypted with your unlocked passphrase vault.";
  return "Durable profiles · credentials protected by the operating system.";
}

/** Capture an edit/delete revision and safe settings without hydrating credentials. */
export function captureNatsProfile(profile: NatsProfileSummary): NatsProfileSummary {
  return {
    ...profile,
    servers: [...profile.servers],
    authentication: { ...profile.authentication },
    tls: { ...profile.tls },
  };
}
