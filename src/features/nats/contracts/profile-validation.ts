import {
  NATS_LIMITS,
  type NatsCreateSecret,
  type NatsUpdateSecret,
  type NatsProfileAuthenticationInput,
  type NatsProfileTlsInput,
  type NatsProfileCreateInput,
  type NatsProfileUpdateInput,
  type NatsProfileStoreCapability,
  type NatsProfileSummary,
  type NatsProfilesSnapshot,
} from "./types";
import {
  NatsContractValidationError,
  natsArray,
  natsBoolean,
  natsCanonicalBase64,
  natsEnum,
  natsExactKeys,
  natsHasAsciiControl,
  natsIdentifier,
  natsInteger,
  natsObject,
  natsText,
  natsTimestamp,
} from "./validation-primitives";

export function parseNatsName(value: unknown): string {
  const name = natsText(value, NATS_LIMITS.nameBytes).trim();
  if (name.length === 0 || natsHasAsciiControl(name)) throw new NatsContractValidationError();
  return name;
}
export function parseNatsToken(value: unknown): string {
  const token = natsText(value, NATS_LIMITS.tokenBytes);
  if (natsHasAsciiControl(token)) throw new NatsContractValidationError();
  return token;
}
/** Canonical PEM limits JSON expansion; actual certificate/TLS validity belongs to the engine. */
export function parseNatsCaPem(value: unknown): string {
  const pem = natsText(value, NATS_LIMITS.caPemBytes);
  const blocks: string[] = [];
  const pattern = /-----BEGIN CERTIFICATE-----([A-Za-z0-9+/=\r\n ]+)-----END CERTIFICATE-----/gu;
  let end = 0;
  for (const match of pem.matchAll(pattern)) {
    if (pem.slice(end, match.index).trim().length !== 0) throw new NatsContractValidationError();
    const body = match[1]!.replace(/[\r\n ]/gu, "");
    if (natsCanonicalBase64(body, NATS_LIMITS.caPemBytes).bytes === 0)
      throw new NatsContractValidationError();
    blocks.push(
      `-----BEGIN CERTIFICATE-----\n${body.match(/.{1,64}/gu)!.join("\n")}\n-----END CERTIFICATE-----`,
    );
    end = match.index + match[0].length;
  }
  if (blocks.length === 0 || pem.slice(end).trim().length !== 0)
    throw new NatsContractValidationError();
  return natsText(`${blocks.join("\n")}\n`, NATS_LIMITS.caPemBytes);
}
export function parseNatsServers(
  value: unknown,
  transport: "plaintext" | "tls",
): readonly string[] {
  const inputs = natsArray(value, NATS_LIMITS.servers);
  if (inputs.length === 0) throw new NatsContractValidationError();
  const servers = inputs.map((input): string => {
    const text = natsText(input, NATS_LIMITS.serverBytes);
    let server: URL;
    try {
      server = new URL(text);
    } catch {
      throw new NatsContractValidationError();
    }
    if (
      !["nats:", "tls:"].includes(server.protocol) ||
      server.username !== "" ||
      server.password !== "" ||
      server.search !== "" ||
      server.hash !== "" ||
      !["", "/"].includes(server.pathname) ||
      server.hostname.length === 0 ||
      server.hostname === "." ||
      (transport === "plaintext" && server.protocol === "tls:")
    )
      throw new NatsContractValidationError();
    const port = server.port === "" ? 4222 : Number(server.port);
    if (!Number.isInteger(port) || port < 1 || port > 65535)
      throw new NatsContractValidationError();
    return natsText(`${server.protocol}//${server.hostname}:${port}`, NATS_LIMITS.serverBytes);
  });
  if (new Set(servers).size !== servers.length) throw new NatsContractValidationError();
  return servers;
}

function secret(value: unknown, update: boolean, kind: "token" | "ca"): NatsUpdateSecret {
  const input = natsObject(value);
  const mode = natsEnum(
    input.mode,
    update ? (["clear", "replace", "retain"] as const) : (["clear", "replace"] as const),
  );
  natsExactKeys(input, mode === "replace" ? ["mode", "value"] : ["mode"]);
  if (mode === "replace")
    return {
      mode,
      value: kind === "token" ? parseNatsToken(input.value) : parseNatsCaPem(input.value),
    };
  if (kind === "token" && mode === "clear") throw new NatsContractValidationError();
  return { mode };
}
function authentication(
  value: unknown,
  update: boolean,
): NatsProfileAuthenticationInput<NatsUpdateSecret> {
  const input = natsObject(value);
  const mode = natsEnum(input.mode, ["none", "token"] as const);
  natsExactKeys(input, mode === "none" ? ["mode"] : ["mode", "token"]);
  return mode === "none" ? { mode } : { mode, token: secret(input.token, update, "token") };
}
function tls(value: unknown, update: boolean): NatsProfileTlsInput<NatsUpdateSecret> {
  const input = natsObject(value);
  const mode = natsEnum(input.mode, ["plaintext", "tls"] as const);
  natsExactKeys(input, mode === "plaintext" ? ["mode"] : ["mode", "caPem"]);
  return mode === "plaintext" ? { mode } : { mode, caPem: secret(input.caPem, update, "ca") };
}
export function parseNatsProfileUpdateInput(value: unknown): NatsProfileUpdateInput {
  const input = natsObject(value);
  natsExactKeys(input, ["name", "servers", "authentication", "tls"]);
  const security = tls(input.tls, true);
  return {
    name: parseNatsName(input.name),
    servers: parseNatsServers(input.servers, security.mode),
    authentication: authentication(input.authentication, true),
    tls: security,
  };
}
export function parseNatsProfileCreateInput(value: unknown): NatsProfileCreateInput {
  const input = natsObject(value);
  natsExactKeys(input, ["name", "servers", "authentication", "tls"]);
  const security = tls(input.tls, false);
  // The create-only parser excludes retain before narrowing these validated values.
  return {
    name: parseNatsName(input.name),
    servers: parseNatsServers(input.servers, security.mode),
    authentication: authentication(
      input.authentication,
      false,
    ) as NatsProfileAuthenticationInput<NatsCreateSecret>,
    tls: security as NatsProfileTlsInput<NatsCreateSecret>,
  };
}
export function parseNatsProfileStoreCapability(value: unknown): NatsProfileStoreCapability {
  const input = natsObject(value);
  natsExactKeys(input, ["durability", "protection", "state"], ["recovery"]);
  const durability = natsEnum(input.durability, ["durable", "session"] as const);
  const protection = natsEnum(input.protection, ["memory", "os-protected", "unavailable"] as const);
  const state = natsEnum(input.state, ["ready", "unavailable"] as const);
  if (
    (state === "unavailable") !== (protection === "unavailable") ||
    (protection === "memory" && durability !== "session") ||
    (protection === "os-protected" && durability !== "durable")
  )
    throw new NatsContractValidationError();
  return {
    durability,
    protection,
    state,
    ...(Object.hasOwn(input, "recovery") ? { recovery: natsText(input.recovery, 1024) } : {}),
  };
}
export function parseNatsProfileSummary(value: unknown): NatsProfileSummary {
  const input = natsObject(value);
  natsExactKeys(input, [
    "id",
    "revision",
    "name",
    "servers",
    "authentication",
    "tls",
    "createdAt",
    "updatedAt",
  ]);
  const auth = natsObject(input.authentication);
  const authMode = natsEnum(auth.mode, ["none", "token"] as const);
  natsExactKeys(auth, authMode === "none" ? ["mode"] : ["mode", "tokenPresent"]);
  const security = natsObject(input.tls);
  const tlsMode = natsEnum(security.mode, ["plaintext", "tls"] as const);
  natsExactKeys(security, tlsMode === "plaintext" ? ["mode"] : ["mode", "caPresent"]);
  return {
    id: natsIdentifier(input.id),
    revision: natsInteger(input.revision, 1),
    name: parseNatsName(input.name),
    servers: parseNatsServers(input.servers, tlsMode),
    authentication:
      authMode === "none"
        ? { mode: authMode }
        : { mode: authMode, tokenPresent: natsBoolean(auth.tokenPresent) },
    tls:
      tlsMode === "plaintext"
        ? { mode: tlsMode }
        : { mode: tlsMode, caPresent: natsBoolean(security.caPresent) },
    createdAt: natsTimestamp(input.createdAt),
    updatedAt: natsTimestamp(input.updatedAt),
  };
}
export function parseNatsProfilesSnapshot(value: unknown): NatsProfilesSnapshot {
  const input = natsObject(value);
  natsExactKeys(input, ["capability", "profiles"]);
  const profiles = natsArray(input.profiles, NATS_LIMITS.profiles).map(parseNatsProfileSummary);
  if (new Set(profiles.map((profile) => profile.id)).size !== profiles.length)
    throw new NatsContractValidationError();
  return { capability: parseNatsProfileStoreCapability(input.capability), profiles };
}
