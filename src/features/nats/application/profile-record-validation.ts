import {
  NATS_LIMITS,
  parseNatsCaPem,
  parseNatsName,
  parseNatsServers,
  parseNatsToken,
} from "../contracts";
import {
  NatsContractValidationError,
  natsArray,
  natsEnum,
  natsExactKeys,
  natsIdentifier,
  natsInteger,
  natsObject,
  natsTimestamp,
  natsUtf8Bytes,
} from "../contracts/validation-primitives";

import type { NatsConnectionInput, NatsProfileRecord } from "./profile-types";

export function parseNatsConnectionInput(value: unknown): NatsConnectionInput {
  const input = natsObject(value);
  natsExactKeys(input, ["servers", "authentication", "tls"]);
  const auth = natsObject(input.authentication);
  const authMode = natsEnum(auth.mode, ["none", "token"] as const);
  natsExactKeys(auth, authMode === "none" ? ["mode"] : ["mode", "token"]);
  const security = natsObject(input.tls);
  const tlsMode = natsEnum(security.mode, ["plaintext", "tls"] as const);
  natsExactKeys(security, ["mode"], tlsMode === "tls" ? ["caPem"] : []);
  return {
    servers: parseNatsServers(input.servers, tlsMode),
    authentication:
      authMode === "none"
        ? { mode: authMode }
        : { mode: authMode, token: parseNatsToken(auth.token) },
    tls:
      tlsMode === "plaintext"
        ? { mode: tlsMode }
        : {
            mode: tlsMode,
            ...(Object.hasOwn(security, "caPem") ? { caPem: parseNatsCaPem(security.caPem) } : {}),
          },
  };
}
export function parseNatsProfileRecord(value: unknown): NatsProfileRecord {
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
  const parsed = {
    ...parseNatsConnectionInput({
      servers: input.servers,
      authentication: input.authentication,
      tls: input.tls,
    }),
    id: natsIdentifier(input.id),
    revision: natsInteger(input.revision, 1),
    name: parseNatsName(input.name),
    createdAt: natsTimestamp(input.createdAt),
    updatedAt: natsTimestamp(input.updatedAt),
  };
  if (
    parsed.updatedAt < parsed.createdAt ||
    natsUtf8Bytes(JSON.stringify(parsed)) > NATS_LIMITS.profilePlaintextBytes
  )
    throw new NatsContractValidationError();
  return parsed;
}

export function parseNatsProfileRecords(value: unknown): readonly NatsProfileRecord[] {
  const records = natsArray(value, NATS_LIMITS.profiles).map(parseNatsProfileRecord);
  if (
    new Set(records.map((record) => record.id)).size !== records.length ||
    new Set(records.map((record) => record.name.toLowerCase())).size !== records.length
  )
    throw new NatsContractValidationError();
  return records;
}
