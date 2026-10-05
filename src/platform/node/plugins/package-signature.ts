import { createPrivateKey, createPublicKey, sign, verify, type KeyObject } from "node:crypto";

import { TRUSTED_PLUGIN_PUBLISHERS, type TrustedPluginPublisher } from "./publishers";

const DOMAIN = Buffer.from("StreamSkope portable plugin signature\u0000v2\u0000", "utf8");
const KEY_ID = /^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$/u;

export interface VerifiedPortableEnvelope {
  readonly payload: Uint8Array;
  readonly publisher: TrustedPluginPublisher;
}

export interface PortablePluginEnvelope {
  readonly formatVersion: 2;
  readonly keyId: string;
  readonly signature: string;
  readonly payload: string;
}

function keyId(value: unknown): string {
  if (typeof value !== "string" || value.length > 80 || !KEY_ID.test(value))
    throw new Error("Invalid plugin publisher key identifier.");
  return value;
}

function canonicalBase64(value: unknown, maximum: number): Buffer {
  if (typeof value !== "string" || value.length === 0 || value.length > 4 * Math.ceil(maximum / 3))
    throw new Error("Portable plugin data exceeds its size limit or is empty.");
  const bytes = Buffer.from(value, "base64");
  if (bytes.byteLength === 0 || bytes.byteLength > maximum || bytes.toString("base64") !== value)
    throw new Error("Portable plugin data must use canonical bounded base64.");
  return bytes;
}

function message(id: string, payload: Uint8Array): Uint8Array {
  const identifier = Buffer.from(id, "utf8");
  const identifierLength = Buffer.alloc(4);
  const payloadLength = Buffer.alloc(4);
  identifierLength.writeUInt32BE(identifier.byteLength);
  payloadLength.writeUInt32BE(payload.byteLength);
  return Buffer.concat([DOMAIN, identifierLength, identifier, payloadLength, payload]);
}

export function signPortableEnvelope(
  payload: Uint8Array,
  publisherKeyId: string,
  privateKey: string | KeyObject,
): PortablePluginEnvelope {
  const id = keyId(publisherKeyId);
  const key = typeof privateKey === "string" ? createPrivateKey(privateKey) : privateKey;
  if (key.type !== "private" || key.asymmetricKeyType !== "ed25519")
    throw new Error("Portable plugins require an Ed25519 private signing key.");
  return {
    formatVersion: 2,
    keyId: id,
    signature: sign(null, message(id, payload), key).toString("base64"),
    payload: Buffer.from(payload).toString("base64"),
  };
}

export function verifyPortableEnvelope(
  envelope: Record<string, unknown>,
  maximumPayload: number,
  trustedPublishers: readonly TrustedPluginPublisher[] = TRUSTED_PLUGIN_PUBLISHERS,
): VerifiedPortableEnvelope {
  if (
    Object.keys(envelope).sort().join("\n") !==
      ["formatVersion", "keyId", "payload", "signature"].join("\n") ||
    envelope.formatVersion !== 2
  )
    throw new Error("Unexpected portable plugin package fields or format.");
  const id = keyId(envelope.keyId);
  const publisher = trustedPublishers.find((entry) => entry.keyId === id);
  if (publisher === undefined)
    throw new Error("The plugin publisher is not trusted by this desktop.");
  const payload = canonicalBase64(envelope.payload, maximumPayload);
  const signature = canonicalBase64(envelope.signature, 64);
  if (signature.byteLength !== 64) throw new Error("Invalid Ed25519 plugin signature length.");
  if (!publisher.publicKey.startsWith("-----BEGIN PUBLIC KEY-----"))
    throw new Error("Trusted plugin publishers must use public verification keys.");
  const publicKey = createPublicKey(publisher.publicKey);
  if (publicKey.asymmetricKeyType !== "ed25519")
    throw new Error("Trusted plugin publishers must use Ed25519 verification keys.");
  if (!verify(null, message(id, payload), publicKey, signature))
    throw new Error("Plugin publisher signature could not be verified.");
  return { payload, publisher };
}
