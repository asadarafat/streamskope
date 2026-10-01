import jks from "jks-js";
import forge from "node-forge";

import { PROFILE_LIMITS, type ProfileTrustKind } from "../contracts";
import type { KafkaProfileTrustDecoderInput, KafkaProfileTrustDecoderResult } from "../application";

import {
  canonicalCertificates,
  certificateEvidence,
  containsPrivateKey,
  KafkaTrustMaterialError,
  KafkaTruststorePasswordError,
  parsePemTrustMaterial,
} from "./trust-material-shared";

const JKS_MAGIC = 0xfeedfeed;

function pkcs12Certificates(value: Buffer, password: string): readonly string[] {
  const parsed = forge.pkcs12.pkcs12FromAsn1(
    forge.asn1.fromDer(value.toString("binary")),
    password,
  );
  const certificates: string[] = [];
  for (const contents of parsed.safeContents) {
    for (const bag of contents.safeBags) {
      if (bag.type !== forge.pki.oids.certBag) throw new KafkaTrustMaterialError();
      if (bag.cert !== undefined && bag.cert !== null)
        certificates.push(forge.pki.certificateToPem(bag.cert));
      else if (bag.asn1 !== undefined)
        certificates.push(
          forge.pem.encode({ type: "CERTIFICATE", body: forge.asn1.toDer(bag.asn1).getBytes() }),
        );
      else throw new KafkaTrustMaterialError();
    }
  }
  return certificates;
}

function canonicalBase64Text(value: string): boolean {
  if (value.length === 0 || value.length % 4 !== 0) {
    return false;
  }
  const padding = value.endsWith("==") ? 2 : value.endsWith("=") ? 1 : 0;
  const contentLength = value.length - padding;
  for (let index = 0; index < contentLength; index += 1) {
    const code = value.charCodeAt(index);
    const valid =
      (code >= 0x41 && code <= 0x5a) ||
      (code >= 0x61 && code <= 0x7a) ||
      (code >= 0x30 && code <= 0x39) ||
      code === 0x2b ||
      code === 0x2f;
    if (!valid) {
      return false;
    }
  }
  for (let index = contentLength; index < value.length; index += 1) {
    if (value.charCodeAt(index) !== 0x3d) {
      return false;
    }
  }
  return true;
}

function strictBase64(value: string): Buffer {
  if (
    value.length === 0 ||
    value.length > PROFILE_LIMITS.trustEncodedCharacters ||
    !canonicalBase64Text(value)
  ) {
    throw new KafkaTrustMaterialError();
  }
  const result = Buffer.from(value, "base64");
  if (
    result.length === 0 ||
    result.length > PROFILE_LIMITS.trustBinaryBytes ||
    result.toString("base64") !== value
  ) {
    throw new KafkaTrustMaterialError();
  }
  return result;
}

function binaryKind(value: Buffer): Exclude<ProfileTrustKind, "pem"> {
  return value.length >= 4 && value.readUInt32BE(0) === JKS_MAGIC ? "jks" : "pkcs12";
}

function binaryCertificates(
  value: Buffer,
  password: string | undefined,
): {
  readonly certificates: readonly string[];
  readonly kind: Exclude<ProfileTrustKind, "pem">;
} {
  if (password === undefined || password.length === 0) {
    throw new KafkaTruststorePasswordError();
  }
  const kind = binaryKind(value);
  let entries: ReturnType<typeof jks.toPem>;
  try {
    if (kind === "pkcs12") return { certificates: pkcs12Certificates(value, password), kind };
    entries = jks.toPem(value, password);
  } catch (error) {
    const message = error instanceof Error ? error.message : "";
    if (/PASSWORD|MAC COULD NOT BE VERIFIED/iu.test(message)) {
      throw new KafkaTruststorePasswordError();
    }
    throw new KafkaTrustMaterialError();
  }
  const certificates: string[] = [];
  for (const entry of Object.values(entries)) {
    if (entry.key !== undefined || (entry.cert !== undefined && containsPrivateKey(entry.cert))) {
      throw new KafkaTrustMaterialError();
    }
    if (entry.ca !== undefined) {
      certificates.push(entry.ca);
    }
    if (entry.cert !== undefined) {
      certificates.push(entry.cert);
    }
  }
  return { certificates, kind };
}

export function parseTrustMaterial(
  input: KafkaProfileTrustDecoderInput,
): KafkaProfileTrustDecoderResult {
  if (input.kind === "pem") {
    return parsePemTrustMaterial(input);
  }
  const decoded = binaryCertificates(strictBase64(input.material), input.password);
  const caPem = canonicalCertificates(decoded.certificates);
  return {
    caPem,
    evidence: certificateEvidence(caPem),
    kind: decoded.kind,
  };
}
