import { X509Certificate } from "node:crypto";

import { SECURE_CONNECTION_LIMITS } from "../contracts";
import {
  TRUST_CERTIFICATE_EVIDENCE_LIMITS,
  type TrustCertificateEvidence,
} from "../contracts/remote-trust-types";
import type { KafkaProfileTrustDecoderInput, KafkaProfileTrustDecoderResult } from "../application";

const CERTIFICATE_PATTERN = /-----BEGIN CERTIFICATE-----[\s\S]*?-----END CERTIFICATE-----/gu;
const PRIVATE_KEY_PATTERN = /-----BEGIN (?:ENCRYPTED |RSA |EC )?PRIVATE KEY-----/u;

export class KafkaTrustMaterialError extends Error {
  readonly code = "TRUST_MATERIAL" as const;
  readonly recovery =
    "Select a certificate-only PEM CA or a valid password-protected JKS/PKCS12 truststore.";
  readonly retryable = false;
  readonly stage = "trust" as const;

  constructor() {
    super("Kafka trust material is malformed, empty, unsupported, or contains a private key.");
    this.name = "KafkaTrustMaterialError";
  }
}

export class KafkaTruststorePasswordError extends Error {
  readonly code = "TRUSTSTORE_PASSWORD" as const;
  readonly recovery = "Verify the JKS or PKCS12 truststore password and try again.";
  readonly retryable = false;
  readonly stage = "trust" as const;

  constructor() {
    super("The Kafka truststore could not be opened with the supplied password.");
    this.name = "KafkaTruststorePasswordError";
  }
}

export function containsPrivateKey(value: string): boolean {
  return PRIVATE_KEY_PATTERN.test(value);
}

export function canonicalCertificates(values: readonly string[]): string {
  const certificates = new Map<string, string>();
  for (const value of values) {
    if (containsPrivateKey(value)) {
      throw new KafkaTrustMaterialError();
    }
    const matches = value.match(CERTIFICATE_PATTERN) ?? [];
    const remainder = value.replace(CERTIFICATE_PATTERN, "").trim();
    if (matches.length === 0 || remainder.length > 0) {
      throw new KafkaTrustMaterialError();
    }
    for (const match of matches) {
      let certificate: X509Certificate;
      try {
        certificate = new X509Certificate(match);
      } catch {
        throw new KafkaTrustMaterialError();
      }
      certificates.set(certificate.raw.toString("base64"), certificate.toString());
    }
  }
  if (certificates.size === 0) {
    throw new KafkaTrustMaterialError();
  }
  const caPem = `${[...certificates.values()].join("\n")}\n`;
  if (caPem.length > SECURE_CONNECTION_LIMITS.caPemCharacters) {
    throw new KafkaTrustMaterialError();
  }
  return caPem;
}

export function parsePemTrustMaterial(
  input: KafkaProfileTrustDecoderInput,
): KafkaProfileTrustDecoderResult {
  if (
    input.kind !== "pem" ||
    Buffer.byteLength(input.material, "utf8") > SECURE_CONNECTION_LIMITS.caPemCharacters
  ) {
    throw new KafkaTrustMaterialError();
  }
  const caPem = canonicalCertificates([input.material]);
  return {
    caPem,
    evidence: certificateEvidence(caPem),
    kind: "pem",
  };
}

export function certificateEvidence(caPem: string): TrustCertificateEvidence {
  const certificates = caPem.match(CERTIFICATE_PATTERN) ?? [];
  const limits = TRUST_CERTIFICATE_EVIDENCE_LIMITS;
  let earliestExpiry = Number.POSITIVE_INFINITY;
  let latestStart = Number.NEGATIVE_INFINITY;
  const entries: TrustCertificateEvidence["certificates"][number][] = [];
  for (const pem of certificates) {
    const certificate = new X509Certificate(pem);
    earliestExpiry = Math.min(earliestExpiry, certificate.validToDate.getTime());
    latestStart = Math.max(latestStart, certificate.validFromDate.getTime());
    if (entries.length < limits.entries)
      entries.push({
        subject: certificate.subject.slice(0, limits.nameCharacters),
        issuer: certificate.issuer.slice(0, limits.nameCharacters),
        validFrom: certificate.validFromDate.toISOString(),
        validTo: certificate.validToDate.toISOString(),
        fingerprint: certificate.fingerprint256,
        truncated:
          certificate.subject.length > limits.nameCharacters ||
          certificate.issuer.length > limits.nameCharacters,
      });
  }
  if (!Number.isFinite(earliestExpiry) || !Number.isFinite(latestStart))
    throw new KafkaTrustMaterialError();
  return {
    count: certificates.length,
    truncated: certificates.length > limits.entries,
    validity: {
      earliestExpiry: new Date(earliestExpiry).toISOString(),
      latestStart: new Date(latestStart).toISOString(),
    },
    certificates: entries,
  };
}
