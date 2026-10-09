import { generateKeyPairSync } from "node:crypto";

import forge from "node-forge";

export function encryptedClientKey(passphrase: string): string {
  return generateKeyPairSync("ec", {
    namedCurve: "prime256v1",
    privateKeyEncoding: { type: "pkcs8", format: "pem", cipher: "aes-256-cbc", passphrase },
    publicKeyEncoding: { type: "spki", format: "pem" },
  }).privateKey;
}

/** Valid encryption/padding around invalid PrivateKeyInfo, as a wrong password can produce. */
export function undecodableClientKey(passphrase: string): string {
  const pki = forge.pki as typeof forge.pki & {
    encryptPrivateKeyInfo(
      value: object,
      password: string,
      options: { algorithm: string; count: number },
    ): object;
    encryptedPrivateKeyToPem(value: object): string;
  };
  const emptySequence = forge.asn1.fromDer("\x30\x00");
  return pki.encryptedPrivateKeyToPem(
    pki.encryptPrivateKeyInfo(emptySequence, passphrase, { algorithm: "aes256", count: 1 }),
  );
}
