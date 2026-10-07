import { createCipheriv, createDecipheriv, randomBytes, scrypt } from "node:crypto";

const PREFIX = Buffer.from("SKV1", "ascii");
const NONCE_BYTES = 12;
const TAG_BYTES = 16;
const HEADER_BYTES = PREFIX.length + NONCE_BYTES + TAG_BYTES;
export const MAXIMUM_VAULT_VALUE_BYTES = 32 * 1_048_576;
export const VAULT_KDF = Object.freeze({ name: "scrypt", N: 32768, r: 8, p: 1 });

export async function deriveVaultKey(passphrase: string, salt: Buffer): Promise<Buffer> {
  const bytes = Buffer.from(passphrase, "utf8");
  try {
    return await new Promise<Buffer>((resolve, reject) => {
      scrypt(bytes, salt, 32, { ...VAULT_KDF, maxmem: 64 * 1_048_576 }, (error, key) => {
        if (error !== null) reject(error);
        else resolve(key);
      });
    });
  } finally {
    bytes.fill(0);
  }
}

export function encryptVaultValue(key: Buffer, aad: Buffer, plaintext: string): Buffer {
  if (Buffer.byteLength(plaintext, "utf8") > MAXIMUM_VAULT_VALUE_BYTES)
    throw new Error("Vault value exceeds its storage bound.");
  const nonce = randomBytes(NONCE_BYTES);
  const cipher = createCipheriv("aes-256-gcm", key, nonce);
  cipher.setAAD(aad);
  const ciphertext = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
  return Buffer.concat([PREFIX, nonce, cipher.getAuthTag(), ciphertext]);
}

export function decryptVaultValue(key: Buffer, aad: Buffer, protectedValue: Buffer): string {
  if (
    protectedValue.length < HEADER_BYTES ||
    protectedValue.length > HEADER_BYTES + MAXIMUM_VAULT_VALUE_BYTES ||
    !protectedValue.subarray(0, PREFIX.length).equals(PREFIX)
  )
    throw new Error("Vault value is corrupt or unsupported.");
  const decipher = createDecipheriv(
    "aes-256-gcm",
    key,
    protectedValue.subarray(PREFIX.length, PREFIX.length + NONCE_BYTES),
  );
  decipher.setAAD(aad);
  decipher.setAuthTag(protectedValue.subarray(PREFIX.length + NONCE_BYTES, HEADER_BYTES));
  const partial = decipher.update(protectedValue.subarray(HEADER_BYTES));
  let final: Buffer | undefined;
  let plaintext: Buffer | undefined;
  try {
    final = decipher.final();
    plaintext = Buffer.concat([partial, final]);
    return new TextDecoder("utf-8", { fatal: true }).decode(plaintext);
  } finally {
    partial.fill(0);
    final?.fill(0);
    plaintext?.fill(0);
  }
}
