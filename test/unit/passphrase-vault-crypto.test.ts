import { describe, expect, it } from "vitest";

import {
  decryptVaultValue,
  deriveVaultKey,
  encryptVaultValue,
  MAXIMUM_VAULT_VALUE_BYTES,
} from "../../src/platform/node/vault/vault-crypto";

describe("passphrase vault cryptography across host platforms", () => {
  it("roundtrips UTF-8 and empty values while authenticating both ciphertext and context", async () => {
    const key = await deriveVaultKey("portable crypto fixture phrase", Buffer.alloc(32, 7));
    const aad = Buffer.from("vault fixture context");
    try {
      for (const value of ["", "🔐 private credential fixture\u0000終"]) {
        const ciphertext = encryptVaultValue(key, aad, value);
        expect(decryptVaultValue(key, aad, ciphertext)).toBe(value);
        expect(() => decryptVaultValue(key, Buffer.from("other context"), ciphertext)).toThrow();
        const corrupt = Buffer.from(ciphertext);
        corrupt.writeUInt8(corrupt.readUInt8(16) ^ 1, 16);
        expect(() => decryptVaultValue(key, aad, corrupt)).toThrow();
      }
    } finally {
      key.fill(0);
    }
  });

  it("bounds value allocations before encrypting or accepting a ciphertext envelope", () => {
    const key = Buffer.alloc(32, 9);
    const aad = Buffer.from("fixture");
    expect(() => encryptVaultValue(key, aad, "x".repeat(MAXIMUM_VAULT_VALUE_BYTES + 1))).toThrow(
      "bound",
    );
    expect(() => decryptVaultValue(key, aad, Buffer.alloc(MAXIMUM_VAULT_VALUE_BYTES + 33))).toThrow(
      "unsupported",
    );
    expect(() => decryptVaultValue(key, aad, Buffer.from("SKV2"))).toThrow("unsupported");
  });
});
