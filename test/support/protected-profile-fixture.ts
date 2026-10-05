import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { X509Certificate } from "node:crypto";

import jks from "jks-js";

import type { ElectronSafeStoragePort } from "../../src/platform/electron/main/electron-profile-protection";
import type { ProfileProtector } from "../../src/platform/node/profile-protector";

type UnprotectedValue = { readonly plaintext: string; readonly shouldReEncrypt: boolean };

/** Reversible test data proves storage boundaries, never real OS encryption. */
export class ReversibleProfileProtector implements ProfileProtector {
  readonly protectedInputs: string[] = [];
  readonly unprotectedInputs: Buffer[] = [];
  rotate = false;
  protectOperation: ((plaintext: string) => Promise<Buffer>) | undefined;
  unprotectOperation: ((protectedValue: Buffer) => Promise<UnprotectedValue>) | undefined;
  private nonce = 0;

  protect(plaintext: string): Promise<Buffer> {
    this.protectedInputs.push(plaintext);
    return (
      this.protectOperation?.(plaintext) ??
      Promise.resolve(Buffer.from(`${++this.nonce}|${plaintext}`, "utf8").reverse())
    );
  }

  unprotect(protectedValue: Buffer): Promise<UnprotectedValue> {
    this.unprotectedInputs.push(protectedValue);
    if (this.unprotectOperation !== undefined) return this.unprotectOperation(protectedValue);
    const restored = Buffer.from(protectedValue).reverse().toString("utf8");
    const separator = restored.indexOf("|");
    if (separator < 1) return Promise.reject(new Error("Invalid reversible fixture"));
    return Promise.resolve({
      plaintext: restored.slice(separator + 1),
      shouldReEncrypt: this.rotate,
    });
  }
}

export class ReversibleSafeStorage
  extends ReversibleProfileProtector
  implements ElectronSafeStoragePort
{
  available = true;
  availabilityCalls = 0;
  backendCalls = 0;
  backend: ReturnType<ElectronSafeStoragePort["getSelectedStorageBackend"]> = "gnome_libsecret";

  async decryptStringAsync(
    encrypted: Buffer,
  ): Promise<{ readonly result: string; readonly shouldReEncrypt: boolean }> {
    const restored = await this.unprotect(encrypted);
    return { result: restored.plaintext, shouldReEncrypt: restored.shouldReEncrypt };
  }

  encryptStringAsync(plaintext: string): Promise<Buffer> {
    return this.protect(plaintext);
  }

  getSelectedStorageBackend(): ReturnType<ElectronSafeStoragePort["getSelectedStorageBackend"]> {
    this.backendCalls += 1;
    return this.backend;
  }

  isAsyncEncryptionAvailable(): Promise<boolean> {
    this.availabilityCalls += 1;
    return Promise.resolve(this.available);
  }
}

export async function protectedProfileFixtureCa(): Promise<string> {
  const material = await readFile(
    join(process.cwd(), "node_modules/jks-js/examples/assets/truststore.jks"),
  );
  const certificate = Object.values(jks.toPem(material, "password"))
    .flatMap((entry) => [entry.ca, entry.cert])
    .find((entry) => entry !== undefined);
  if (certificate === undefined) throw new Error("Expected the pinned certificate fixture.");
  return `${new X509Certificate(certificate).toString().trim()}\n`;
}
