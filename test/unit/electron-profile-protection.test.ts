import { afterEach, describe, expect, it, vi } from "vitest";

import {
  ElectronProfileProtectionError,
  initializeElectronProfileProtection,
  type ElectronSafeStoragePort,
} from "../../src/main/electron-profile-protection";

class FakeSafeStorage implements ElectronSafeStoragePort {
  asyncAvailable = true;
  backend: ReturnType<ElectronSafeStoragePort["getSelectedStorageBackend"]> = "gnome_libsecret";
  decryptResult = {
    result: "decrypted-secret",
    shouldReEncrypt: false,
  };
  encryptedValues: string[] = [];

  decryptStringAsync(): Promise<{
    readonly result: string;
    readonly shouldReEncrypt: boolean;
  }> {
    return Promise.resolve(this.decryptResult);
  }

  encryptStringAsync(value: string): Promise<Buffer> {
    this.encryptedValues.push(value);
    return Promise.resolve(Buffer.from("protected-value"));
  }

  getSelectedStorageBackend():
    "basic_text" | "gnome_libsecret" | "kwallet" | "kwallet5" | "kwallet6" | "unknown" {
    return this.backend;
  }

  isAsyncEncryptionAvailable(): Promise<boolean> {
    return Promise.resolve(this.asyncAvailable);
  }
}

describe("Electron Kafka profile protection", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("uses only the asynchronous OS protection API after availability is confirmed", async () => {
    const safeStorage = new FakeSafeStorage();

    const result = await initializeElectronProfileProtection(safeStorage, "linux");

    expect(result.capability).toEqual({
      durability: "durable",
      protection: "os-protected",
      state: "ready",
    });
    expect(result.protector).toBeDefined();
    await expect(result.protector!.protect("fixture-secret")).resolves.toEqual(
      Buffer.from("protected-value"),
    );
    await expect(result.protector!.unprotect(Buffer.from("ciphertext"))).resolves.toEqual({
      plaintext: "decrypted-secret",
      shouldReEncrypt: false,
    });
    expect(safeStorage.encryptedValues).toEqual(["fixture-secret"]);
  });

  it("fails closed when asynchronous OS protection is unavailable", async () => {
    const safeStorage = new FakeSafeStorage();
    safeStorage.asyncAvailable = false;

    const result = await initializeElectronProfileProtection(safeStorage, "linux");

    expect(result.protector).toBeUndefined();
    expect(result.capability).toMatchObject({
      durability: "durable",
      protection: "unavailable",
      state: "unavailable",
    });
    expect(result.capability.recovery).toContain("credential");
  });

  it("fails closed when OS protection availability stalls past the startup boundary", async () => {
    vi.useFakeTimers();
    const safeStorage = new FakeSafeStorage();
    let resolveAvailability: ((available: boolean) => void) | undefined;
    safeStorage.isAsyncEncryptionAvailable = (): Promise<boolean> =>
      new Promise<boolean>((resolve) => {
        resolveAvailability = resolve;
      });

    const pending = initializeElectronProfileProtection(safeStorage, "linux");
    const settled = vi.fn();
    void pending.then(settled);

    await vi.advanceTimersByTimeAsync(1_000);

    expect(settled).toHaveBeenCalledTimes(1);
    const result = await pending;
    expect(result.capability).toMatchObject({
      durability: "durable",
      protection: "unavailable",
      state: "unavailable",
    });

    resolveAvailability?.(true);
    await Promise.resolve();
    expect(result.protector).toBeUndefined();
  });

  it("rejects Linux basic_text even when Electron reports encryption available", async () => {
    const safeStorage = new FakeSafeStorage();
    safeStorage.backend = "basic_text";

    const result = await initializeElectronProfileProtection(safeStorage, "linux");

    expect(result.protector).toBeUndefined();
    expect(result.capability).toMatchObject({
      durability: "durable",
      protection: "unavailable",
      state: "unavailable",
    });
  });

  it("does not echo protected input when encryption or decryption rejects", async () => {
    const safeStorage = new FakeSafeStorage();
    safeStorage.encryptStringAsync = (): Promise<Buffer> =>
      Promise.reject(new Error("failed for fixture-secret"));
    safeStorage.decryptStringAsync = (): Promise<{
      readonly result: string;
      readonly shouldReEncrypt: boolean;
    }> => Promise.reject(new Error("failed for secret-ciphertext"));
    const result = await initializeElectronProfileProtection(safeStorage, "darwin");

    await expect(result.protector!.protect("fixture-secret")).rejects.toBeInstanceOf(
      ElectronProfileProtectionError,
    );
    await expect(result.protector!.protect("fixture-secret")).rejects.not.toThrow(/fixture-secret/);
    await expect(result.protector!.unprotect(Buffer.from("secret-ciphertext"))).rejects.not.toThrow(
      /secret-ciphertext/,
    );
  });
});
