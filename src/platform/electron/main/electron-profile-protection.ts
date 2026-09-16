import type { ProfileStoreCapability } from "../../../features/kafka/contracts";

import type { KafkaProfileProtector } from "./kafka-profile-file-store";

export type ElectronStorageBackend =
  "basic_text" | "gnome_libsecret" | "kwallet" | "kwallet5" | "kwallet6" | "unknown";

const PROFILE_PROTECTION_AVAILABILITY_TIMEOUT_MS = 1_000;

export interface ElectronSafeStoragePort {
  decryptStringAsync(encrypted: Buffer): Promise<{
    readonly result: string;
    readonly shouldReEncrypt: boolean;
  }>;
  encryptStringAsync(plainText: string): Promise<Buffer>;
  getSelectedStorageBackend(): ElectronStorageBackend;
  isAsyncEncryptionAvailable(): Promise<boolean>;
}

export interface ElectronProfileProtection {
  readonly capability: ProfileStoreCapability;
  readonly protector?: KafkaProfileProtector;
}

export class ElectronProfileProtectionError extends Error {
  constructor(operation: "decrypt" | "encrypt") {
    super(`Operating-system profile ${operation}ion failed.`);
    this.name = "ElectronProfileProtectionError";
  }
}

class SafeStorageProfileProtector implements KafkaProfileProtector {
  constructor(private readonly safeStorage: ElectronSafeStoragePort) {}

  async protect(plaintext: string): Promise<Buffer> {
    try {
      return await this.safeStorage.encryptStringAsync(plaintext);
    } catch {
      throw new ElectronProfileProtectionError("encrypt");
    }
  }

  async unprotect(
    protectedValue: Buffer,
  ): Promise<{ readonly plaintext: string; readonly shouldReEncrypt: boolean }> {
    try {
      const value = await this.safeStorage.decryptStringAsync(protectedValue);
      return {
        plaintext: value.result,
        shouldReEncrypt: value.shouldReEncrypt,
      };
    } catch {
      throw new ElectronProfileProtectionError("decrypt");
    }
  }
}

const unavailableCapability: ProfileStoreCapability = {
  durability: "durable",
  protection: "unavailable",
  recovery:
    "Unlock or configure the operating-system credential service, then restart StreamSkope.",
  state: "unavailable",
};

async function boundedEncryptionAvailability(
  safeStorage: ElectronSafeStoragePort,
): Promise<boolean> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      safeStorage.isAsyncEncryptionAvailable(),
      new Promise<boolean>((resolve) => {
        timeout = setTimeout(() => {
          resolve(false);
        }, PROFILE_PROTECTION_AVAILABILITY_TIMEOUT_MS);
      }),
    ]);
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout);
    }
  }
}

export async function initializeElectronProfileProtection(
  safeStorage: ElectronSafeStoragePort,
  platform: NodeJS.Platform,
): Promise<ElectronProfileProtection> {
  let available: boolean;
  try {
    available = await boundedEncryptionAvailability(safeStorage);
  } catch {
    return { capability: unavailableCapability };
  }
  if (!available) {
    return { capability: unavailableCapability };
  }
  if (platform === "linux") {
    const backend = safeStorage.getSelectedStorageBackend();
    if (backend === "basic_text" || backend === "unknown") {
      return { capability: unavailableCapability };
    }
  }
  return {
    capability: {
      durability: "durable",
      protection: "os-protected",
      state: "ready",
    },
    protector: new SafeStorageProfileProtector(safeStorage),
  };
}
