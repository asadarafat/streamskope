import { appendFile } from "node:fs/promises";
import { isAbsolute, join } from "node:path";

import { app, shell } from "electron";

import type { KafkaBackendFacade } from "../../src/features/kafka/facade";
import { createElectronKafkaBackend } from "../../src/platform/electron/main/electron-kafka-backend";
import type { ElectronSafeStoragePort } from "../../src/platform/electron/main/electron-profile-protection";
import { createElectronShell } from "../../src/platform/electron/main/electron-shell";
import {
  installPackagedRendererProtocol,
  PACKAGED_RENDERER_URL,
  registerPackagedRendererScheme,
} from "../../src/platform/electron/main/packaged-renderer-protocol";

class DeterministicSafeStorage implements ElectronSafeStoragePort {
  constructor(private readonly available: boolean) {}

  decryptStringAsync(
    encrypted: Buffer,
  ): Promise<{ readonly result: string; readonly shouldReEncrypt: boolean }> {
    return Promise.resolve({
      result: Buffer.from(encrypted).reverse().toString("utf8"),
      shouldReEncrypt: false,
    });
  }

  encryptStringAsync(plainText: string): Promise<Buffer> {
    return Promise.resolve(Buffer.from(plainText, "utf8").reverse());
  }

  getSelectedStorageBackend(): "gnome_libsecret" {
    return "gnome_libsecret";
  }

  isAsyncEncryptionAvailable(): Promise<boolean> {
    return Promise.resolve(this.available);
  }
}

let backend: KafkaBackendFacade | undefined;
let shuttingDown = false;

registerPackagedRendererScheme();

async function shutdown(exitCode: number): Promise<void> {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  try {
    await backend?.shutdown();
    app.exit(exitCode);
  } catch {
    app.exit(1);
  }
}

async function start(): Promise<void> {
  const rendererUrl = process.env.STREAMSKOPE_PROFILE_RENDERER_URL;
  const rendererRoot = process.env.STREAMSKOPE_PROFILE_RENDERER_ROOT;
  const userDataPath = process.env.STREAMSKOPE_PROFILE_USER_DATA;
  if (
    rendererUrl !== PACKAGED_RENDERER_URL ||
    rendererRoot === undefined ||
    userDataPath === undefined
  ) {
    throw new Error("Profile Electron test paths are required.");
  }
  const externalUrlLogPath = process.env.STREAMSKOPE_PROFILE_EXTERNAL_URL_LOG;
  if (externalUrlLogPath !== undefined) {
    if (!isAbsolute(externalUrlLogPath)) {
      throw new Error("Profile Electron external-URL log path must be absolute.");
    }
    shell.openExternal = async (url): Promise<void> => {
      await appendFile(externalUrlLogPath, `${url}\n`, { encoding: "utf8", mode: 0o600 });
    };
  }
  await app.whenReady();
  installPackagedRendererProtocol(rendererRoot);
  backend = await createElectronKafkaBackend({
    platform: "linux",
    safeStorage: new DeterministicSafeStorage(
      process.env.STREAMSKOPE_PROFILE_STORAGE !== "unavailable",
    ),
    userDataPath,
  });
  await createElectronShell({
    backend,
    preloadPath: join(__dirname, "preload.cjs"),
    rendererUrl,
  });
}

app.on("window-all-closed", () => {
  void shutdown(0);
});

void start().catch(() => {
  void shutdown(1);
});
