import { join } from "node:path";

import { app, safeStorage } from "electron";

import { createElectronKafkaBackend } from "./electron-kafka-backend";
import { createElectronShell } from "./electron-shell";
import {
  installPackagedRendererProtocol,
  PACKAGED_RENDERER_URL,
  registerPackagedRendererScheme,
} from "./packaged-renderer-protocol";

let backend: Awaited<ReturnType<typeof createElectronKafkaBackend>> | undefined;
let shutdownPromise: Promise<void> | undefined;

registerPackagedRendererScheme();

async function shutdown(exitCode: number): Promise<void> {
  if (shutdownPromise !== undefined) {
    return shutdownPromise;
  }
  shutdownPromise = (async (): Promise<void> => {
    let cleanupFailure: unknown;
    try {
      await backend?.shutdown();
    } catch (error) {
      cleanupFailure = error;
    }
    app.exit(cleanupFailure === undefined ? exitCode : 1);
  })();
  return shutdownPromise;
}

async function start(): Promise<void> {
  await app.whenReady();
  const developmentRendererUrl = process.env.STREAMSKOPE_RENDERER_URL;
  if (developmentRendererUrl === undefined) {
    installPackagedRendererProtocol(join(__dirname, "..", "renderer"));
  }
  backend = await createElectronKafkaBackend({
    platform: process.platform,
    safeStorage,
    userDataPath: app.getPath("userData"),
  });
  await createElectronShell({
    backend,
    preloadPath: join(__dirname, "preload.cjs"),
    rendererUrl: developmentRendererUrl ?? PACKAGED_RENDERER_URL,
  });
}

app.on("window-all-closed", () => {
  void shutdown(0);
});

void start().catch((error: unknown) => {
  const summary = error instanceof Error ? error.message : "Unknown startup failure.";
  process.stderr.write(`StreamSkope startup failed: ${summary}\n`);
  void shutdown(1);
});
