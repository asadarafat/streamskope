import { readFile } from "node:fs/promises";
import { join } from "node:path";

import { app, safeStorage } from "electron";

import type { KafkaBackendFacade } from "../../src/features/kafka/facade";
import { createElectronKafkaBackend } from "../../src/platform/electron/main/electron-kafka-backend";
import { createElectronShell } from "../../src/platform/electron/main/electron-shell";
import {
  installPackagedRendererProtocol,
  PACKAGED_RENDERER_URL,
  registerPackagedRendererScheme,
} from "../../src/platform/electron/main/packaged-renderer-protocol";
import { parsePluginPackage } from "../../src/platform/node/plugins/package";
import type { OfficialPluginEntry } from "../../src/platform/node/plugins/catalog";
import { PluginRuntime } from "../../src/platform/node/plugins/runtime";
import { PluginStore } from "../../src/platform/node/plugins/store";

let backend: KafkaBackendFacade | undefined;
let closing = false;
registerPackagedRendererScheme();

async function shutdown(exitCode: number): Promise<void> {
  if (closing) return;
  closing = true;
  try {
    await backend?.shutdown();
    app.exit(exitCode);
  } catch {
    app.exit(1);
  }
}

async function start(): Promise<void> {
  const rendererRoot = process.env.STREAMSKOPE_PLUGIN_TEST_RENDERER;
  const userData = process.env.STREAMSKOPE_PLUGIN_TEST_USER_DATA;
  const catalogPackage = process.env.STREAMSKOPE_PLUGIN_TEST_PACKAGE;
  if (rendererRoot === undefined || userData === undefined || catalogPackage === undefined)
    throw new Error("Electron plugin test paths are required.");
  app.setPath("userData", userData);
  await app.whenReady();
  const plugins = new PluginRuntime({
    store: new PluginStore(join(userData, "plugins")),
    catalog: {
      list: async (): Promise<readonly OfficialPluginEntry[]> => {
        const { manifest, sha256 } = parsePluginPackage(await readFile(catalogPackage));
        return [{ manifest, sha256, downloadUrl: "https://api.github.com/test-release-asset" }];
      },
      download: async (): Promise<{ bytes: Uint8Array; sha256: string }> => {
        const bytes = await readFile(catalogPackage);
        return { bytes, sha256: parsePluginPackage(bytes).sha256 };
      },
    },
    restart: (): never => {
      throw new Error("This acceptance test forbids application restart.");
    },
  });
  installPackagedRendererProtocol(rendererRoot, (path) => plugins.rendererAsset(path));
  backend = await createElectronKafkaBackend({
    platform: process.platform,
    safeStorage,
    userDataPath: userData,
    plugins,
  });
  await createElectronShell({
    backend,
    preloadPath: join(__dirname, "preload.cjs"),
    rendererUrl: PACKAGED_RENDERER_URL,
  });
}

app.on("window-all-closed", () => {
  void shutdown(0);
});

void start().catch((error: unknown) => {
  process.stderr.write(`Electron plugin test failed: ${String(error)}\n`);
  void shutdown(1);
});
