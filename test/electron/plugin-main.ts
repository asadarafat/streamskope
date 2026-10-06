import { readFile } from "node:fs/promises";
import { join } from "node:path";

import { app, safeStorage } from "electron";

import type { KafkaBackendFacade } from "../../src/features/kafka/facade";
import { createElectronKafkaBackend } from "../../src/platform/electron/main/electron-kafka-backend";
import { createElectronNatsProfileStore } from "../../src/platform/electron/main/electron-nats-profile-store";
import { initializeElectronProfileProtection } from "../../src/platform/electron/main/electron-profile-protection";
import {
  createElectronShell,
  type RunningElectronShell,
} from "../../src/platform/electron/main/electron-shell";
import {
  installPackagedRendererProtocol,
  PACKAGED_RENDERER_URL,
  registerPackagedRendererScheme,
} from "../../src/platform/electron/main/packaged-renderer-protocol";
import { createKafkaElectronDeliveryBinding } from "../../src/platform/electron/main/kafka-provider-delivery";
import { createNatsElectronDeliveryBinding } from "../../src/platform/electron/main/nats-provider-delivery";
import { createKafkaProviderEndpoint } from "../../src/platform/node/kafka-provider";
import { createNatsBackend } from "../../src/platform/node/nats-backend";
import { createNatsProviderEndpoint } from "../../src/platform/node/nats-provider";
import { ProviderHostRegistry } from "../../src/platform/node/provider-host";
import { parsePluginPackage } from "../../src/platform/node/plugins/package";
import type { OfficialPluginEntry } from "../../src/platform/node/plugins/catalog";
import { PluginRuntime } from "../../src/platform/node/plugins/runtime";
import { PluginStore } from "../../src/platform/node/plugins/store";
import type { TrustedPluginPublisher } from "../../src/platform/node/plugins/publishers";
import { createPluginPackageFilePicker } from "../../src/platform/electron/main/plugin-file-picker";

let backend: KafkaBackendFacade | undefined;
let natsBackend: ReturnType<typeof createNatsBackend> | undefined;
let providers: ProviderHostRegistry | undefined;
let runningShell: RunningElectronShell | undefined;
let closing = false;
registerPackagedRendererScheme();
// Capture startup failures before Playwright can attach to the first window.
app.on("web-contents-created", (_event, contents) => {
  contents.on("console-message", (details) => {
    if (details.level === "error") process.stderr.write(`${details.message}\n`);
  });
});

async function shutdown(exitCode: number): Promise<void> {
  if (closing) return;
  closing = true;
  const ownedProviders = providers === undefined ? [backend, natsBackend] : [providers];
  const results = await Promise.allSettled(
    [
      (): Promise<void> => runningShell?.close() ?? Promise.resolve(),
      ...ownedProviders.map(
        (owner): (() => Promise<void>) =>
          (): Promise<void> =>
            owner?.shutdown() ?? Promise.resolve(),
      ),
    ].map((run): Promise<void> => {
      try {
        return run();
      } catch (cause) {
        return Promise.reject(new Error("Electron plugin fixture cleanup failed.", { cause }));
      }
    }),
  );
  app.exit(results.some((result) => result.status === "rejected") ? 1 : exitCode);
}

async function start(): Promise<void> {
  const rendererRoot = process.env.STREAMSKOPE_PLUGIN_TEST_RENDERER;
  const userData = process.env.STREAMSKOPE_PLUGIN_TEST_USER_DATA;
  const catalogPackage = process.env.STREAMSKOPE_PLUGIN_TEST_PACKAGE;
  if (rendererRoot === undefined || userData === undefined || catalogPackage === undefined)
    throw new Error("Electron plugin test paths are required.");
  app.setPath("userData", userData);
  await app.whenReady();
  const publisherFixture = process.env.STREAMSKOPE_PLUGIN_TEST_PUBLISHERS;
  const portableFile = process.env.STREAMSKOPE_PLUGIN_TEST_FILE;
  const offline = process.env.STREAMSKOPE_PLUGIN_TEST_OFFLINE === "1";
  const hostRelease = process.env.STREAMSKOPE_PLUGIN_TEST_HOST_RELEASE;
  const plugins = new PluginRuntime({
    ...(hostRelease === undefined ? {} : { hostRelease }),
    store: new PluginStore(join(userData, "plugins"), {
      ...(publisherFixture === undefined
        ? {}
        : {
            trustedPublishers: JSON.parse(publisherFixture) as TrustedPluginPublisher[],
          }),
    }),
    ...(portableFile === undefined
      ? {}
      : {
          choosePackageFile: createPluginPackageFilePicker(() =>
            Promise.resolve({
              canceled: false,
              filePaths: [portableFile],
            }),
          ),
        }),
    catalog: {
      list: async (): Promise<readonly OfficialPluginEntry[]> => {
        if (offline) throw new Error("GitHub is unavailable in this isolated fixture.");
        const { manifest, sha256 } = parsePluginPackage(await readFile(catalogPackage));
        return [
          {
            manifest,
            sha256,
            downloadUrl: "https://api.github.com/repos/asadarafat/streamskope/releases/assets/1",
          },
        ];
      },
      download: async (): Promise<{ bytes: Uint8Array; sha256: string }> => {
        if (offline) throw new Error("GitHub is unavailable in this isolated fixture.");
        const bytes = await readFile(catalogPackage);
        return { bytes, sha256: parsePluginPackage(bytes).sha256 };
      },
    },
    restart: (): never => {
      throw new Error("This acceptance test forbids application restart.");
    },
  });
  installPackagedRendererProtocol(rendererRoot, (path) => plugins.rendererAsset(path));
  const profileProtection = await initializeElectronProfileProtection(
    safeStorage,
    process.platform,
  );
  backend = await createElectronKafkaBackend({
    platform: process.platform,
    safeStorage,
    userDataPath: userData,
    plugins,
    profileProtection,
  });
  natsBackend = createNatsBackend({
    profileStore: createElectronNatsProfileStore({ userDataPath: userData, profileProtection }),
  });
  providers = new ProviderHostRegistry([
    createKafkaProviderEndpoint(backend),
    createNatsProviderEndpoint(natsBackend),
  ]);
  runningShell = await createElectronShell({
    registry: providers,
    deliveryBindings: [
      createKafkaElectronDeliveryBinding(backend),
      createNatsElectronDeliveryBinding(),
    ],
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
