import { join } from "node:path";

import { app, dialog, safeStorage } from "electron";

import { PluginRuntime } from "../../node/plugins/runtime";
import { PluginStore } from "../../node/plugins/store";
import { ProviderHostRegistry } from "../../node/provider-host";
import { createKafkaProviderEndpoint } from "../../node/kafka-provider";

import { confirmPluginExit } from "./plugin-exit";
import { createElectronKafkaBackend } from "./electron-kafka-backend";
import { createElectronShell } from "./electron-shell";
import { createKafkaElectronDeliveryBinding } from "./kafka-provider-delivery";
import {
  installPackagedRendererProtocol,
  PACKAGED_RENDERER_URL,
  registerPackagedRendererScheme,
} from "./packaged-renderer-protocol";

let backend: Awaited<ReturnType<typeof createElectronKafkaBackend>> | undefined;
let providers: ProviderHostRegistry | undefined;
let shutdownPromise: Promise<void> | undefined;
let exitPending = false;
let restartRequested = false;

async function requestExit(): Promise<void> {
  if (exitPending || shutdownPromise !== undefined) return;
  exitPending = true;
  try {
    const allowed =
      backend === undefined ||
      (await confirmPluginExit(
        backend,
        async (prompt) => {
          const cancelId = Math.max(
            0,
            prompt.actions.findIndex((action) => action.id === prompt.cancelAction),
          );
          const result = await dialog.showMessageBox({
            type: "question",
            title: prompt.title,
            message: prompt.message,
            detail: prompt.detail,
            buttons: prompt.actions.map((action) => action.label),
            defaultId: cancelId,
            cancelId,
          });
          return prompt.actions[result.response]?.id ?? prompt.cancelAction;
        },
        async (message) => {
          await dialog.showMessageBox({ type: "error", title: "Cleanup pending", message });
        },
      ));
    if (allowed) await shutdown(0);
    else restartRequested = false;
  } finally {
    exitPending = false;
  }
}

registerPackagedRendererScheme();

function shutdown(exitCode: number): Promise<void> {
  if (shutdownPromise !== undefined) {
    return shutdownPromise;
  }
  shutdownPromise = Promise.resolve().then(async (): Promise<void> => {
    let cleanupFailure: unknown;
    try {
      if (providers !== undefined) await providers.shutdown();
      else await backend?.shutdown();
    } catch (error) {
      cleanupFailure = error;
    }
    if (restartRequested && cleanupFailure === undefined) app.relaunch();
    app.exit(cleanupFailure === undefined ? exitCode : 1);
  });
  return shutdownPromise;
}

async function start(): Promise<void> {
  await app.whenReady();
  const developmentRendererUrl = process.env.STREAMSKOPE_RENDERER_URL;
  const plugins = new PluginRuntime({
    store: new PluginStore(join(app.getPath("userData"), "plugins")),
    restart: (): void => {
      if (exitPending || shutdownPromise !== undefined) return;
      restartRequested = true;
      // Let the command acknowledgement reach the renderer before requesting application exit.
      setTimeout(() => {
        void requestExit();
      }, 100);
    },
  });
  if (developmentRendererUrl === undefined) {
    installPackagedRendererProtocol(join(__dirname, "..", "renderer"), (path) =>
      plugins.rendererAsset(path),
    );
  }
  backend = await createElectronKafkaBackend({
    platform: process.platform,
    safeStorage,
    userDataPath: app.getPath("userData"),
    plugins,
  });
  providers = new ProviderHostRegistry([createKafkaProviderEndpoint(backend)]);
  const shell = await createElectronShell({
    registry: providers,
    deliveryBindings: [createKafkaElectronDeliveryBinding(backend)],
    preloadPath: join(__dirname, "preload.cjs"),
    rendererUrl: developmentRendererUrl ?? PACKAGED_RENDERER_URL,
  });
  shell.window.on("close", (event) => {
    if (shutdownPromise !== undefined) return;
    event.preventDefault();
    void requestExit();
  });
}

app.on("before-quit", (event) => {
  if (shutdownPromise !== undefined) return;
  event.preventDefault();
  void requestExit();
});

app.on("window-all-closed", () => {
  void shutdown(0);
});

void start().catch((error: unknown) => {
  const summary = error instanceof Error ? error.message : "Unknown startup failure.";
  process.stderr.write(`StreamSkope startup failed: ${summary}\n`);
  void shutdown(1);
});
