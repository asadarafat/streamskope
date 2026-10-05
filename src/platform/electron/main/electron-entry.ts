import { join } from "node:path";

import { app, dialog, safeStorage } from "electron";

import { PluginRuntime } from "../../node/plugins/runtime";
import { PluginStore } from "../../node/plugins/store";
import { ProviderHostRegistry } from "../../node/provider-host";
import { createKafkaProviderEndpoint } from "../../node/kafka-provider";
import { createNatsBackend } from "../../node/nats-backend";
import { createNatsProviderEndpoint } from "../../node/nats-provider";

import { confirmPluginExit } from "./plugin-exit";
import { createElectronKafkaBackend } from "./electron-kafka-backend";
import { createElectronShell, type RunningElectronShell } from "./electron-shell";
import { createKafkaElectronDeliveryBinding } from "./kafka-provider-delivery";
import { createNatsElectronDeliveryBinding } from "./nats-provider-delivery";
import { createElectronNatsProfileStore } from "./electron-nats-profile-store";
import { initializeElectronProfileProtection } from "./electron-profile-protection";
import {
  installPackagedRendererProtocol,
  PACKAGED_RENDERER_URL,
  registerPackagedRendererScheme,
} from "./packaged-renderer-protocol";

let backend: Awaited<ReturnType<typeof createElectronKafkaBackend>> | undefined;
let natsBackend: ReturnType<typeof createNatsBackend> | undefined;
let providers: ProviderHostRegistry | undefined;
let runningShell: RunningElectronShell | undefined;
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
  let complete = (): void => undefined;
  shutdownPromise = new Promise<void>((resolve) => {
    complete = resolve;
  });
  const attempts: Promise<void>[] = [];
  try {
    if (runningShell !== undefined) attempts.push(runningShell.close());
  } catch (cause) {
    attempts.push(Promise.reject(new Error("Desktop shell cleanup failed.", { cause })));
  }
  const ownedProviders = providers === undefined ? [backend, natsBackend] : [providers];
  for (const owner of ownedProviders) {
    try {
      if (owner !== undefined) attempts.push(owner.shutdown());
    } catch (cause) {
      attempts.push(Promise.reject(new Error("Desktop provider shutdown failed.", { cause })));
    }
  }
  void Promise.allSettled(attempts).then((results) => {
    const failed = results.some((result) => result.status === "rejected");
    try {
      if (restartRequested && !failed) app.relaunch();
      app.exit(failed ? 1 : exitCode);
    } finally {
      complete();
    }
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
  const userDataPath = app.getPath("userData");
  const profileProtection = await initializeElectronProfileProtection(
    safeStorage,
    process.platform,
  );
  backend = await createElectronKafkaBackend({
    platform: process.platform,
    safeStorage,
    userDataPath,
    plugins,
    profileProtection,
  });
  natsBackend = createNatsBackend({
    profileStore: createElectronNatsProfileStore({ userDataPath, profileProtection }),
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
    rendererUrl: developmentRendererUrl ?? PACKAGED_RENDERER_URL,
  });
  runningShell.window.on("close", (event) => {
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
