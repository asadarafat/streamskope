import { join } from "node:path";

import { app, dialog, net, safeStorage, session } from "electron";

import {
  formatOperationalDiagnostic,
  operationalDiagnostic,
  OperationalDiagnosticError,
  type OperationalDiagnosticCode,
} from "../../diagnostics";
import { PluginRuntime } from "../../node/plugins/runtime";
import { PluginStore } from "../../node/plugins/store";
import { ProviderHostRegistry } from "../../node/provider-host";
import { createKafkaProviderEndpoint } from "../../node/kafka-provider";
import { createNatsBackend } from "../../node/nats-backend";
import { createNatsProviderEndpoint } from "../../node/nats-provider";

import { confirmPluginExit } from "./plugin-exit";
import { createPluginPackageFilePicker } from "./plugin-file-picker";
import { createPluginNetworkTransport } from "./plugin-network-transport";
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
let startupFailure: OperationalDiagnosticCode = "DESKTOP_START_FAILED";

function reportFailure(error: unknown, fallback: OperationalDiagnosticCode): void {
  try {
    process.stderr.write(
      `${formatOperationalDiagnostic(operationalDiagnostic(error, fallback))}\n`,
    );
  } catch {
    // Diagnostics must never interrupt cleanup or change the application's exit status.
  }
}

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
  const attempts: { code: OperationalDiagnosticCode; promise: Promise<void> }[] = [];
  const cleanup = (code: OperationalDiagnosticCode, operation: () => Promise<void>): void => {
    try {
      attempts.push({ code, promise: operation() });
    } catch (cause) {
      attempts.push({
        code,
        promise: Promise.reject(new OperationalDiagnosticError(code, { cause })),
      });
    }
  };
  const shell = runningShell;
  if (shell !== undefined) cleanup("DESKTOP_SHELL_CLEANUP_UNCONFIRMED", () => shell.close());
  const ownedProviders: readonly [
    typeof backend | typeof natsBackend | typeof providers,
    OperationalDiagnosticCode,
  ][] =
    providers === undefined
      ? [
          [backend, "KAFKA_CLEANUP_UNCONFIRMED"],
          [natsBackend, "NATS_CLEANUP_UNCONFIRMED"],
        ]
      : [[providers, "PROVIDER_CLEANUP_UNCONFIRMED"]];
  for (const [owner, code] of ownedProviders) {
    if (owner !== undefined) cleanup(code, () => owner.shutdown());
  }
  void Promise.allSettled(attempts.map(({ promise }) => promise)).then((results) => {
    const failed = results.some((result) => result.status === "rejected");
    results.forEach((result, index) => {
      if (result.status === "rejected") reportFailure(result.reason, attempts[index]!.code);
    });
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
  const userDataPath = app.getPath("userData");
  startupFailure = "PROFILE_PROTECTION_START_FAILED";
  const profileProtection = await initializeElectronProfileProtection(
    safeStorage,
    process.platform,
  );
  startupFailure = "PLUGIN_RUNTIME_START_FAILED";
  const plugins = new PluginRuntime({
    store: new PluginStore(join(userDataPath, "plugins")),
    networkTransport: createPluginNetworkTransport({
      session: session.fromPartition("streamskope-plugin-downloads", { cache: false }),
      request: (options) => net.request(options),
    }),
    ...(profileProtection.protector === undefined
      ? {}
      : { networkProtector: profileProtection.protector }),
    choosePackageFile: createPluginPackageFilePicker((options) =>
      runningShell === undefined
        ? dialog.showOpenDialog(options)
        : dialog.showOpenDialog(runningShell.window, options),
    ),
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
    startupFailure = "DESKTOP_SHELL_START_FAILED";
    installPackagedRendererProtocol(join(__dirname, "..", "renderer"), (path) =>
      plugins.rendererAsset(path),
    );
  }
  startupFailure = "KAFKA_RUNTIME_START_FAILED";
  backend = await createElectronKafkaBackend({
    platform: process.platform,
    safeStorage,
    userDataPath,
    plugins,
    profileProtection,
  });
  startupFailure = "NATS_RUNTIME_START_FAILED";
  natsBackend = createNatsBackend({
    profileStore: createElectronNatsProfileStore({ userDataPath, profileProtection }),
  });
  startupFailure = "PRIVATE_HOST_START_FAILED";
  providers = new ProviderHostRegistry([
    createKafkaProviderEndpoint(backend),
    createNatsProviderEndpoint(natsBackend),
  ]);
  startupFailure = "DESKTOP_SHELL_START_FAILED";
  runningShell = await createElectronShell({
    exportFiles: backend.exportFiles,
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
  reportFailure(error, startupFailure);
  void shutdown(1);
});
