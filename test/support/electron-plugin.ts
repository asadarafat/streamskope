import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { join, resolve } from "node:path";

import {
  _electron as electron,
  expect,
  type ElectronApplication,
  type Page,
  type TestInfo,
} from "@playwright/test";

import { buildElectronSmoke, buildRenderer } from "./electron-application";
import {
  protectedStorageSessionAvailable,
  startProtectedStorageSession,
} from "./protected-storage-session";

export const electronPluginStorageAvailable = protectedStorageSessionAvailable;

/** Production Electron shell and protected store; only the package catalog is local. */
export async function startElectronPluginFixture(
  bytes: Uint8Array,
  info: TestInfo,
  sensitiveValues: readonly string[] = [],
  environmentOverrides: Readonly<Record<string, string>> = {},
): Promise<{
  application: ElectronApplication;
  page: Page;
  directory: string;
  catalogPath: string;
  userDataPath: string;
  processId: number | undefined;
  origin: number;
  security: {
    contextIsolation: boolean;
    nodeIntegration: boolean;
    sandbox: boolean;
    storage: string;
  };
  errors: string[];
  assetFailures: string[];
  close(): Promise<void>;
}> {
  await mkdir(resolve("dist"), { recursive: true });
  const directory = await mkdtemp(join(resolve("dist"), "electron-plugin-e2e-"));
  const catalogPath = join(directory, "catalog.skope-plugin");
  const environment = Object.fromEntries(
    Object.entries(process.env).filter(
      (entry): entry is [string, string] => entry[1] !== undefined,
    ),
  );
  Object.assign(environment, environmentOverrides);
  delete environment.ELECTRON_RUN_AS_NODE;
  environment.STREAMSKOPE_PLUGIN_TEST_RENDERER = join(directory, "renderer");
  environment.STREAMSKOPE_PLUGIN_TEST_USER_DATA = join(directory, "user-data");
  environment.STREAMSKOPE_PLUGIN_TEST_PACKAGE = catalogPath;
  let protectedStorage: Awaited<ReturnType<typeof startProtectedStorageSession>> | undefined;
  let application: ElectronApplication | undefined;
  const errors: string[] = [];
  const assetFailures: string[] = [];
  const hostOutput: string[] = [];
  try {
    await buildElectronSmoke(directory, "plugin");
    await buildRenderer(directory);
    await writeFile(catalogPath, bytes);
    protectedStorage = await startProtectedStorageSession(directory);
    Object.assign(environment, protectedStorage.environment);
    const require = createRequire(resolve("package.json"));
    application = await electron.launch({
      executablePath: require("electron") as string,
      args: [
        ...(process.getuid?.() === 0 ? ["--no-sandbox"] : []),
        ...protectedStorage.electronArguments,
        join(directory, "plugin-main.cjs"),
      ],
      env: environment,
    });
    application
      .process()
      .stderr?.on("data", (chunk: Buffer) => hostOutput.push(chunk.toString("utf8")));
    const processId = application.process().pid;
    const page = await application.firstWindow();
    page.on("pageerror", (error) => errors.push(error.message));
    page.on("console", (message) => {
      if (message.type() === "error") errors.push(message.text());
    });
    page.on("requestfailed", (request) => {
      if (request.url().includes("/plugins/")) assetFailures.push(request.url());
    });
    await page.setViewportSize({ width: 1200, height: 800 });
    await expect(page.getByLabel("Profile storage status")).toContainText("OS-protected profiles", {
      timeout: 15_000,
    });
    expect(page.url()).toBe("streamskope://app/");
    const origin = await page.evaluate(() => performance.timeOrigin);
    const security = await application.evaluate(({ BrowserWindow, safeStorage }) => {
      const webContents = BrowserWindow.getAllWindows()[0]!.webContents as unknown as {
        getLastWebPreferences(): {
          contextIsolation: boolean;
          nodeIntegration: boolean;
          sandbox: boolean;
        };
      };
      const preferences = webContents.getLastWebPreferences();
      return {
        contextIsolation: preferences.contextIsolation,
        nodeIntegration: preferences.nodeIntegration,
        sandbox: preferences.sandbox,
        storage: safeStorage.getSelectedStorageBackend(),
      };
    });
    expect(security.contextIsolation).toBe(true);
    expect(security.nodeIntegration).toBe(false);
    expect(security.sandbox).toBe(true);
    if (process.platform === "linux") expect(security.storage).toBe("gnome_libsecret");

    const launched = application;
    return {
      application: launched,
      page,
      directory,
      catalogPath,
      userDataPath: environment.STREAMSKOPE_PLUGIN_TEST_USER_DATA,
      processId,
      origin,
      security,
      errors,
      assetFailures,
      close: async (): Promise<void> => {
        try {
          await launched.close();
        } finally {
          await cleanup();
        }
      },
    };
  } catch (error) {
    await application?.close();
    await cleanup();
    throw error;
  }
  async function cleanup(): Promise<void> {
    let output = [hostOutput.join(""), ...errors, ...assetFailures].join("\n");
    for (const value of sensitiveValues) if (value) output = output.replaceAll(value, "[redacted]");
    await info.attach("electron-host-output", { body: output, contentType: "text/plain" });
    await protectedStorage?.dispose();
    await rm(directory, { recursive: true, force: true });
  }
}
