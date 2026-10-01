import { execFile, spawn, type ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import {
  _electron as electron,
  expect,
  test,
  type ElectronApplication,
  type Page,
} from "@playwright/test";

import { HOST_PROTOCOL_VERSION, type StreamSkopeHost } from "../../src/features/kafka/contracts";
import { buildElectronSmoke, buildRenderer } from "../support/electron-application";
import { pluginPackageFixtures } from "../support/plugin-package-fixture";

const run = promisify(execFile);

async function openPlugins(page: Page): Promise<void> {
  await page.getByRole("button", { name: "Preferences", exact: true }).click();
  await page.getByRole("tab", { name: "Plugins", exact: true }).click();
  await expect(page.getByRole("region", { name: "EDA Capture", exact: true })).toBeVisible();
}

async function closePreferences(page: Page): Promise<void> {
  await page
    .getByRole("dialog", { name: "Workbench Preferences" })
    .getByRole("button", { name: "Close", exact: true })
    .click();
}

async function pluginInstallation(page: Page): Promise<
  | {
      activationId: string | undefined;
      rendererUrl: string | undefined;
      version: string | undefined;
    }
  | undefined
> {
  return page.evaluate(async (version) => {
    const host = (window as unknown as { streamSkopeHost: StreamSkopeHost }).streamSkopeHost;
    const response = await host.execute({
      command: "plugins.list",
      id: crypto.randomUUID(),
      payload: {},
      version,
    });
    if (!response.ok) throw new Error(response.error.summary);
    const plugin = response.result.pluginSnapshot.plugins.find(
      (entry) => entry.id === "streamskope.eda",
    );
    return plugin === undefined
      ? undefined
      : {
          activationId: plugin.activationId,
          rendererUrl: plugin.rendererUrl,
          version: plugin.active?.version,
        };
  }, HOST_PROTOCOL_VERSION);
}

async function openCapture(page: Page, screenshot?: string): Promise<void> {
  await page.getByRole("button", { name: "Add connection", exact: true }).click();
  await page.getByRole("menuitem", { name: "Capture from EDA", exact: true }).click();
  const capture = page.getByRole("dialog", { name: "Capture Nokia EDA streams" });
  await expect(capture.getByLabel("EDA API URL")).toBeVisible();
  await expect(capture.getByLabel("EDA username")).toBeVisible();
  await expect(capture.getByRole("button", { name: "Discover sources" })).toBeVisible();
  if (screenshot !== undefined) await page.screenshot({ path: screenshot, animations: "disabled" });
  await page.keyboard.press("Escape");
  await expect(capture).toHaveCount(0);
}

test("installs, updates, removes and reinstalls EDA in one production Electron window", async ({
  browserName: _browserName,
}, info) => {
  test.setTimeout(180_000);
  test.skip(
    process.platform === "linux" &&
      !["/usr/bin/dbus-daemon", "/usr/bin/dbus-send", "/usr/bin/gnome-keyring-daemon"].every(
        existsSync,
      ),
    "Real protected-storage acceptance requires D-Bus and GNOME Keyring on Linux.",
  );
  if (process.env.STREAMSKOPE_PLUGIN_PACKAGE_READY !== "1")
    await run(process.execPath, ["--import", "tsx", "tools/package.ts", "plugin"], {
      maxBuffer: 4 * 1_048_576,
    });
  const { current: original, update } = await pluginPackageFixtures();
  const updateVersion = update.manifest.version;
  await mkdir(resolve("dist"), { recursive: true });
  const directory = await mkdtemp(join(resolve("dist"), "electron-plugin-e2e-"));
  const catalogPath = join(directory, "catalog.skope-plugin");
  const environment = Object.fromEntries(
    Object.entries(process.env).filter(
      (entry): entry is [string, string] => entry[1] !== undefined,
    ),
  );
  delete environment.ELECTRON_RUN_AS_NODE;
  environment.STREAMSKOPE_PLUGIN_TEST_RENDERER = join(directory, "renderer");
  environment.STREAMSKOPE_PLUGIN_TEST_USER_DATA = join(directory, "user-data");
  environment.STREAMSKOPE_PLUGIN_TEST_PACKAGE = catalogPath;
  let busPid: number | undefined;
  let keyring: ChildProcess | undefined;
  let application: ElectronApplication | undefined;
  const errors: string[] = [];
  const assetFailures: string[] = [];
  const hostOutput: string[] = [];
  try {
    await buildElectronSmoke(directory, "plugin");
    await buildRenderer(directory);
    await writeFile(catalogPath, original.bytes);
    if (process.platform === "linux") {
      environment.XDG_DATA_HOME = join(directory, "data");
      environment.XDG_CONFIG_HOME = join(directory, "config");
      environment.GNOME_KEYRING_CONTROL = join(directory, "keyring");
      for (const path of [
        environment.XDG_DATA_HOME,
        environment.XDG_CONFIG_HOME,
        environment.GNOME_KEYRING_CONTROL,
      ])
        await mkdir(path, { recursive: true });
      const bus = await run("dbus-daemon", [
        "--session",
        "--fork",
        "--print-address=1",
        "--print-pid=1",
      ]);
      const [address, pid] = bus.stdout.trim().split("\n");
      if (address === undefined || pid === undefined || !/^\d+$/u.test(pid))
        throw new Error("D-Bus did not report an isolated session.");
      environment.DBUS_SESSION_BUS_ADDRESS = address;
      busPid = Number(pid);
      keyring = spawn(
        "gnome-keyring-daemon",
        [
          "--foreground",
          "--unlock",
          "--components=secrets",
          `--control-directory=${environment.GNOME_KEYRING_CONTROL}`,
        ],
        { env: environment, stdio: ["pipe", "ignore", "ignore"] },
      );
      keyring.stdin?.end("\n");
      await expect
        .poll(async () => {
          const { stdout } = await run(
            "dbus-send",
            [
              "--session",
              "--print-reply",
              "--dest=org.freedesktop.DBus",
              "/org/freedesktop/DBus",
              "org.freedesktop.DBus.NameHasOwner",
              "string:org.freedesktop.secrets",
            ],
            { env: environment },
          );
          return stdout.includes("boolean true");
        })
        .toBe(true);
    }
    const require = createRequire(resolve("package.json"));
    application = await electron.launch({
      executablePath: require("electron") as string,
      args: [
        ...(process.getuid?.() === 0 ? ["--no-sandbox"] : []),
        ...(process.platform === "linux" ? ["--password-store=gnome-libsecret"] : []),
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

    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await expect(page.getByRole("menuitem", { name: "Capture from EDA", exact: true })).toHaveCount(
      0,
    );
    await page.keyboard.press("Escape");
    await openPlugins(page);
    const card = page.getByRole("region", { name: "EDA Capture", exact: true });
    await card.getByRole("button", { name: "Install", exact: true }).click();
    await expect(card).toContainText(`Active version ${original.manifest.version}`);
    const installed = await pluginInstallation(page);
    expect(installed?.activationId).toBeTruthy();
    await closePreferences(page);
    await openCapture(page);

    await writeFile(catalogPath, update.bytes);
    await openPlugins(page);
    await card.getByRole("button", { name: `Update to ${updateVersion}`, exact: true }).click();
    await expect(card).toContainText(`Active version ${updateVersion}`);
    const replacement = await pluginInstallation(page);
    expect(replacement?.activationId).not.toBe(installed?.activationId);
    expect(replacement?.rendererUrl).not.toBe(installed?.rendererUrl);
    await closePreferences(page);
    await openCapture(page);

    await openPlugins(page);
    await card.getByRole("button", { name: "Remove", exact: true }).click();
    await page.getByRole("button", { name: "Remove plugin", exact: true }).click();
    await expect(card).toContainText("Not installed");
    await closePreferences(page);
    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await expect(page.getByRole("menuitem", { name: "Capture from EDA", exact: true })).toHaveCount(
      0,
    );
    await expect(
      page.getByRole("menuitem", { name: "Existing Kafka cluster", exact: true }),
    ).toBeVisible();
    await page.keyboard.press("Escape");

    await openPlugins(page);
    await card.getByRole("button", { name: "Install", exact: true }).click();
    await expect(card).toContainText(`Active version ${updateVersion}`);
    const reinstalled = await pluginInstallation(page);
    expect(reinstalled?.activationId).not.toBe(replacement?.activationId);
    expect(reinstalled?.rendererUrl).not.toBe(replacement?.rendererUrl);
    await closePreferences(page);
    await openCapture(page, info.outputPath("electron-plugin-hot-lifecycle.png"));
    expect(await page.evaluate(() => performance.timeOrigin)).toBe(origin);
    expect(application.process().pid).toBe(processId);
    expect(application.windows()).toEqual([page]);
    expect(errors).toEqual([]);
    expect(assetFailures).toEqual([]);
    await info.attach("plugin-lifecycle-evidence", {
      body: JSON.stringify(
        {
          security,
          installed,
          replacement,
          reinstalled,
          sameProcess: true,
          sameWindow: true,
          rendererReloaded: false,
        },
        null,
        2,
      ),
      contentType: "application/json",
    });
    await application.close();
    application = undefined;
  } finally {
    await application?.close();
    await info.attach("electron-host-output", {
      body: hostOutput.join(""),
      contentType: "text/plain",
    });
    keyring?.kill("SIGTERM");
    if (busPid !== undefined) process.kill(busPid, "SIGTERM");
    await rm(directory, { recursive: true, force: true });
  }
});
