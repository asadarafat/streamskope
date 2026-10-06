import { execFile } from "node:child_process";
import { createServer, type Server } from "node:http";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

import { chromium } from "@playwright/test";
import { afterAll, beforeAll, expect, it } from "vitest";

import type { StreamSkopeHost } from "../../src/features/kafka/contracts";
import type { PluginRenderer } from "../../src/plugins/renderer-api";
import {
  parsePluginPackage,
  type VerifiedPluginPackage,
} from "../../src/platform/node/plugins/package";
import { PluginStore } from "../../src/platform/node/plugins/store";
import { PluginRuntime } from "../../src/platform/node/plugins/runtime";
import {
  NSP_WORKFLOW_DEFINITION,
  NSP_WORKFLOW_FINGERPRINT,
} from "../../plugins/nsp/backend/workflow";
import { builtPluginAssets } from "../support/plugin-package-fixture";
import {
  compareSemanticVersions,
  isPluginCompatibleWithHost,
} from "../../src/plugins/compatibility";
import { DevelopmentPluginCatalog } from "../../tools/dev/plugin-catalog";

const execute = promisify(execFile);
let directory: string;
let plugin: VerifiedPluginPackage;
let backendPath: string;
let nspPlugin: VerifiedPluginPackage;
let nspBackendPath: string;

beforeAll(async () => {
  directory = await mkdtemp(join(tmpdir(), "streamskope-plugin-distribution-"));
  await execute(process.execPath, ["--import", "tsx", "tools/package.ts", "plugin"], {
    timeout: 90_000,
    maxBuffer: 2 * 1024 * 1024,
  });
  const bytes = await readFile(
    join("dist/plugin-package", (await builtPluginAssets("eda")).packageAsset),
  );
  plugin = parsePluginPackage(bytes);
  const store = new PluginStore(directory);
  await store.install(bytes, plugin.sha256);
  const nspBytes = await readFile(
    join("dist/plugin-package", (await builtPluginAssets("nsp")).packageAsset),
  );
  nspPlugin = parsePluginPackage(nspBytes);
  await store.install(nspBytes, nspPlugin.sha256);
  const activated = await store.activatePending();
  const active = activated.find((candidate) => candidate.manifest.id === plugin.manifest.id);
  if (!active) throw new Error("The packaged EDA plugin did not activate.");
  backendPath = active.backendPath;
  const nspActive = activated.find((candidate) => candidate.manifest.id === nspPlugin.manifest.id);
  if (!nspActive) throw new Error("The packaged NSP plugin did not activate.");
  nspBackendPath = nspActive.backendPath;
}, 95_000);

afterAll(async () => {
  if (directory) await rm(directory, { recursive: true, force: true });
});

it("publishes matching package metadata and loads the backend without the repository or node_modules", async () => {
  expect(
    JSON.parse(
      await readFile(
        join("dist/plugin-package", (await builtPluginAssets("eda")).manifestAsset),
        "utf8",
      ),
    ),
  ).toEqual(plugin.manifest);
  const script = join(directory, "probe.cjs");
  await writeFile(
    script,
    `
    const assert = require('node:assert/strict');
    const { activate } = require(process.argv[2]);
    const host = {
      probeTopics: async () => { throw new Error('Preflight must not contact Kafka'); },
      execute: async () => { throw new Error('Preflight must not execute core commands'); },
      publish() {}, recordActivity() {}, profiles: async () => [], deleteProfile: async () => {},
      connectionActive: () => false,
      failure: error => { throw error; },
    };
    (async () => {
      const backend = await activate(host);
      const response = await backend.execute({
        method: 'edaCapture.preflight', input: {}, requestId: 'distribution-test', correlationId: 'distribution-test'
      });
      assert.equal(response.ok, true);
      assert.equal(response.result.captureHost.state, 'configured');
      assert.equal(await backend.beforeExit(), undefined);
      await backend.close();
      console.log('standalone-backend-ok');
    })().catch(error => { console.error(error); process.exitCode = 1; });
  `,
  );
  const result = await execute(process.execPath, [script, backendPath], {
    cwd: directory,
    env: { ...process.env, NODE_OPTIONS: "", NODE_PATH: "" },
    timeout: 15_000,
  });
  expect(result.stdout.trim()).toBe("standalone-backend-ok");
});

it("loads the packaged NSP backend independently with no network or repository dependencies", async () => {
  expect(
    JSON.parse(
      await readFile(
        join("dist/plugin-package", (await builtPluginAssets("nsp")).manifestAsset),
        "utf8",
      ),
    ),
  ).toEqual(nspPlugin.manifest);
  const script = join(directory, "nsp-probe.cjs");
  await writeFile(
    script,
    `
    const assert = require('node:assert/strict');
    const { activate } = require(process.argv[2]);
    let state = null;
    const host = {
      probeTopics: async () => { throw new Error('Status must not contact Kafka'); },
      execute: async () => { throw new Error('Status must not execute core commands'); },
      publish() {}, recordActivity() {}, profiles: async () => [], deleteProfile: async () => {},
      connectionActive: () => false,
      failure: error => { throw error; },
      recoveryState: { read: async () => state, write: async value => { state = value; } },
    };
    (async () => {
      const backend = await activate(host);
      const response = await backend.execute({
        method: 'nspCapture.status', input: {}, requestId: 'distribution-test', correlationId: 'distribution-test'
      });
      assert.equal(response.ok, true);
      assert.equal(response.status.state, 'idle');
      assert.equal(await backend.beforeExit?.(), undefined);
      await backend.close();
      console.log('standalone-nsp-backend-ok');
    })().catch(error => { console.error(error); process.exitCode = 1; });
  `,
  );
  const result = await execute(process.execPath, [script, nspBackendPath], {
    cwd: directory,
    env: { ...process.env, NODE_OPTIONS: "", NODE_PATH: "" },
    timeout: 15_000,
  });
  expect(result.stdout.trim()).toBe("standalone-nsp-backend-ok");
});

it("packages distinct development identities without changing source and includes the exact NSP workflow", async () => {
  const files = (await readdir("dist/plugin-package")).sort();
  const edaAssets = await builtPluginAssets("eda");
  const nspAssets = await builtPluginAssets("nsp");
  expect(files).toEqual(
    [
      edaAssets.manifestAsset,
      edaAssets.packageAsset,
      `${nspAssets.prefix}-nsp-capture.workflow.yaml`,
      nspAssets.manifestAsset,
      nspAssets.packageAsset,
    ].sort(),
  );
  for (const packaged of [plugin, nspPlugin]) {
    expect(packaged.manifest).toMatchObject({
      apiVersion: 4,
      compatibility: { streamskope: { minimum: "0.9.0", maximumExclusive: "0.10.0" } },
    });
    const releaseManifest = { ...packaged.manifest, version: "0.1.0" };
    expect(isPluginCompatibleWithHost(releaseManifest, "v0.9.0")).toBe(true);
    expect(isPluginCompatibleWithHost(releaseManifest, "v0.9.1")).toBe(true);
    expect(isPluginCompatibleWithHost(releaseManifest, "v0.8.99")).toBe(false);
    expect(isPluginCompatibleWithHost(releaseManifest, "v0.10.0")).toBe(false);
    expect(packaged.manifest.version).toMatch(/^0\.0\.0-dev\.[1-9]\d*$/u);
    expect(packaged.manifest).not.toHaveProperty("revision");
  }
  expect(
    compareSemanticVersions(nspPlugin.manifest.version, plugin.manifest.version),
  ).toBeGreaterThan(0);
  for (const name of ["eda", "nsp"]) {
    expect(
      JSON.parse(await readFile(join("plugins", name, "manifest.json"), "utf8")),
    ).toHaveProperty("version", "0.0.0-dev");
  }
  const resource = nspPlugin.files.get("nsp-capture.workflow.yaml")!;
  expect(Buffer.from(resource).toString("utf8")).toBe(NSP_WORKFLOW_DEFINITION);
  expect(NSP_WORKFLOW_FINGERPRINT).toBe(
    "fbd1ad41aa8fb3bf09adbdadd5ad03ea439edbc6e687b2b66fce2b76db9a920b",
  );
  expect(nspPlugin.manifest.resources).toEqual([
    { path: "nsp-capture.workflow.yaml", sha256: NSP_WORKFLOW_FINGERPRINT },
  ]);
  const download = await readFile(
    join(
      "dist/plugin-package",
      files.find((name) => name.endsWith(".yaml"))!,
    ),
  );
  expect(download).toEqual(Buffer.from(resource));
  expect(download).toEqual(await readFile("plugins/nsp/resources/nsp-capture.workflow.yaml"));
});

async function listen(server: Server): Promise<number> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("Expected a TCP server.");
  return address.port;
}

it("loads the packaged renderer in a browser and renders capture through the generic plugin bridge", async () => {
  const server = createServer((request, response) => {
    if (request.url === "/") {
      response.setHeader("Content-Type", "text/html");
      response.end(
        '<!doctype html><html><body><div id="plugin"></div><script type="module">import renderer from "/renderer.js"; window.pluginRenderer = renderer;</script></body></html>',
      );
      return;
    }
    const name = request.url?.slice(1);
    const bytes = name === undefined ? undefined : plugin.files.get(name);
    if (bytes === undefined) {
      response.writeHead(404).end();
      return;
    }
    response.setHeader("Content-Type", name?.endsWith(".css") ? "text/css" : "text/javascript");
    response.end(bytes);
  });
  const port = await listen(server);
  try {
    const browser = await chromium.launch({ headless: true });
    try {
      const page = await browser.newPage();
      page.setDefaultTimeout(5_000);
      const errors: string[] = [];
      const unexpectedRequests: string[] = [];
      page.on("pageerror", (error) => errors.push(error.message));
      page.on("request", (request) => {
        if (!request.url().startsWith(`http://127.0.0.1:${port}/`))
          unexpectedRequests.push(request.url());
      });
      await page.goto(`http://127.0.0.1:${port}/`);
      await page.waitForFunction(() => "pluginRenderer" in window);
      const identity = await page.evaluate(() => {
        const renderer = (window as unknown as { pluginRenderer: PluginRenderer }).pluginRenderer;
        const requests: string[] = [];
        const host = {
          execute: (command: {
            command: string;
            id: string;
            version: number;
            payload: { method: string };
          }): Promise<unknown> => {
            if (command.command !== "plugin.execute") throw new Error("Unexpected core command.");
            requests.push(command.payload.method);
            return Promise.resolve({
              command: command.command,
              id: command.id,
              version: command.version,
              ok: true,
              result: {
                correlationId: command.id,
                output: {
                  command: command.payload.method,
                  id: command.id,
                  version: 1,
                  ok: true,
                  result: {
                    correlationId: command.id,
                    ...(command.payload.method === "edaCapture.preflight"
                      ? {
                          captureHost: {
                            state: "configured",
                            context: "eda-agent",
                            detail: "Packaged backend available",
                          },
                        }
                      : { captureSession: { state: "idle", tunnel: "closed" } }),
                  },
                },
              },
            });
          },
          subscribe: (): (() => void) => () => {},
          openExternalUrl: (): Promise<void> => Promise.resolve(),
        } as unknown as StreamSkopeHost;
        const element = document.getElementById("plugin");
        if (!element) throw new Error("Missing plugin mount point.");
        const mounted = renderer.mount(element, {
          view: "connection",
          actionId: "capture",
          profiles: [],
          host,
          onClose() {},
          onProfileReady() {},
          onExistingDestination() {},
        });
        Object.assign(window, {
          pluginRequests: requests,
          disposePlugin: (): void => mounted.dispose(),
        });
        return { id: renderer.id, action: renderer.connectionActions[0]?.label };
      });
      expect(identity).toEqual({ id: "streamskope.eda", action: "Capture from EDA" });
      await page.getByRole("dialog").waitFor();
      expect(await page.getByLabel("EDA API URL").isVisible()).toBe(true);
      expect(await page.getByRole("button", { name: "Discover sources" }).isVisible()).toBe(true);
      await page.waitForFunction(
        () => (window as unknown as { pluginRequests: string[] }).pluginRequests.length >= 2,
      );
      expect(
        await page.evaluate(() =>
          (window as unknown as { pluginRequests: string[] }).pluginRequests.sort(),
        ),
      ).toEqual(["edaCapture.preflight", "edaCapture.status"]);
      await page.evaluate(() => {
        (window as unknown as { disposePlugin(): void }).disposePlugin();
      });
      await page.getByRole("dialog").waitFor({ state: "detached" });
      expect(errors).toEqual([]);
      expect(unexpectedRequests).toEqual([]);
    } finally {
      await browser.close();
    }
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
}, 30_000);

it("renders the packaged NSP onboarding with only the generic plugin bridge", async () => {
  const server = createServer((request, response) => {
    if (request.url === "/") {
      response.setHeader("Content-Type", "text/html");
      response.end(
        '<!doctype html><html><body><div id="plugin"></div><script type="module">import renderer from "/renderer.js"; window.pluginRenderer = renderer;</script></body></html>',
      );
      return;
    }
    const name = request.url?.slice(1);
    const bytes = name === undefined ? undefined : nspPlugin.files.get(name);
    if (bytes === undefined) {
      response.writeHead(404).end();
      return;
    }
    response.setHeader("Content-Type", name?.endsWith(".css") ? "text/css" : "text/javascript");
    response.end(bytes);
  });
  const port = await listen(server);
  try {
    const browser = await chromium.launch({ headless: true });
    try {
      const page = await browser.newPage();
      page.setDefaultTimeout(5_000);
      const errors: string[] = [];
      const unexpectedRequests: string[] = [];
      page.on("pageerror", (error) => errors.push(error.message));
      page.on("request", (request) => {
        if (!request.url().startsWith(`http://127.0.0.1:${port}/`))
          unexpectedRequests.push(request.url());
      });
      await page.goto(`http://127.0.0.1:${port}/`);
      await page.waitForFunction(() => "pluginRenderer" in window);
      const identity = await page.evaluate(() => {
        const renderer = (window as unknown as { pluginRenderer: PluginRenderer }).pluginRenderer;
        const requests: string[] = [];
        const host = {
          execute: (command: {
            command: string;
            id: string;
            version: number;
            payload: { method: string; pluginId: string };
          }): Promise<unknown> => {
            if (
              command.command !== "plugin.execute" ||
              command.payload.pluginId !== "streamskope.nsp"
            )
              throw new Error("Unexpected core command.");
            requests.push(command.payload.method);
            return Promise.resolve({
              command: command.command,
              id: command.id,
              version: command.version,
              ok: true,
              result: {
                correlationId: command.id,
                output: { ok: true, status: { state: "idle" } },
              },
            });
          },
          subscribe: (): (() => void) => () => {},
          openExternalUrl: (): Promise<void> => Promise.resolve(),
        } as unknown as StreamSkopeHost;
        const element = document.getElementById("plugin");
        if (!element) throw new Error("Missing plugin mount point.");
        const mounted = renderer.mount(element, {
          view: "connection",
          actionId: "capture",
          profiles: [],
          host,
          onClose() {},
          onProfileReady() {},
          onExistingDestination() {},
        });
        Object.assign(window, {
          pluginRequests: requests,
          disposePlugin: (): void => mounted.dispose(),
        });
        return { id: renderer.id, action: renderer.connectionActions[0]?.label };
      });
      expect(identity).toEqual({ id: "streamskope.nsp", action: "Connect to NSP" });
      const dialog = page.getByRole("dialog", { name: "Connect to NSP" });
      await dialog.waitFor();
      expect(await dialog.getByLabel("NSP API URL").isVisible()).toBe(true);
      expect(await dialog.getByLabel("NSP username").isVisible()).toBe(true);
      expect(await dialog.getByLabel("NSP password").isVisible()).toBe(true);
      expect(
        await dialog.getByRole("checkbox", { name: "Verify NSP API certificate" }).isChecked(),
      ).toBe(true);
      expect(
        await dialog.getByRole("button", { name: "Create connection profile" }).isVisible(),
      ).toBe(true);
      await page.waitForFunction(
        () => (window as unknown as { pluginRequests: string[] }).pluginRequests.length === 1,
      );
      expect(
        await page.evaluate(
          () => (window as unknown as { pluginRequests: string[] }).pluginRequests,
        ),
      ).toEqual(["nspCapture.status"]);
      await page.evaluate(() => (window as unknown as { disposePlugin(): void }).disposePlugin());
      await dialog.waitFor({ state: "detached" });
      expect(errors).toEqual([]);
      expect(unexpectedRequests).toEqual([]);
    } finally {
      await browser.close();
    }
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
}, 30_000);

it("installs and removes the verified package through Preferences across application restarts", async () => {
  try {
    await execute(
      process.execPath,
      ["tools/package/e2e.mjs", "web", "test/e2e/web-plugin-installation.spec.ts"],
      {
        env: { ...process.env, STREAMSKOPE_PLUGIN_PACKAGE_READY: "1" },
        timeout: 120_000,
        maxBuffer: 2 * 1024 * 1024,
      },
    );
  } catch (error) {
    const failure = error !== null && typeof error === "object" ? error : {};
    const stdout = "stdout" in failure && typeof failure.stdout === "string" ? failure.stdout : "";
    const stderr = "stderr" in failure && typeof failure.stderr === "string" ? failure.stderr : "";
    throw new Error(
      `Plugin lifecycle browser qualification failed. Child output (bounded tails):\n${stdout.slice(-12_000)}\n${stderr.slice(-12_000)}`,
      { cause: error },
    );
  }
}, 125_000);

it("hot-updates a second local build in the same store without losing recovery state", async () => {
  const catalog = new DevelopmentPluginCatalog("dist/plugin-package");
  const store = new PluginStore(join(directory, "development-update"));
  const runtime = new PluginRuntime({
    store,
    catalog,
  });
  runtime.bindHost({
    execute: () => Promise.reject(new Error("Update must not execute core commands")),
    connectionActive: () => false,
    profiles: () => Promise.resolve([]),
    deleteProfile: () => Promise.reject(new Error("Update must not delete profiles")),
    disconnectPluginConnection: () => Promise.resolve(),
    recordActivity: () => undefined,
    failure: (error) => {
      throw error;
    },
  });
  try {
    const original = (await runtime.install(plugin.manifest.id)).plugins[0]!;
    const recovery = { fixture: "retained ownership" };
    await store.writeRecoveryState(plugin.manifest.id, recovery);
    await execute(process.execPath, ["--import", "tsx", "tools/package.ts", "plugin"], {
      timeout: 90_000,
      maxBuffer: 2 * 1024 * 1024,
    });
    const refreshed = (await catalog.list()).find(
      (entry) => entry.manifest.id === plugin.manifest.id,
    )!;
    const { bytes } = await catalog.download(plugin.manifest.id);
    const rebuilt = parsePluginPackage(bytes);
    expect(refreshed.manifest).toEqual(rebuilt.manifest);
    expect(refreshed.sha256).toBe(rebuilt.sha256);
    expect(
      compareSemanticVersions(rebuilt.manifest.version, original.active!.version),
    ).toBeGreaterThan(0);
    const updated = (await runtime.install(plugin.manifest.id)).plugins[0]!;
    expect(updated.active?.version).toBe(rebuilt.manifest.version);
    expect(updated.activationId).not.toBe(original.activationId);
    expect(updated.previous?.version).toBe(original.active?.version);
    expect(updated.restartRequired).toBe(false);
    expect(await store.readRecoveryState(plugin.manifest.id)).toEqual(recovery);
    expect(JSON.parse(await readFile("plugins/eda/manifest.json", "utf8"))).toHaveProperty(
      "version",
      "0.0.0-dev",
    );
  } finally {
    await runtime.close();
  }
}, 95_000);
