import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type HostEvent,
  type HostEventListener,
  type StreamSkopeBackend,
} from "../../src/features/kafka/contracts";
import { launchWebDevelopment, type RunningWebDevelopment } from "../../src/platform/dev-host";
import { expectWorkbenchReady } from "../support/workbench-browser";
import { testHostResponse, testHostAccepted } from "../support/host-response";
import { edaPluginManifest, testHostResponse as edaResponse } from "../support/eda-ui-host";
import {
  EDA_PLUGIN_ID,
  EDA_PROTOCOL_VERSION,
  parseEdaCaptureCommand,
  type EdaCaptureCommand,
  type EdaCaptureCommandResponse,
} from "../../plugins/eda/contracts";
import { parsePluginJson } from "../../src/plugins/validation";
import type { PluginRendererAsset } from "../../src/platform/node/plugins/runtime";

class PlaintextProfileBackend implements StreamSkopeBackend {
  readonly commands: HostCommand[] = [];
  private readonly listeners = new Set<HostEventListener>();
  private sequence = 0;

  emit(event: Omit<HostEvent, "sequence" | "version">): void {
    const sequenced = {
      ...event,
      sequence: this.sequence++,
      version: HOST_PROTOCOL_VERSION,
    } as HostEvent;
    for (const listener of this.listeners) listener(sequenced);
  }

  execute<Command extends HostCommand>(
    command: Command,
  ): Promise<HostCommandResponse<Command["command"]>>;
  execute(command: HostCommand): Promise<HostCommandResponse> {
    this.commands.push(command);
    if (command.command === "plugins.list")
      return Promise.resolve(
        testHostResponse(command, {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: {
            correlationId: command.id,
            pluginSnapshot: {
              plugins: [
                {
                  id: EDA_PLUGIN_ID,
                  installed: edaPluginManifest,
                  active: edaPluginManifest,
                  pending: null,
                  restartRequired: false,
                  rendererUrl: `/plugins/${EDA_PLUGIN_ID}/fixture/renderer.js`,
                },
              ],
            },
          },
        }),
      );
    if (command.command === "plugin.execute")
      return this.executeEda(
        parseEdaCaptureCommand({
          command: command.payload.method,
          id: command.id,
          payload: command.payload.input,
          version: EDA_PROTOCOL_VERSION,
        }),
      ).then((output) =>
        testHostResponse(command, {
          command: command.command,
          id: command.id,
          version: HOST_PROTOCOL_VERSION,
          ok: true,
          result: { correlationId: command.id, output: parsePluginJson(output) },
        }),
      );
    return Promise.resolve(testHostAccepted(command, `plaintext-${command.id}`));
  }

  async pluginAsset(pathname: string): Promise<PluginRendererAsset | undefined> {
    if (pathname !== `/plugins/${EDA_PLUGIN_ID}/fixture/renderer.js`) return undefined;
    return {
      content: await readFile(resolve("dist/plugins/eda/renderer.js")),
      contentType: "text/javascript; charset=utf-8",
    };
  }

  private executeEda(command: EdaCaptureCommand): Promise<EdaCaptureCommandResponse> {
    const base = { command: command.command, id: command.id, version: EDA_PROTOCOL_VERSION };
    if (command.command === "edaCapture.preflight")
      return Promise.resolve(
        edaResponse(command, {
          ...base,
          ok: true,
          result: {
            correlationId: command.id,
            captureHost: {
              state: "configured",
              context: "explicit-capture-host",
              edaApiUrl: "https://eda.example.test",
              detail: "Kubernetes credentials are explicitly configured on the capture host.",
            },
          },
        }),
      );
    if (command.command === "edaCapture.status")
      return Promise.resolve(
        edaResponse(command, {
          ...base,
          ok: true,
          result: {
            correlationId: command.id,
            captureSession: { state: "idle", tunnel: "closed", detail: "No capture running." },
          },
        }),
      );
    if (command.command === "edaCapture.inspect")
      return Promise.resolve(
        edaResponse(command, {
          ...base,
          ok: true,
          result: {
            correlationId: command.id,
            inspection: {
              context: "explicit-capture-host",
              contexts: ["explicit-capture-host"],
              namespace: "eda-system",
              imageSetup: { state: "unconfigured" },
              sources: [
                {
                  apiVersion: "kafka.eda.nokia.com/v1",
                  kind: "Producer",
                  namespace: "eda-system",
                  name: "interfaces",
                  topics: ["interfaces"],
                  brokers: ["existing-kafka.example.test:9093"],
                },
              ],
            },
          },
        }),
      );
    throw new Error(`Unexpected EDA command ${command.command}`);
  }

  /** This event-only fixture owns no broker reader. */
  stopStream(): Promise<void> {
    return Promise.resolve();
  }

  shutdown(): Promise<void> {
    return Promise.resolve();
  }

  subscribe(listener: HostEventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
}

async function reservePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolveListen();
    });
  });
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("Expected a temporary TCP endpoint.");
  }
  const port = address.port;
  await new Promise<void>((resolveClose, reject) => {
    server.close((error) => (error === undefined ? resolveClose() : reject(error)));
  });
  return port;
}

const backend = new PlaintextProfileBackend();
let launch: RunningWebDevelopment | undefined;

test.describe("plaintext Kafka profile", () => {
  test.describe.configure({ mode: "serial", timeout: 60_000 });

  test.beforeAll(async () => {
    launch = await launchWebDevelopment({
      backend,
      hostPort: await reservePort(),
      rendererPort: await reservePort(),
      rendererRoot: resolve(process.cwd()),
    });
  });

  test.afterAll(async () => {
    await launch?.close();
  });

  for (const colorScheme of ["light", "dark"] as const) {
    test(`reviews explicit capture changes by keyboard in ${colorScheme} mode`, async ({
      page,
    }, info) => {
      if (launch === undefined) throw new Error("Web development launch is not ready.");
      await page.setViewportSize({ height: 650, width: 1000 });
      await page.emulateMedia({ colorScheme, reducedMotion: "reduce" });
      await page.goto(launch.browserUrl);
      await expectWorkbenchReady(page);
      backend.emit({
        event: "profiles.changed",
        payload: {
          profiles: [],
          store: { state: "ready", durability: "session", protection: "memory" },
        },
      });
      const before = backend.commands.length;
      await page.getByRole("button", { name: "Add connection" }).click();
      await page.getByRole("menuitem", { name: "Capture from EDA" }).focus();
      await page.keyboard.press("Enter");
      const dialog = page.getByRole("dialog", { name: "Capture Nokia EDA streams" });
      await expect(dialog.getByText("Capture deployment setup")).toHaveCount(0);
      await expect(dialog.getByLabel("EDA API URL")).toHaveValue("");
      await dialog.getByLabel("EDA API URL").fill("https://eda.example.test");
      await dialog.getByLabel("EDA username").fill("review-user");
      await dialog.getByLabel("EDA password").fill("fixture-password");
      await dialog.getByRole("button", { name: "Discover sources" }).focus();
      await page.keyboard.press("Enter");
      await expect(dialog.getByRole("button", { name: "Connect to existing Kafka" })).toBeEnabled();
      await page.screenshot({
        path: info.outputPath(`eda-destination-${colorScheme}.png`),
        animations: "disabled",
      });
      await dialog.getByRole("button", { name: "Set up temporary capture" }).click();
      await expect(dialog.getByText(/Starting capture creates a temporary broker/u)).toBeVisible();
      await expect(dialog.getByRole("button", { name: "Start capture" })).toBeVisible();
      expect(
        backend.commands
          .slice(before)
          .some(
            (command) =>
              command.command === "plugin.execute" &&
              command.payload.method === "edaCapture.deploy",
          ),
      ).toBe(false);
      await expect(dialog).not.toContainText("kubeconfig");
      const accessibility = await new AxeBuilder({ page }).include('[role="dialog"]').analyze();
      expect(accessibility.violations).toEqual([]);
      await page.screenshot({
        path: info.outputPath(`capture-review-${colorScheme}.png`),
        animations: "disabled",
      });
      await page.keyboard.press("Escape");
      await expect(dialog).toHaveCount(0);
    });
  }

  test("opens a normal Kafka profile from an existing EDA destination without deployment", async ({
    page,
  }) => {
    if (launch === undefined) throw new Error("Web development launch is not ready.");
    await page.goto(launch.browserUrl);
    await expectWorkbenchReady(page);
    backend.emit({
      event: "profiles.changed",
      payload: {
        profiles: [],
        store: { state: "ready", durability: "session", protection: "memory" },
      },
    });
    const before = backend.commands.length;
    await page.getByRole("button", { name: "Add connection" }).click();
    await page.getByRole("menuitem", { name: "Capture from EDA" }).click();
    const dialog = page.getByRole("dialog", { name: "Capture Nokia EDA streams" });
    await dialog.getByLabel("EDA API URL").fill("https://eda.example.test");
    await dialog.getByLabel("EDA username").fill("fixture-user");
    await dialog.getByLabel("EDA password").fill("fixture-password");
    await dialog.getByRole("button", { name: "Discover sources" }).click();
    await dialog.getByRole("button", { name: "Connect to existing Kafka" }).click();
    await expect(page.getByLabel("Profile name", { exact: false })).toHaveValue("EDA · interfaces");
    await expect(page.getByLabel("Bootstrap brokers", { exact: false })).toHaveValue(
      "existing-kafka.example.test:9093",
    );
    expect(
      backend.commands
        .slice(before)
        .some(
          (command) =>
            (command.command === "plugin.execute" &&
              command.payload.method === "edaCapture.deploy") ||
            command.command === "profiles.create",
        ),
    ).toBe(false);
  });

  test("selects deliberate plaintext by keyboard and submits no trust", async ({ page }) => {
    if (launch === undefined) throw new Error("Web development launch is not ready.");
    await page.setViewportSize({ height: 720, width: 1_080 });
    await page.emulateMedia({ colorScheme: "light", reducedMotion: "reduce" });
    await page.goto(launch.browserUrl);
    await expectWorkbenchReady(page);
    backend.emit({
      event: "profiles.changed",
      payload: {
        profiles: [],
        store: { durability: "session", protection: "memory", state: "ready" },
      },
    });

    const addProfile = page.getByRole("button", { name: "Add connection" });
    await addProfile.focus();
    await page.keyboard.press("Enter");
    await page.getByRole("menuitem", { name: "Existing Kafka cluster" }).focus();
    await page.keyboard.press("Enter");
    const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
    const tls = editor.getByRole("radio", { name: "TLS" });
    const plaintext = editor.getByRole("radio", { name: "Plaintext (insecure)" });
    await expect(tls).toBeChecked();
    await expect(editor.getByText(/Plaintext is insecure/u)).toHaveCount(0);

    await tls.focus();
    await page.keyboard.press("ArrowRight");
    await expect(plaintext).toBeChecked();
    await expect(editor.getByText(/Plaintext is insecure/u)).toBeVisible();
    await expect(editor.getByRole("heading", { name: "TLS trust" })).toHaveCount(0);
    await editor.getByRole("textbox", { name: "Profile name" }).fill("Browser plaintext");
    await editor.getByRole("textbox", { name: "Bootstrap brokers" }).fill("127.0.0.1:19092");
    const testConnection = editor.getByRole("button", { name: "Test connection" });
    await testConnection.focus();
    await page.keyboard.press("Enter");

    await expect
      .poll(() => {
        const command = [...backend.commands]
          .reverse()
          .find((candidate) => candidate.command === "profiles.test");
        return command?.command === "profiles.test" ? command.payload : undefined;
      })
      .toEqual({
        mode: "create",
        profile: {
          brokers: ["127.0.0.1:19092"],
          name: "Browser plaintext",
          transport: "plaintext",
        },
      });
    const command = [...backend.commands]
      .reverse()
      .find((candidate) => candidate.command === "profiles.test");
    expect(JSON.stringify(command)).not.toMatch(/trust|source|capture/i);
    expect(
      (await new AxeBuilder({ page }).include('[role="dialog"]').analyze()).violations,
    ).toEqual([]);

    const cancel = editor.getByRole("button", { name: "Cancel" });
    await cancel.focus();
    await page.keyboard.press("Enter");
    await expect(editor).toBeHidden();
    await expect(addProfile).toBeFocused();
  });
});
