import { createHash, randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

import { headers } from "@nats-io/transport-node";
import {
  _electron as electron,
  expect,
  test,
  type ElectronApplication,
  type Locator,
  type Page,
} from "@playwright/test";
import { build } from "vite";

import {
  NATS_PROTOCOL_VERSION,
  parseCorrelatedNatsResponse,
  type NatsCommandResultMap,
  type NatsEvent,
  type NatsHost,
  type NatsRecord,
} from "../../src/features/nats/contracts";
import { buildRenderer } from "../support/electron-application";
import { startNatsFixture, type NatsFixture } from "../support/nats-fixture";
import {
  protectedStorageSessionAvailable,
  startProtectedStorageSession,
} from "../support/protected-storage-session";
import { findSensitiveArtifactPaths } from "../support/sensitive-artifacts";

interface NativeNatsWindow {
  streamSkopeProviders: { nats: NatsHost };
  natsNativeEvidence: { events: NatsEvent[]; leaked: boolean };
}

function privateSentinels(fixture: NatsFixture): readonly string[] {
  return [fixture.token, fixture.caPem, JSON.stringify(fixture.caPem).slice(1, -1)];
}

// Profiles contain real generated credentials. Automatic failure evidence must not capture editors.
test.use({ screenshot: "off", trace: "off", video: "off" });
test.beforeAll((): void => {
  process.env.PLAYWRIGHT_NO_COPY_PROMPT = "1";
});
const artifactPolicies: { outputDir: string; sensitiveValues: readonly string[] }[] = [];
test.afterAll(async (): Promise<void> => {
  for (const policy of artifactPolicies) {
    const paths = await findSensitiveArtifactPaths([policy.outputDir], policy.sensitiveValues);
    if (paths.length > 0)
      throw new Error("Native NATS artifacts contained private profile material.");
  }
});

async function selectNats(page: Page, fixture: NatsFixture): Promise<void> {
  await expect(page.getByRole("combobox", { name: "Messaging provider" })).toBeVisible({
    timeout: 20_000,
  });
  await page.getByRole("combobox", { name: "Messaging provider" }).click();
  await page.getByRole("option", { name: "Core NATS", exact: true }).click();
  await expect(page.getByRole("combobox", { name: "Messaging provider" })).toContainText(
    "Core NATS",
  );
  await page.evaluate(
    (secrets): void => {
      const target = window as unknown as NativeNatsWindow;
      target.natsNativeEvidence = { events: [], leaked: false };
      target.streamSkopeProviders.nats.subscribe((event): void => {
        if (secrets.some((secret) => JSON.stringify(event).includes(secret))) {
          target.natsNativeEvidence.leaked = true;
        } else target.natsNativeEvidence.events.push(event);
      });
    },
    [...privateSentinels(fixture)],
  );
}

async function resource(
  page: Page,
  name: "Connection Profiles" | "Live Subscription",
): Promise<void> {
  await page
    .getByRole("navigation", { name: "StreamSkope resources" })
    .getByRole("button", { name, exact: true })
    .click();
  await expect(page.getByRole("main", { name })).toBeVisible();
}

async function snapshot(
  page: Page,
  fixture: NatsFixture,
): Promise<NatsCommandResultMap["profiles.list"]> {
  const command = {
    command: "profiles.list" as const,
    id: randomUUID(),
    version: NATS_PROTOCOL_VERSION,
    payload: {},
  };
  const response: unknown = JSON.parse(
    await page.evaluate(async (json: string): Promise<string> => {
      const target = window as unknown as NativeNatsWindow;
      return JSON.stringify(
        await target.streamSkopeProviders.nats.execute(JSON.parse(json) as typeof command),
      );
    }, JSON.stringify(command)),
  );
  if (privateSentinels(fixture).some((secret) => JSON.stringify(response).includes(secret)))
    throw new Error("The native NATS response exposed private profile material.");
  const parsed = parseCorrelatedNatsResponse(response, command);
  if (!parsed.ok) throw new Error("The native NATS snapshot request failed.");
  return parsed.result;
}

async function records(page: Page): Promise<readonly NatsRecord[]> {
  return page.evaluate(() => {
    const evidence = (window as unknown as NativeNatsWindow).natsNativeEvidence;
    if (evidence.leaked) throw new Error("Native NATS events exposed private profile material.");
    return evidence.events.flatMap((event) =>
      event.event === "records.batch" ? event.payload.records : [],
    );
  });
}

async function privateField(field: Locator, value: string): Promise<void> {
  try {
    await field.fill(value);
  } catch {
    throw new Error("The native NATS private profile field could not be filled.");
  }
}

test("restores a genuine protected Core NATS profile across restart and confirms real TLS capture cleanup", async ({
  browserName: _browserName,
}, info) => {
  test.setTimeout(240_000);
  test.skip(
    !protectedStorageSessionAvailable,
    "A genuine operating-system credential service is required.",
  );
  test.skip(
    process.platform !== "linux",
    "The owned NATS fixture currently requires Linux Docker.",
  );
  await mkdir(resolve("dist"), { recursive: true });
  const directory = await mkdtemp(join(resolve("dist"), "electron-nats-live-"));
  const output = join(directory, "electron");
  const userData = join(directory, "user-data");
  const profileFile = join(userData, "profiles", "nats-profiles.json");
  let fixture: NatsFixture | undefined;
  let storage: Awaited<ReturnType<typeof startProtectedStorageSession>> | undefined;
  let application: ElectronApplication | undefined;
  const checks: string[] = [];
  const diagnostics: string[] = [];
  const hostOutput: string[] = [];
  let phase = "production bundle";
  let failureDetail: string | undefined;
  let failed = false;
  let cleanupFailed = false;
  try {
    // Bundle the actual product entry, not the Kafka-only smoke host or deterministic protector.
    for (const entry of [
      { input: "src/platform/electron/main/electron-entry.ts", name: "main", emptyOutDir: true },
      { input: "src/platform/electron/preload/index.ts", name: "preload", emptyOutDir: false },
    ]) {
      await build({
        configFile: false,
        root: process.cwd(),
        logLevel: "silent",
        build: {
          ssr: true,
          sourcemap: false,
          minify: false,
          outDir: output,
          emptyOutDir: entry.emptyOutDir,
          rollupOptions: {
            input: resolve(entry.input),
            external: ["electron"],
            output: { codeSplitting: false, entryFileNames: `${entry.name}.cjs`, format: "cjs" },
          },
        },
        ssr: {
          external: ["@platformatic/kafka", "jks-js", "node-forge", "ssh2"],
          noExternal: true,
        },
      });
    }
    await buildRenderer(directory);
    await writeFile(
      join(output, "launch.cjs"),
      'const { app } = require("electron");\napp.setPath("userData", process.env.STREAMSKOPE_NATS_TEST_USER_DATA);\nrequire("./main.cjs");\n',
    );
    phase = "owned TLS server and genuine keyring";
    fixture = await startNatsFixture({ network: "host-loopback" });
    artifactPolicies.push({
      outputDir: info.outputDir,
      sensitiveValues: privateSentinels(fixture),
    });
    storage = await startProtectedStorageSession(directory);
    const environment = Object.fromEntries(
      Object.entries(process.env).filter(
        (entry): entry is [string, string] => entry[1] !== undefined,
      ),
    );
    Object.assign(environment, storage.environment, { STREAMSKOPE_NATS_TEST_USER_DATA: userData });
    delete environment.ELECTRON_RUN_AS_NODE;
    delete environment.STREAMSKOPE_RENDERER_URL;
    const require = createRequire(resolve("package.json"));
    const launch = async (): Promise<Page> => {
      phase = "native process launch";
      application = await electron.launch({
        executablePath: require("electron") as string,
        args: [
          ...(process.getuid?.() === 0 ? ["--no-sandbox"] : []),
          ...storage!.electronArguments,
          join(output, "launch.cjs"),
        ],
        env: environment,
      });
      application.process().stderr?.on("data", (bytes: Buffer): void => {
        if (hostOutput.join("").length < 16_384) hostOutput.push(bytes.toString("utf8"));
      });
      phase = "native first window";
      const page = await application.firstWindow();
      await page.setViewportSize({ width: 1440, height: 900 });
      page.on("pageerror", (error) => diagnostics.push(error.message));
      phase = "native security and keyring capability";
      const security = await application.evaluate(({ BrowserWindow, safeStorage }) => {
        const contents = BrowserWindow.getAllWindows()[0]!.webContents as unknown as {
          getLastWebPreferences(): {
            contextIsolation: boolean;
            nodeIntegration: boolean;
            sandbox: boolean;
          };
        };
        const preferences = contents.getLastWebPreferences();
        return {
          contextIsolation: preferences.contextIsolation,
          nodeIntegration: preferences.nodeIntegration,
          sandbox: preferences.sandbox,
          storage: safeStorage.getSelectedStorageBackend(),
        };
      });
      expect(security).toMatchObject({
        contextIsolation: true,
        nodeIntegration: false,
        sandbox: true,
        storage: "gnome_libsecret",
      });
      phase = "native Core NATS provider selection";
      await selectNats(page, fixture!);
      return page;
    };
    phase = "protected native profile creation";
    let page = await launch();
    const originalPid = application!.process().pid;
    phase = "native NATS resource navigation";
    await resource(page, "Connection Profiles");
    phase = "native NATS profile editor opening";
    await page.getByRole("button", { name: "Add NATS profile" }).click();
    const editor = page.getByRole("dialog", { name: "Create NATS profile" });
    const name = "Native verified NATS";
    phase = "native NATS public profile fields";
    await editor.getByRole("textbox", { name: "Profile name", exact: true }).fill(name);
    await editor.getByRole("textbox", { name: "NATS servers", exact: true }).fill(fixture.server);
    await editor.getByRole("combobox", { name: "Authentication", exact: true }).click();
    await page.getByRole("option", { name: "Token", exact: true }).click();
    phase = "native NATS private token field";
    await privateField(editor.getByLabel("Token", { exact: true }), fixture.token);
    phase = "native NATS verified TLS selection";
    await editor.getByRole("radio", { name: "Verified TLS", exact: true }).check();
    phase = "native NATS private CA field";
    await privateField(
      editor.getByRole("textbox", { name: "CA certificate PEM", exact: true }),
      fixture.caPem,
    );
    phase = "native NATS profile save";
    await editor.getByRole("button", { name: "Save profile", exact: true }).click();
    await expect(editor).toHaveCount(0);
    phase = "native NATS saved profile snapshot";
    const saved = await snapshot(page, fixture);
    expect(saved.profiles.capability).toMatchObject({
      durability: "durable",
      protection: "os-protected",
      state: "ready",
    });
    expect(saved.profiles.profiles).toHaveLength(1);
    const original = saved.profiles.profiles[0]!;
    expect(original.authentication).toEqual({ mode: "token", tokenPresent: true });
    expect(original.tls).toEqual({ mode: "tls", caPresent: true });
    phase = "native NATS encrypted profile file";
    const protectedBytes = await readFile(profileFile, "utf8");
    expect(privateSentinels(fixture).some((secret) => protectedBytes.includes(secret))).toBe(false);
    checks.push(
      "production-provider-and-preload",
      "genuine-gnome-protection",
      "protected-profile-created",
      "no-plaintext-secrets-in-profile-file",
    );

    phase = "restart restoration with retained credential service";
    await application!.close();
    application = undefined;
    page = await launch();
    expect(application!.process().pid).not.toBe(originalPid);
    await resource(page, "Connection Profiles");
    await expect(
      page.getByRole("button", { name: `Connect profile ${name}`, exact: true }),
    ).toBeVisible();
    const restored = await snapshot(page, fixture);
    expect(restored.profiles.profiles).toEqual([original]);
    await page.getByRole("button", { name: `Edit profile ${name}`, exact: true }).click();
    const restoredEditor = page.getByRole("dialog", { name: "Edit NATS profile" });
    for (const field of [
      restoredEditor.getByLabel("Token", { exact: true }),
      restoredEditor.getByRole("textbox", { name: "CA certificate PEM", exact: true }),
    ]) {
      expect(
        await field.evaluate(
          (element) =>
            (element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement) &&
            element.value === "",
        ),
      ).toBe(true);
    }
    await restoredEditor.getByRole("button", { name: "Cancel", exact: true }).click();
    checks.push("same-account-restart-restores-profile", "restored-editor-does-not-return-secrets");

    phase = "real token and verified TLS subscription";
    await page.getByRole("button", { name: `Connect profile ${name}`, exact: true }).click();
    await expect(page.getByLabel("Connection status", { exact: true })).toContainText("Connected");
    await resource(page, "Live Subscription");
    await page
      .getByRole("textbox", { name: "Subject filter", exact: true })
      .fill("qualification.*");
    await page.getByRole("button", { name: "Start subscription", exact: true }).click();
    await expect(page.getByLabel("Subscription status", { exact: true })).toContainText(
      "Streaming",
    );
    const started = await snapshot(page, fixture);
    expect(started.subscription.state).toBe("streaming");
    const publisher = await fixture.publisher();
    const originalJson = ' {\n "message": "native restart proof", "count": 1\n}\n';
    const metadata = headers();
    metadata.append("Trace", "one");
    metadata.append("Trace", "two");
    publisher.publish("qualification.native-json", originalJson);
    publisher.publish("qualification.native-binary", Uint8Array.from([0, 255, 128, 1]), {
      reply: "reply.native",
      headers: metadata,
    });
    await publisher.flush();
    await expect.poll(async () => (await records(page)).length, { timeout: 10_000 }).toBe(2);
    expect(await records(page)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          subject: "qualification.native-json",
          generation: started.subscription.generation,
          payload: { encoding: "utf8", data: originalJson },
          payloadBytes: Buffer.byteLength(originalJson),
        }),
        expect.objectContaining({
          subject: "qualification.native-binary",
          reply: "reply.native",
          payload: { encoding: "base64", data: "AP+AAQ==" },
          payloadBytes: 4,
          headers: [{ name: "Trace", values: ["one", "two"] }],
        }),
      ]),
    );
    await page
      .getByRole("grid", { name: "NATS records" })
      .getByRole("gridcell", { name: "qualification.native-json", exact: true })
      .click();
    expect(
      await page
        .getByRole("complementary", { name: "Record inspector" })
        .getByLabel("Original payload", { exact: true })
        .textContent(),
    ).toBe(originalJson);
    await page.getByRole("button", { name: "Close record inspector", exact: true }).click();
    checks.push(
      "restored-token-and-ca-connect-without-reentry",
      "real-original-record-fidelity",
      "native-inspector-original-payload",
    );

    phase = "confirmed stop and disconnect";
    await page.getByRole("button", { name: "Stop subscription", exact: true }).click();
    await expect(page.getByLabel("Subscription status", { exact: true })).toContainText("Stopped");
    expect((await snapshot(page, fixture)).subscription.state).toBe("stopped");
    publisher.publish("qualification.stopped-sentinel", "must-not-arrive-after-native-stop");
    await publisher.flush();
    await delay(350);
    expect((await records(page)).length).toBe(2);
    await page.getByRole("button", { name: "Disconnect", exact: true }).click();
    await expect(page.getByLabel("Connection status", { exact: true })).toContainText(
      "Disconnected",
    );
    const final = await snapshot(page, fixture);
    expect(final.connection.state).toBe("disconnected");
    expect(final.subscription.state).toBe("stopped");
    expect(final.profiles.profiles).toEqual([original]);
    expect(
      privateSentinels(fixture).some((secret) => diagnostics.join("\n").includes(secret)),
    ).toBe(false);
    expect(diagnostics).toEqual([]);
    checks.push(
      "confirmed-stop-prevents-later-receipt",
      "disconnect-retains-protected-profile",
      "no-private-material-in-responses-or-events",
      "no-renderer-errors",
    );
    await info.attach("nats-native-live-evidence", {
      contentType: "application/json",
      body: JSON.stringify(
        {
          outcome: "passed",
          checks,
          platform: process.platform,
          arch: process.arch,
          originalRecordSha256: createHash("sha256").update(originalJson).digest("hex"),
          scope:
            "unmodified production Electron entry and preload; genuine isolated keyring retained across two process lifetimes; owned NATS token/TLS server; source bundles, not installed packages",
        },
        null,
        2,
      ),
    });
  } catch (error) {
    failed = true;
    failureDetail = sanitize(
      error instanceof Error ? error.message : "Native NATS scenario failed.",
    );
  } finally {
    // Close the native host before its server and keep the real keyring alive until both app lifetimes end.
    try {
      await application?.close();
    } catch {
      cleanupFailed = true;
    }
    const cleanup = await Promise.allSettled([
      fixture?.dispose() ?? Promise.resolve(),
      storage?.dispose() ?? Promise.resolve(),
      rm(directory, { recursive: true, force: true }),
    ]);
    cleanupFailed ||= cleanup.some((result) => result.status === "rejected");
    if (failed || cleanupFailed)
      await info.attach("nats-native-live-failure", {
        contentType: "application/json",
        body: JSON.stringify({
          outcome: "failed",
          phase,
          checks,
          cleanupFailed,
          ...(failureDetail === undefined ? {} : { detail: failureDetail }),
          hostOutput: sanitize(hostOutput.join("\n")),
        }),
      });
  }
  if (failed || cleanupFailed)
    throw new Error(
      `Native NATS qualification failed during ${phase}; inspect sanitized evidence.`,
    );

  function sanitize(value: string): string {
    const privateValues = [
      directory,
      userData,
      ...(fixture === undefined ? [] : privateSentinels(fixture)),
      fixture?.server,
      fixture?.ipServer,
    ];
    for (const privateValue of privateValues)
      if (privateValue) value = value.split(privateValue).join("[redacted]");
    return value
      .replace(
        /-----BEGIN CERTIFICATE-----[\s\S]*?-----END CERTIFICATE-----/gu,
        "[certificate redacted]",
      )
      .replace(/\/(?:Users|home|tmp)\/[^\s"'<>]+/gu, "[private path]")
      .slice(0, 4096);
  }
});
