import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { promisify } from "node:util";

import { expect, test, type Page } from "@playwright/test";

import { parseRecovery } from "../../plugins/nsp/backend/recovery";
import {
  fromPluginProfileSource,
  NSP_PLUGIN_ID,
  parseNspConnectInput,
  parseNspResult,
  type NspConnectInput,
} from "../../plugins/nsp/contracts";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type ProfileSummary,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import { PluginStore } from "../../src/platform/node/plugins/store";
import {
  electronPluginStorageAvailable,
  startElectronPluginFixture,
} from "../support/electron-plugin";
import {
  liveNspApiClient,
  liveNspFixtureAdmin,
  ownedNspTopic,
  removeOwnedNspTopic,
} from "../support/nsp-live-broker";
import { pluginPackageFixtures } from "../support/plugin-package-fixture";
import { openWorkbenchResource } from "../support/workbench-browser";

// Credentials and live message data never enter traces, screenshots or video artifacts.
test.use({ screenshot: "off", trace: "off", video: "off" });
const run = promisify(execFile);
interface LiveWindow {
  streamSkopeHost: StreamSkopeHost;
  nspEvidence: { profiles: readonly ProfileSummary[]; received: boolean };
}
async function execute<C extends HostCommand>(
  page: Page,
  command: C,
): Promise<HostCommandResponse<C["command"]>> {
  // The host protocol is JSON; avoid recursively instantiating Playwright's serializer types.
  const response = await page.evaluate(
    async (input: string): Promise<string> =>
      JSON.stringify(
        await (window as unknown as LiveWindow).streamSkopeHost.execute(
          JSON.parse(input) as HostCommand,
        ),
      ),
    JSON.stringify(command),
  );
  return JSON.parse(response) as HostCommandResponse<C["command"]>;
}
async function profiles(page: Page): Promise<readonly ProfileSummary[]> {
  const response = await execute(page, {
    command: "profiles.list",
    id: randomUUID(),
    version: HOST_PROTOCOL_VERSION,
    payload: {},
  });
  assert(response.ok, "Profile listing failed.");
  return JSON.parse(
    await page.evaluate(() =>
      JSON.stringify((window as unknown as LiveWindow).nspEvidence.profiles),
    ),
  ) as readonly ProfileSummary[];
}
async function installation(
  page: Page,
): Promise<
  | {
      activationId: string | undefined;
      rendererUrl: string | undefined;
      version: string | undefined;
    }
  | undefined
> {
  const response = await execute(page, {
    command: "plugins.list",
    id: randomUUID(),
    version: HOST_PROTOCOL_VERSION,
    payload: {},
  });
  assert(response.ok, "Plugin listing failed.");
  const value = response.result.pluginSnapshot.plugins.find(
    (plugin) => plugin.id === NSP_PLUGIN_ID,
  );
  return value
    ? {
        activationId: value.activationId,
        rendererUrl: value.rendererUrl,
        version: value.active?.version,
      }
    : undefined;
}
async function openPlugins(page: Page): Promise<void> {
  await page.getByRole("button", { name: "Preferences", exact: true }).click();
  await page.getByRole("tab", { name: "Plugins", exact: true }).click();
}
async function closePreferences(page: Page): Promise<void> {
  await page
    .getByRole("dialog", { name: "Workbench Preferences" })
    .getByRole("button", { name: "Close", exact: true })
    .click();
}
async function credentials(page: Page, input: NspConnectInput, refresh = false): Promise<void> {
  const dialog = page.getByRole("dialog", {
    name: refresh ? "Refresh NSP connection" : "Connect to NSP",
    exact: true,
  });
  if (!refresh) await dialog.getByLabel("NSP API URL", { exact: true }).fill(input.apiUrl);
  await dialog.getByLabel("NSP username", { exact: true }).fill(input.username);
  await dialog.getByLabel("NSP password", { exact: true }).fill(input.password);
  await dialog.getByLabel("Verify NSP API certificate").setChecked(input.verifyCertificate);
  if (!refresh && input.brokers !== undefined) {
    await dialog.getByLabel("Override default Kafka brokers").check();
    await dialog
      .getByLabel("Kafka broker endpoints", { exact: true })
      .fill(input.brokers.join(", "));
  }
  if (!refresh && input.authentication !== undefined) {
    await dialog.getByRole("combobox", { name: "Kafka authentication", exact: true }).click();
    const label = { auto: "Detect automatically", tls: "TLS only", oauth: "TLS and NSP OAuth" };
    await page.getByRole("option", { name: label[input.authentication], exact: true }).click();
  }
  await dialog
    .getByRole("button", {
      name: refresh ? "Refresh credentials" : "Create connection profile",
      exact: true,
    })
    .click();
  await expect(dialog).toHaveCount(0, { timeout: 120_000 });
}

test("qualifies installed NSP UI, known record receipt and hot package lifecycle against a live target", async ({
  browserName: _browserName,
}, info) => {
  test.setTimeout(420_000);
  test.skip(!process.env.STREAMSKOPE_NSP_CONFIG, "A local NSP configuration is required.");
  test.skip(!electronPluginStorageAvailable, "Real OS protected storage is required.");
  assert(!process.env.GITHUB_ACTIONS, "Live NSP qualification is local only.");
  const input = parseNspConnectInput(
    JSON.parse(await readFile(process.env.STREAMSKOPE_NSP_CONFIG!, "utf8")),
  );
  if (process.env.STREAMSKOPE_PLUGIN_PACKAGE_READY !== "1")
    await run(process.execPath, ["--import", "tsx", "tools/package/plugin.ts", "nsp"], {
      maxBuffer: 4 * 1_048_576,
    });
  const client = await liveNspApiClient(input);
  const targetVersion = await client.readVersion().finally(() => client.close());
  const { current, update } = await pluginPackageFixtures("nsp");
  const fixture = await startElectronPluginFixture(current.bytes, info, [
    input.password,
    input.apiUrl,
    input.username,
  ]);
  const { page, application } = fixture;
  page.setDefaultTimeout(30_000);
  const marker = `streamskope-native-qualification:${randomUUID()}`;
  const topic = ownedNspTopic();
  let admin: Awaited<ReturnType<typeof liveNspFixtureAdmin>> | undefined;
  let fixtureAttempted = false;
  const checks: string[] = [];
  let passed = false;
  const failures: { phase: string; message: string }[] = [];
  const recordFailure = (phase: string, error: unknown): void => {
    passed = false;
    let message = error instanceof Error ? error.message : "Native NSP qualification failed.";
    for (const value of [input.password, input.apiUrl, input.username])
      if (value) message = message.replaceAll(value, "[redacted]");
    message = message.replace(
      /eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/gu,
      "[redacted token]",
    );
    failures.push({ phase, message: message.slice(0, 1024) });
  };
  const finish = async (phase: string, action: () => Promise<void>): Promise<void> => {
    try {
      await action();
    } catch (error) {
      recordFailure(phase, error);
    }
  };
  try {
    await page.evaluate(
      ({ topic, marker }) => {
        const target = window as unknown as LiveWindow;
        target.nspEvidence = { profiles: [], received: false };
        target.streamSkopeHost.subscribe((event) => {
          if (event.event === "profiles.changed")
            target.nspEvidence.profiles = event.payload.profiles;
          if (event.event === "messages.batch" && event.payload.topic === topic)
            target.nspEvidence.received ||= event.payload.messages.some(
              (message) => message.payload === marker,
            );
        });
      },
      { topic, marker },
    );
    expect(await installation(page)).toBeUndefined();
    await openPlugins(page);
    const card = page.getByRole("region", { name: "NSP Capture", exact: true });
    await card.getByRole("button", { name: "Install", exact: true }).click();
    await expect(card).toContainText(`Active version ${current.manifest.version}`);
    const installed = await installation(page);
    assert(installed?.activationId, "NSP must be activated from its installed package.");
    await closePreferences(page);
    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await page.getByRole("menuitem", { name: "Connect to NSP", exact: true }).click();
    await credentials(page, input);
    const saved = await profiles(page);
    expect(saved).toHaveLength(1);
    const profile = saved[0]!;
    expect(profile.transport).toBe("tls");
    const source = fromPluginProfileSource(profile.source);
    assert(source, "NSP profile ownership must be retained.");
    const stored = await readFile(
      join(fixture.userDataPath, "profiles", "kafka-profiles.json"),
      "utf8",
    );
    assert(stored.includes('"protectedValue"'), "The native profile must use protected storage.");
    assert(!stored.includes(input.password), "The API password must not be stored in plaintext.");
    checks.push("package-ui-install", "real-workflow-profile-dialog", "os-protected-profile");
    await page.getByRole("button", { name: "Refresh NSP credentials", exact: true }).click();
    await credentials(page, input, true);
    expect((await profiles(page)).map((p) => p.id)).toEqual([profile.id]);
    checks.push("native-refresh-reuses-profile");
    await page
      .getByRole("button", { name: `Connect profile ${profile.name}`, exact: true })
      .click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected", {
      timeout: 30_000,
    });
    admin = await liveNspFixtureAdmin(input, source.authentication);
    assert(!(await admin.listTopics()).includes(topic));
    fixtureAttempted = true;
    await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 });
    const review = await execute(page, {
      command: "writes.review",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: {
        kind: "record",
        topic,
        partition: 0,
        record: {
          state: "complete",
          encoding: "base64",
          key: null,
          value: Buffer.from(marker).toString("base64"),
          headers: [],
        },
      },
    });
    assert(review.ok, "Native reviewed record write failed.");
    const written = await execute(page, {
      command: "writes.apply",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: { planId: review.result.review.planId },
    });
    assert(
      written.ok && written.result.outcome.state === "acknowledged",
      "NSP did not acknowledge the native fixture record.",
    );
    await openWorkbenchResource(page, "Topics");
    await page.getByRole("button", { name: "Refresh topics", exact: true }).click();
    await page.getByRole("searchbox", { name: "Search topics", exact: true }).fill(topic);
    await page.getByRole("button", { name: topic, exact: true }).click();
    const stopped = await execute(page, {
      command: "messages.stop",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: {},
    });
    assert(stopped.ok, "Native topic read preparation failed.");
    await page.getByRole("combobox", { name: "Read mode" }).click();
    await page.getByRole("option", { name: "First N", exact: true }).click();
    await page.getByRole("button", { name: `Load messages ${topic}`, exact: true }).click();
    await expect(page.getByText(marker, { exact: true }).first()).toBeVisible({ timeout: 30_000 });
    await expect
      .poll(() => page.evaluate(() => (window as unknown as LiveWindow).nspEvidence.received), {
        timeout: 30_000,
      })
      .toBe(true);
    checks.push("native-ui-known-record-receipt");
    await openWorkbenchResource(page, "Connection Profiles");
    await writeFile(fixture.catalogPath, update.bytes);
    await openPlugins(page);
    await card
      .getByRole("button", { name: `Update to ${update.manifest.version}`, exact: true })
      .click();
    await expect(card).toContainText(`Active version ${update.manifest.version}`);
    const updated = await installation(page);
    expect(updated?.activationId).not.toBe(installed.activationId);
    expect(updated?.rendererUrl).not.toBe(installed.rendererUrl);
    await closePreferences(page);
    await expect(page.getByLabel("Connection status")).toContainText("Disconnected");
    expect((await profiles(page)).map((p) => p.id)).toEqual([profile.id]);
    await page.getByRole("button", { name: "Refresh NSP credentials", exact: true }).click();
    await credentials(page, input, true);
    checks.push("hot-ui-update", "updated-profile-refresh");
    await openPlugins(page);
    await card.getByRole("button", { name: "Remove", exact: true }).click();
    await page.getByRole("button", { name: "Remove plugin", exact: true }).click();
    await expect(card).toContainText("Not installed");
    await closePreferences(page);
    expect(await installation(page)).toBeUndefined();
    expect((await profiles(page)).map((p) => p.id)).toEqual([profile.id]);
    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await expect(page.getByRole("menuitem", { name: "Connect to NSP", exact: true })).toHaveCount(
      0,
    );
    await page.keyboard.press("Escape");
    await openPlugins(page);
    await card.getByRole("button", { name: "Install", exact: true }).click();
    await expect(card).toContainText(`Active version ${update.manifest.version}`);
    await closePreferences(page);
    await page.getByRole("button", { name: "Refresh NSP credentials", exact: true }).click();
    await credentials(page, input, true);
    expect((await profiles(page)).map((p) => p.id)).toEqual([profile.id]);
    await page
      .getByRole("button", { name: `Connect profile ${profile.name}`, exact: true })
      .click();
    await expect(page.getByLabel("Connection status")).toContainText("Connected", {
      timeout: 30_000,
    });
    checks.push("ui-removal-preserves-profile", "ui-reinstall-refresh-and-reconnect");
    expect(await page.evaluate(() => performance.timeOrigin)).toBe(fixture.origin);
    expect(application.process().pid).toBe(fixture.processId);
    expect(application.windows()).toEqual([page]);
    expect(fixture.errors).toEqual([]);
    expect(fixture.assetFailures).toEqual([]);
    expect(
      await new PluginStore(join(fixture.userDataPath, "plugins")).readRecoveryState(NSP_PLUGIN_ID),
    ).toBeNull();
    await removeOwnedNspTopic(admin, topic);
    fixtureAttempted = false;
    checks.push(
      "same-process-and-window",
      "execution-journal-cleared",
      "owned-topic-deletion-confirmed",
    );
    passed = true;
  } catch (error) {
    recordFailure("scenario", error);
  } finally {
    const active = await installation(page).catch(() => undefined);
    if (active?.activationId) {
      try {
        const current = await execute(page, {
          command: "plugin.execute",
          id: randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: {
            pluginId: NSP_PLUGIN_ID,
            activationId: active.activationId,
            method: "nspCapture.status",
            input: {},
          },
        });
        const status = current.ok ? parseNspResult(current.result.output) : undefined;
        if (status?.ok && status.status?.state === "running" && status.status.requestId) {
          await execute(page, {
            command: "plugin.execute",
            id: randomUUID(),
            version: HOST_PROTOCOL_VERSION,
            payload: {
              pluginId: NSP_PLUGIN_ID,
              activationId: active.activationId,
              method: "nspCapture.cancel",
              input: { requestId: status.status.requestId },
            },
          });
        }
      } catch {
        // A lost native host falls back to the persisted identifiers below.
      }
    }
    await page
      .evaluate(() => {
        for (const input of document.querySelectorAll<HTMLInputElement>('input[type="password"]')) {
          input.value = "";
          input.setAttribute("value", "");
          input.dispatchEvent(new Event("input", { bubbles: true }));
        }
      })
      .catch(() => undefined);
    await finish("owned NSP execution cleanup", async () => {
      const store = new PluginStore(join(fixture.userDataPath, "plugins"));
      const pending = parseRecovery(await store.readRecoveryState(NSP_PLUGIN_ID));
      if (pending !== undefined) {
        // Keep identifiers even if the host disappeared before its final cleanup response.
        await writeFile(info.outputPath("nsp-recovery-private.json"), JSON.stringify(pending), {
          mode: 0o600,
        });
        const cleanup = await liveNspApiClient(input);
        try {
          await cleanup.cleanupExecution(pending.requestId, pending.executionId);
          await store.writeRecoveryState(NSP_PLUGIN_ID, null);
        } finally {
          await finish("NSP cleanup token revocation", () => cleanup.close());
        }
      }
    });
    await finish("owned Kafka topic cleanup", async () => {
      if (fixtureAttempted && admin) await removeOwnedNspTopic(admin, topic);
    });
    await finish("fixture Kafka client close", async () => {
      await admin?.close();
    });
    await finish("native host close", () => fixture.close());
    await finish("qualification evidence", async () => {
      await info.attach("nsp-native-live-evidence", {
        body: JSON.stringify(
          {
            outcome: passed ? "passed" : "failed",
            ...(failures.length === 0 ? {} : { failures }),
            checks,
            target: targetVersion,
            hostProtocol: HOST_PROTOCOL_VERSION,
            apiCertificateVerification: input.verifyCertificate,
            ownedTopic: topic,
            security: fixture.security,
            sandboxScope: {
              rendererPreferencesOnly: true,
              noSandboxLaunchArgument: process.getuid?.() === 0,
              osSandboxEnforcement: "not independently measured",
            },
            packages: [current, update].map(({ manifest, sha256 }) => ({
              version: manifest.version,
              apiVersion: manifest.apiVersion,
              sha256,
            })),
            expectedRecordSha256: createHash("sha256").update(marker).digest("hex"),
            scope:
              "Production Electron shell, OS protected storage and real installed backend/renderer; isolated local catalog. Generated owned-topic record only; no business payloads read. Update package retains the same source code with a newer qualification-only manifest.",
          },
          null,
          2,
        ),
        contentType: "application/json",
      });
    });
  }
  if (failures.length > 0)
    throw new Error("Native NSP qualification failed; inspect its sanitized failure evidence.");
});
