import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { writeFile } from "node:fs/promises";
import { createServer, type Server } from "node:net";
import { promisify } from "node:util";

import { expect, test, type Page } from "@playwright/test";

import { EdaApiError } from "../../plugins/eda/backend/eda-api-client";
import { sourceFromResource } from "../../plugins/eda/backend/eda-capture-source";
import {
  EDA_CAPTURE_DEFAULTS,
  EDA_PLUGIN_ID,
  fromPluginProfileSource,
  parseEdaCaptureResponse,
  type EdaApiCredentialsInput,
  type ProfileEdaCaptureSource,
} from "../../plugins/eda/contracts";
import {
  HOST_PROTOCOL_VERSION,
  type HostCommand,
  type HostCommandResponse,
  type ProfileSummary,
  type StreamSkopeHost,
} from "../../src/features/kafka/contracts";
import {
  localEdaClient,
  requireTargetEdaVersion,
  selectLiveEdaTopic,
} from "../../tools/check/eda-fixture";
import {
  electronPluginStorageAvailable,
  startElectronPluginFixture,
} from "../support/electron-plugin";
import { pluginPackageFixtures } from "../support/plugin-package-fixture";
import { openWorkbenchResource } from "../support/workbench-browser";

// Live credentials and message data must never be recorded in a trace or failure screenshot.
test.use({ screenshot: "off", trace: "off", video: "off" });
const run = promisify(execFile);
interface LiveWindow {
  streamSkopeHost: StreamSkopeHost;
  edaEvidence: {
    profiles: readonly ProfileSummary[];
    records: { topic: string; partition: number; offset: string; matchesFixture: boolean }[];
    sessionIds: string[];
    topics: readonly string[];
  };
}
async function execute<C extends HostCommand>(
  page: Page,
  command: C,
): Promise<HostCommandResponse<C["command"]>> {
  // JSON transfer avoids Playwright recursively expanding the entire command/result union.
  const response = await page.evaluate(async (json: string): Promise<string> => {
    const input = JSON.parse(json) as HostCommand;
    return JSON.stringify(await (window as unknown as LiveWindow).streamSkopeHost.execute(input));
  }, JSON.stringify(command));
  return JSON.parse(response) as HostCommandResponse<C["command"]>;
}
async function profiles(page: Page): Promise<readonly ProfileSummary[]> {
  const result = await execute(page, {
    command: "profiles.list",
    id: randomUUID(),
    version: HOST_PROTOCOL_VERSION,
    payload: {},
  });
  assert(result.ok, "Profile listing failed.");
  return page.evaluate(() => (window as unknown as LiveWindow).edaEvidence.profiles);
}
async function installation(page: Page): Promise<
  | {
      activationId: string | undefined;
      version: string | undefined;
      rendererUrl: string | undefined;
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
  const entry = response.result.pluginSnapshot.plugins.find((p) => p.id === EDA_PLUGIN_ID);
  return entry
    ? {
        activationId: entry.activationId,
        version: entry.active?.version,
        rendererUrl: entry.rendererUrl,
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
async function credentials(page: Page, input: EdaApiCredentialsInput): Promise<void> {
  const dialog = page.getByRole("dialog", { name: "Capture Nokia EDA streams" });
  await dialog.getByLabel("EDA API URL", { exact: true }).fill(input.baseUrl);
  await dialog.getByLabel("EDA username", { exact: true }).fill(input.username);
  await dialog.getByLabel("EDA password", { exact: true }).fill(input.password);
  await expect(dialog.getByLabel("Verify EDA API certificate")).toBeChecked();
  await dialog.getByRole("button", { name: "Discover sources", exact: true }).click();
}
async function bindPort(): Promise<{ server: Server; port: number }> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  assert(address !== null && typeof address === "object");
  return { server, port: address.port };
}
async function closePort(server: Server): Promise<void> {
  if (server.listening)
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
}

test("qualifies live EDA capture through an installed plugin in one protected Electron window", async ({
  browserName: _browserName,
}, info) => {
  test.setTimeout(900_000);
  test.skip(
    process.env.STREAMSKOPE_EDA_INSTALLED_LIVE !== "1",
    "Opt in with STREAMSKOPE_EDA_INSTALLED_LIVE=1 and local EDA API settings.",
  );
  assert(
    electronPluginStorageAvailable,
    "Real protected storage requires D-Bus and GNOME Keyring on Linux.",
  );
  const expectedRecordText = process.env.STREAMSKOPE_EDA_EXPECT_RECORD_TEXT;
  assert(
    expectedRecordText,
    "Configure expected EDA fixture text so receipt proves a known exported record.",
  );
  const client = await localEdaClient();
  assert(client, "Local EDA API settings are required.");
  const observedVersion = await client.clusterVersion();
  requireTargetEdaVersion(observedVersion.releaseVersion);
  assert.equal(
    (await client.captureApplicationStatus()).state,
    "installed",
    "The shared test cluster must already have the capture application; this test never removes it.",
  );
  const edaApi: EdaApiCredentialsInput = {
    baseUrl: process.env.STREAMSKOPE_EDA_API_URL!,
    username: process.env.STREAMSKOPE_EDA_API_USERNAME!,
    password: process.env.STREAMSKOPE_EDA_API_PASSWORD!,
    verifyTls: true,
  };
  const candidates = (await client.listProducers()).flatMap((resource) => {
    if (resource.kind !== "Producer" && resource.kind !== "ClusterProducer") return [];
    const source = sourceFromResource(
      resource,
      "kafka.eda.nokia.com/v1",
      resource.kind,
      EDA_CAPTURE_DEFAULTS.namespace,
    );
    return source ? [source] : [];
  });
  const selected = candidates.find(
    (p) =>
      !process.env.STREAMSKOPE_EDA_CAPTURE_PRODUCER ||
      p.name === process.env.STREAMSKOPE_EDA_CAPTURE_PRODUCER,
  );
  assert(selected, "A producer with periodic exported records is required.");
  const originalSpec = (await client.getProducer(selected)).spec;
  const topic = selectLiveEdaTopic(originalSpec, selected.topics);
  assert(topic, "A real exported topic is required.");
  if (process.env.STREAMSKOPE_PLUGIN_PACKAGE_READY !== "1")
    await run(process.execPath, ["--import", "tsx", "tools/package/plugin.ts", "eda"], {
      maxBuffer: 4 * 1_048_576,
    });
  const { current, update } = await pluginPackageFixtures();
  const fixture = await startElectronPluginFixture(
    current.bytes,
    info,
    [edaApi.password, edaApi.baseUrl],
    process.env.STREAMSKOPE_EDA_API_CA
      ? { NODE_EXTRA_CA_CERTS: process.env.STREAMSKOPE_EDA_API_CA }
      : {},
  );
  const { page, application } = fixture;
  page.setDefaultTimeout(30_000);
  const occupied = await bindPort();
  const owned = new Set<string>();
  const checks: string[] = [];
  let passed = false;
  let scenarioFailure: unknown;
  const cleanupFailures: { operation: string; error: string }[] = [];
  const sanitizedError = (error: unknown): string => {
    let text =
      error instanceof Error
        ? (error.stack ?? error.message)
            .split("\n")
            .filter((line, index) => index === 0 || /^\s+at /u.test(line))
            .join("\n")
        : "Unknown qualification error";
    for (const value of [edaApi.password, edaApi.baseUrl])
      if (value) text = text.replaceAll(value, "[redacted]");
    return text;
  };
  let captureProfileId: string | undefined;
  const removed = async (id: string): Promise<void> => {
    await expect
      .poll(
        async () => {
          try {
            await client.getCaptureSession(id);
            return false;
          } catch (error) {
            if (error instanceof EdaApiError && error.code === "PROFILE_NOT_FOUND") return true;
            throw error;
          }
        },
        { timeout: 30_000 },
      )
      .toBe(true);
  };
  const recordReceipt = async (): Promise<void> => {
    await expect
      .poll(
        async () => {
          const listed = await execute(page, {
            command: "topics.list",
            id: randomUUID(),
            version: HOST_PROTOCOL_VERSION,
            payload: {},
          });
          assert(listed.ok, "Topic metadata request failed.");
          return page.evaluate(
            (name) => (window as unknown as LiveWindow).edaEvidence.topics.includes(name),
            topic,
          );
        },
        { timeout: 60_000 },
      )
      .toBe(true);

    const before = await page.evaluate(
      () =>
        (window as unknown as LiveWindow).edaEvidence.records.filter(
          (record) => record.matchesFixture,
        ).length,
    );
    const started = await execute(page, {
      command: "messages.start",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: { topic, mode: "earliest", maxMessages: 100 },
    });
    assert(
      started.ok,
      `Kafka message reader did not start: ${JSON.stringify(started.ok ? {} : started.error)}`,
    );
    await expect
      .poll(
        () =>
          page.evaluate(
            () =>
              (window as unknown as LiveWindow).edaEvidence.records.filter(
                (record) => record.matchesFixture,
              ).length,
          ),
        { timeout: 90_000 },
      )
      .toBeGreaterThan(before);
    const stopped = await execute(page, {
      command: "messages.stop",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: {},
    });
    assert(stopped.ok);
  };
  const savedSource = async (): Promise<ProfileEdaCaptureSource> => {
    const currentProfiles = await profiles(page);
    const profile =
      currentProfiles.find((p) => p.id === captureProfileId) ??
      currentProfiles.find((p) => p.source?.pluginId === EDA_PLUGIN_ID);
    assert(profile, "Capture profile was not saved.");
    captureProfileId = profile.id;
    const source = fromPluginProfileSource(profile.source);
    assert(source?.sessionId, "Capture profile lacks its remote session identifier.");
    owned.add(source.sessionId);
    return source;
  };
  const connect = async (): Promise<void> => {
    const profile = (await profiles(page)).find((p) => p.id === captureProfileId)!;
    if (!profile.active)
      await page
        .getByRole("button", {
          name: `Connect insecure plaintext profile ${profile.name}`,
          exact: true,
        })
        .click();
    await expect
      .poll(async () => (await profiles(page)).find((p) => p.id === captureProfileId)?.active, {
        timeout: 30_000,
      })
      .toBe(true);
  };
  const resume = async (): Promise<ProfileEdaCaptureSource> => {
    await page.getByRole("button", { name: "Resume capture", exact: true }).click();
    await credentials(page, edaApi);
    await page
      .getByRole("dialog", { name: "Capture Nokia EDA streams" })
      .getByRole("button", { name: "Resume capture", exact: true })
      .click();
    await expect(page.getByRole("dialog", { name: "Capture Nokia EDA streams" })).toHaveCount(0, {
      timeout: 360_000,
    });
    return savedSource();
  };
  try {
    await page.evaluate((expected) => {
      const target = window as unknown as LiveWindow;
      target.edaEvidence = { profiles: [], records: [], sessionIds: [], topics: [] };
      target.streamSkopeHost.subscribe((event) => {
        if (event.event === "profiles.changed") {
          target.edaEvidence.profiles = event.payload.profiles;
          for (const profile of event.payload.profiles) {
            const id = profile.source?.data.sessionId;
            if (typeof id === "string" && !target.edaEvidence.sessionIds.includes(id))
              target.edaEvidence.sessionIds.push(id);
          }
        }
        if (event.event === "topics.changed") target.edaEvidence.topics = event.payload.topics;
        if (event.event === "messages.batch")
          for (const record of event.payload.messages)
            target.edaEvidence.records.push({
              topic: record.topic,
              partition: record.partition,
              offset: record.offset,
              matchesFixture: record.payload?.includes(expected) === true,
            });
      });
    }, expectedRecordText);
    await openPlugins(page);
    const card = page.getByRole("region", { name: "EDA Capture", exact: true });
    await card.getByRole("button", { name: "Install", exact: true }).click();
    await expect(card).toContainText(`Active version ${current.manifest.version}`);
    const first = await installation(page);
    assert(first?.activationId);
    checks.push(
      "actual-package-ui-install",
      "protected-profile-store",
      "sandboxed-production-shell",
    );
    await closePreferences(page);
    // Observe the shared cluster application; installing it again could roll other users’ captures.
    const appResponse = await execute(page, {
      command: "plugin.execute",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: {
        pluginId: EDA_PLUGIN_ID,
        activationId: first.activationId,
        method: "edaCapture.application.status",
        input: { edaApi: { ...edaApi } },
      },
    });
    assert(appResponse.ok);
    const appResult = parseEdaCaptureResponse(appResponse.result.output);
    assert(
      appResult.ok &&
        "application" in appResult.result &&
        appResult.result.application.state === "installed",
    );
    checks.push("existing-cluster-application-ready");
    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await page.getByRole("menuitem", { name: "Capture from EDA", exact: true }).click();
    await credentials(page, edaApi);
    if (candidates.length > 1) {
      await page.getByLabel("Exporter source", { exact: true }).click();
      await page
        .getByRole("option", {
          name: `${selected.kind} · ${selected.name} · ${selected.topics.join(", ")}`,
          exact: true,
        })
        .click();
    }
    await page.getByRole("button", { name: "Set up temporary capture", exact: true }).click();
    const dialog = page.getByRole("dialog", { name: "Capture Nokia EDA streams" });
    await expect(dialog).toContainText("StreamSkope Capture is installed", { timeout: 30_000 });
    await dialog.getByLabel("Local Kafka port", { exact: true }).fill(String(occupied.port));
    await dialog.getByRole("button", { name: "Start capture", exact: true }).click();
    await expect(dialog).toContainText(/already in use|already being used/u, { timeout: 30_000 });
    assert(occupied.server.listening, "Port conflict must not close another process's listener.");
    assert.equal((await profiles(page)).length, 0);
    assert.deepEqual((await client.getProducer(selected)).spec, originalSpec);
    checks.push("port-conflict-actionable-no-profile", "foreign-listener-preserved");
    await closePort(occupied.server);
    await dialog.getByRole("button", { name: "Start capture", exact: true }).click();
    await expect(dialog).toHaveCount(0, { timeout: 360_000 });
    const initialSource = await savedSource();
    // The agent permits one live capture globally. Unknown-ID cleanup must leave it intact.
    await client.removeCaptureSession(randomUUID());
    assert.equal((await client.getCaptureSession(initialSource.sessionId!)).phase, "Ready");
    checks.push("unknown-session-cleanup-preserves-active-session");
    await connect();
    await recordReceipt();
    checks.push("ui-capture-profile-created", "known-exported-record-received");
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
    assert(stopped.ok, "Topic read preparation failed.");
    await page.getByRole("combobox", { name: "Read mode" }).click();
    await page.getByRole("option", { name: "First N", exact: true }).click();
    await page.getByRole("button", { name: `Load messages ${topic}`, exact: true }).click();
    await expect(page.getByText(expectedRecordText, { exact: false }).first()).toBeVisible({
      timeout: 60_000,
    });
    checks.push("native-ui-known-record-visible");
    await openWorkbenchResource(page, "Connection Profiles");
    const captureProfile = (await profiles(page)).find((p) => p.id === captureProfileId)!;
    await page
      .getByRole("button", {
        name: `Disconnect insecure plaintext profile ${captureProfile.name}`,
        exact: true,
      })
      .click();
    await expect
      .poll(async () => (await profiles(page)).find((p) => p.id === captureProfileId)?.active)
      .toBe(false);
    assert.equal((await client.getCaptureSession(initialSource.sessionId!)).phase, "Ready");
    await connect();
    await recordReceipt();
    checks.push("disconnect-retains-capture", "reconnect-receives-record");
    await writeFile(fixture.catalogPath, update.bytes);
    await openPlugins(page);
    await card
      .getByRole("button", { name: `Update to ${update.manifest.version}`, exact: true })
      .click();
    const updateDialog = page.getByRole("dialog", { name: /Update/u });
    await expect(updateDialog).toContainText("Stop EDA capture");
    await updateDialog
      .getByRole("button", { name: "Stop capture and update", exact: true })
      .click();
    await expect(card).toContainText(`Active version ${update.manifest.version}`, {
      timeout: 60_000,
    });
    const replacement = await installation(page);
    assert(replacement?.activationId && replacement.activationId !== first.activationId);
    await removed(initialSource.sessionId!);
    assert.deepEqual((await client.getProducer(selected)).spec, originalSpec);
    const retained = (await profiles(page)).find((p) => p.id === captureProfileId);
    assert(retained && !retained.active);
    checks.push(
      "active-update-confirmed-cleanup",
      "saved-profile-retained",
      "original-producer-preserved-during-update",
    );
    await closePreferences(page);
    const updatedSource = await resume();
    assert.notEqual(updatedSource.sessionId, initialSource.sessionId);
    assert.equal((await profiles(page)).length, 1);
    await connect();
    await recordReceipt();
    checks.push("resume-after-update-new-session-same-profile", "updated-package-record-receipt");
    await openPlugins(page);
    await card.getByRole("button", { name: "Remove", exact: true }).click();
    await page.getByRole("button", { name: "Stop capture and remove", exact: true }).click();
    await expect(card).toContainText("Not installed", { timeout: 60_000 });
    await removed(updatedSource.sessionId!);
    assert.deepEqual((await client.getProducer(selected)).spec, originalSpec);
    assert.equal((await profiles(page)).length, 1);
    checks.push("active-removal-cleans-only-owned-session", "removed-plugin-retains-profile");
    const repeated = await execute(page, {
      command: "plugins.remove",
      id: randomUUID(),
      version: HOST_PROTOCOL_VERSION,
      payload: { pluginId: EDA_PLUGIN_ID },
    });
    assert(repeated.ok, "Repeated plugin removal must remain safe.");
    await card.getByRole("button", { name: "Install", exact: true }).click();
    await expect(card).toContainText(`Active version ${update.manifest.version}`);
    await closePreferences(page);
    const reinstalledSource = await resume();
    assert.notEqual(reinstalledSource.sessionId, updatedSource.sessionId);
    await connect();
    await recordReceipt();
    checks.push(
      "repeat-removal-idempotent",
      "reinstall-resume-same-profile",
      "reinstalled-package-record-receipt",
    );
    await page
      .getByRole("button", {
        name: `Disconnect insecure plaintext profile ${captureProfile.name}`,
        exact: true,
      })
      .click();
    await page.getByRole("button", { name: "Stop and remove capture", exact: true }).click();
    await page
      .getByRole("dialog", { name: "Remove capture resources", exact: true })
      .getByRole("button", { name: "Remove capture resources", exact: true })
      .click();
    await expect.poll(async () => (await profiles(page)).length, { timeout: 30_000 }).toBe(0);
    await removed(reinstalledSource.sessionId!);
    assert.deepEqual((await client.getProducer(selected)).spec, originalSpec);
    assert.deepEqual((await client.getProducer(selected)).spec, originalSpec);
    checks.push("ui-stop-cleans-session-and-profile", "original-producer-preserved");
    expect(await page.evaluate(() => performance.timeOrigin)).toBe(fixture.origin);
    expect(application.process().pid).toBe(fixture.processId);
    expect(application.windows()).toEqual([page]);
    expect(fixture.errors).toEqual([]);
    expect(fixture.assetFailures).toEqual([]);
    checks.push("same-process-and-window-no-reload", "no-renderer-errors");
    passed = true;
  } catch (error) {
    scenarioFailure = error;
  } finally {
    // Playwright can attach an error-context DOM snapshot even when tracing is disabled.
    await page
      .locator('input[type="password"]')
      .evaluateAll((inputs) => {
        for (const input of inputs) if (input instanceof HTMLInputElement) input.value = "";
      })
      .catch(() => undefined);
    for (const id of await page
      .evaluate(() => (window as unknown as LiveWindow).edaEvidence?.sessionIds ?? [])
      .catch(() => []))
      owned.add(id);
    // A deploy can succeed before profile save, so recover its UUID from the live backend too.
    try {
      const active = await installation(page);
      if (active?.activationId) {
        const response = await execute(page, {
          command: "plugin.execute",
          id: randomUUID(),
          version: HOST_PROTOCOL_VERSION,
          payload: {
            pluginId: EDA_PLUGIN_ID,
            activationId: active.activationId,
            method: "edaCapture.status",
            input: {},
          },
        });
        if (response.ok) {
          const status = parseEdaCaptureResponse(response.result.output);
          if (
            status.ok &&
            "captureSession" in status.result &&
            status.result.captureSession.source?.sessionId
          )
            owned.add(status.result.captureSession.source.sessionId);
        }
      }
    } catch {
      /* Earlier failures still leave profile/session identifiers for bounded cleanup. */
    }
    try {
      await closePort(occupied.server);
    } catch (error) {
      cleanupFailures.push({ operation: "local-port", error: sanitizedError(error) });
    }
    // Close Kafka/tunnels before removing only UUIDs created by this test; attempt every UUID.
    try {
      await fixture.close();
    } catch (error) {
      cleanupFailures.push({ operation: "electron-close", error: sanitizedError(error) });
    }
    for (const id of owned) {
      try {
        await client.removeCaptureSession(id);
        await removed(id);
      } catch (error) {
        cleanupFailures.push({ operation: `session-${id}`, error: sanitizedError(error) });
      }
    }
    try {
      assert.deepEqual((await client.getProducer(selected)).spec, originalSpec);
    } catch (error) {
      cleanupFailures.push({ operation: "original-producer", error: sanitizedError(error) });
    }
    await info.attach("live-eda-installed-lifecycle", {
      body: JSON.stringify(
        {
          outcome: passed && cleanupFailures.length === 0 ? "passed" : "failed",
          checkedAt: new Date().toISOString(),
          targetVersion: observedVersion.releaseVersion,
          platform: process.platform,
          architecture: process.arch,
          packageSha256: current.sha256,
          expectedFixtureTextSha256: createHash("sha256").update(expectedRecordText).digest("hex"),
          recordPredicate: "configured known fixture text is present in received Kafka payload",
          updatePackageSha256: update.sha256,
          updateKind:
            "same actual built backend/renderer with a fixture-only next manifest version",
          catalog: "isolated local package catalog; public download not qualified",
          scope:
            "production Electron shell, real protected storage, UI install/capture/update/remove/reinstall, real EDA API and Kafka; installer replacement and fresh cluster-app installation are separate",
          checks,
          ownedSessionIds: [...owned],
          ...(scenarioFailure ? { failure: sanitizedError(scenarioFailure) } : {}),
          cleanupFailures,
          cleanup:
            cleanupFailures.length === 0
              ? "all test-owned UUIDs absent; original producer unchanged"
              : "cleanup not confirmed; inspect test-owned UUIDs in local diagnostics",
        },
        null,
        2,
      ),
      contentType: "application/json",
    });
  }
  if (scenarioFailure) throw new Error("EDA lifecycle scenario failed; see sanitized evidence.");
  assert.equal(cleanupFailures.length, 0, "EDA qualification cleanup could not be confirmed.");
});
