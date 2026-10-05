import { setTimeout as delay } from "node:timers/promises";

import AxeBuilder from "@axe-core/playwright";
import { headers, type NatsConnection } from "@nats-io/transport-node";
import { expect, test as base, type Locator, type Page, type Request } from "@playwright/test";

import type { NatsCommandName } from "../../src/features/nats/contracts";
import {
  startNatsBrowserFixture,
  type NatsBrowserFixture,
  type NatsBrowserReceipt,
} from "../support/nats-browser-fixture";
import { findSensitiveArtifactPaths } from "../support/sensitive-artifacts";
import {
  expectNoHorizontalOverflow,
  observeBrowserDiagnostics,
} from "../support/workbench-browser";

interface NatsArtifactPolicy {
  readonly outputDir: string;
  readonly sensitiveValues: readonly string[];
}

interface NatsArtifactHygiene {
  remember(policy: NatsArtifactPolicy): void;
}

const test = base.extend<
  {
    natsBrowser: NatsBrowserFixture;
    natsAuthentication: "token" | "anonymous-restricted";
  },
  {
    natsArtifactHygiene: NatsArtifactHygiene;
  }
>({
  natsAuthentication: ["token", { option: true }],
  context: async (
    { natsBrowser: _natsBrowser, playwright, browserName, launchOptions, contextOptions },
    use,
  ): Promise<void> => {
    // Docker adds/removes host interfaces. Launch each network monitor only after the
    // owned server and renderer are ready, and close it before their fixture teardown.
    const browser = await playwright[browserName].launch(launchOptions);
    try {
      await use(await browser.newContext(contextOptions));
    } finally {
      await boundedCleanup(() => browser.close());
    }
  },
  natsArtifactHygiene: [
    async ({ browserName: _browserName }, use): Promise<void> => {
      const policies: NatsArtifactPolicy[] = [];
      try {
        await use({
          remember: (policy) => {
            policies.push(policy);
          },
        });
      } finally {
        // Worker teardown follows test teardown and every afterAll artifact recorder.
        // Retain generated sentinels only in memory until late error-context files have been written.
        await verifyFinalArtifactPolicies(policies);
      }
    },
    { scope: "worker", timeout: 60_000 },
  ],
  natsBrowser: [
    async ({ natsAuthentication, natsArtifactHygiene }, use, testInfo): Promise<void> => {
      const fixture = await startNatsBrowserFixture(natsAuthentication);
      natsArtifactHygiene.remember({
        outputDir: testInfo.outputDir,
        sensitiveValues: fixture.sensitiveValues,
      });
      try {
        await use(fixture);
      } finally {
        await verifyFixtureCleanup(fixture, testInfo.outputDir);
      }
    },
    // Pinned image pull, private certificate generation, and verified server readiness are local setup.
    { timeout: 180_000 },
  ],
});

// Credential-bearing editors are exercised here; evidence is only captured explicitly after dismissal.
test.use({
  trace: "off",
  video: "off",
  screenshot: "off",
  contextOptions: { reducedMotion: "reduce" },
});
test.beforeAll((): void => {
  // Playwright otherwise captures an automatic aria snapshot on failure, independently of trace.
  // afterAll itself has an artifact recorder. Keep the flag through this isolated file's worker
  // lifetime, including failed context cleanup and the final worker artifact scan.
  process.env.PLAYWRIGHT_NO_COPY_PROMPT = "1";
});

async function verifyFinalArtifactPolicies(policies: readonly NatsArtifactPolicy[]): Promise<void> {
  const scans = await Promise.allSettled(
    policies.map((policy) =>
      findSensitiveArtifactPaths([policy.outputDir], policy.sensitiveValues),
    ),
  );
  if (scans.some((scan) => scan.status === "rejected" || scan.value.length > 0))
    throw new Error("The final NATS browser artifact verification failed.");
}

async function boundedCleanup<Value>(work: () => Promise<Value>): Promise<Value> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work(),
      new Promise<never>((_accept, reject) => {
        timer = setTimeout(
          () => reject(new Error("Owned NATS browser cleanup exceeded its deadline.")),
          60_000,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function verifyFixtureCleanup(fixture: NatsBrowserFixture, outputDir: string): Promise<void> {
  const cleanup = await boundedCleanup(() =>
    Promise.allSettled([
      fixture.dispose(),
      findSensitiveArtifactPaths([outputDir], fixture.sensitiveValues),
    ]),
  );
  if (
    !fixture.publicBoundarySafe() ||
    cleanup.some((result) => result.status === "rejected") ||
    (cleanup[1]?.status === "fulfilled" && cleanup[1].value.length > 0)
  )
    throw new Error("NATS browser cleanup or private-material artifact verification failed.");
}

async function selectProvider(page: Page, provider: "Kafka" | "Core NATS"): Promise<void> {
  const selector = page.getByRole("combobox", { name: "Messaging provider" });
  await selector.click();
  await page.getByRole("option", { name: provider, exact: true }).click();
  await expect(selector).toContainText(provider);
  await expect(selector).toBeEnabled();
}

async function openProduct(
  page: Page,
  fixture: NatsBrowserFixture,
  diagnostics: ReturnType<typeof observeBrowserDiagnostics>,
): Promise<void> {
  const rendererOrigin = new URL(fixture.launch.browserUrl).origin;
  let failures = 0;
  const failedResource = (request: Request): void => {
    const url = new URL(request.url());
    if (
      url.origin !== rendererOrigin ||
      !["document", "script", "stylesheet"].includes(request.resourceType()) ||
      failures >= 32
    )
      return;
    failures += 1;
    const failure = request.failure()?.errorText ?? "";
    const code = /^net::ERR_[A-Z_]+$/u.test(failure) ? failure : "request failed";
    // Static startup evidence only: no query, headers, bodies, or submitted profile material.
    diagnostics.problems.push(
      fixture.containsSensitive(url.pathname)
        ? "A startup resource pathname contained private fixture material."
        : `startup resource ${url.pathname}: ${code}`,
    );
  };
  page.on("requestfailed", failedResource);
  try {
    await page.goto(fixture.launch.browserUrl);
    await expect(page.getByRole("combobox", { name: "Messaging provider" })).toBeVisible({
      timeout: 20_000,
    });
  } catch {
    expectSafeDiagnostics(fixture, diagnostics);
    throw new Error("The actual product did not expose its messaging provider selector.");
  } finally {
    page.off("requestfailed", failedResource);
  }
}

async function openNatsResource(
  page: Page,
  resource: "Connection Profiles" | "Live Subscription",
): Promise<void> {
  const navigation = page.getByRole("navigation", { name: "StreamSkope resources" });
  if (!(await navigation.isVisible()))
    await page.getByRole("button", { name: "Open NATS resources" }).click();
  await navigation.getByRole("button", { name: resource, exact: true }).click();
  await expect(page.getByRole("main", { name: resource })).toBeVisible();
}

async function expectPrivateEditorCleared(page: Page, fixture: NatsBrowserFixture): Promise<void> {
  await expect(page.getByRole("dialog", { name: /^(Create|Edit) NATS profile$/u })).toHaveCount(0);
  const leaked = await page.evaluate(
    (needles): boolean => {
      const storageText = (storage: Storage): string[] => {
        const values: string[] = [];
        for (let index = 0; index < storage.length; index += 1) {
          const key = storage.key(index);
          if (key !== null) values.push(key, storage.getItem(key) ?? "");
        }
        return values;
      };
      const text = [
        location.href,
        document.body.innerText,
        ...Array.from(document.querySelectorAll("input, textarea"), (element) =>
          element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement
            ? element.value
            : "",
        ),
        ...storageText(localStorage),
        ...storageText(sessionStorage),
      ];
      return text.some((value) => needles.some((needle) => value.includes(needle)));
    },
    [...fixture.sensitiveValues],
  );
  expect(leaked).toBe(false);
  expect(fixture.publicBoundarySafe()).toBe(true);
}

async function createProfile(
  page: Page,
  fixture: NatsBrowserFixture,
  name: string,
  options: { token?: string; caPem?: string; anonymous?: boolean } = {},
): Promise<void> {
  await openNatsResource(page, "Connection Profiles");
  await page.getByRole("button", { name: "Add NATS profile" }).click();
  const dialog = page.getByRole("dialog", { name: "Create NATS profile" });
  await expect(dialog.getByRole("textbox", { name: "Profile name", exact: true })).toBeFocused();
  await dialog.getByRole("textbox", { name: "Profile name", exact: true }).fill(name);
  await dialog
    .getByRole("textbox", { name: "NATS servers", exact: true })
    .fill(fixture.server.server);
  if (!options.anonymous) {
    await dialog.getByRole("combobox", { name: "Authentication", exact: true }).click();
    await page.getByRole("option", { name: "Token", exact: true }).click();
    const token = dialog.getByLabel("Token", { exact: true });
    await expect(token).toHaveAttribute("type", "password");
    await fillPrivateField(token, options.token ?? fixture.server.token);
  }
  await dialog.getByRole("radio", { name: "Verified TLS", exact: true }).check();
  await fillPrivateField(
    dialog.getByRole("textbox", { name: "CA certificate PEM", exact: true }),
    options.caPem ?? fixture.server.caPem,
  );
  const receiptIndex = fixture.receipts.length;
  await dialog.getByRole("button", { name: "Save profile", exact: true }).click();
  await expectReceipt(fixture, "profiles.create", receiptIndex);
  await expectPrivateEditorCleared(page, fixture);
  await expect(page.getByRole("grid", { name: "NATS profiles" })).toContainText(name);
}

async function fillPrivateField(field: Locator, value: string): Promise<void> {
  try {
    await field.fill(value);
  } catch {
    // Playwright action errors can contain the fill argument; retain no credential-bearing cause.
    throw new Error("The private NATS profile field could not be filled.");
  }
}

async function expectReceipt(
  fixture: NatsBrowserFixture,
  command: NatsCommandName,
  after: number,
  ok = true,
): Promise<NatsBrowserReceipt> {
  await expect
    .poll(() => fixture.receipts.slice(after).some((receipt) => receipt.command === command), {
      timeout: 10_000,
      intervals: [25, 50, 100],
    })
    .toBe(true);
  const receipt = fixture.receipts.slice(after).find((candidate) => candidate.command === command);
  if (receipt === undefined) throw new Error("The actual NATS command receipt was unavailable.");
  expect(receipt.response.ok).toBe(ok);
  return receipt;
}

async function connectProfile(
  page: Page,
  fixture: NatsBrowserFixture,
  name: string,
): Promise<void> {
  await openNatsResource(page, "Connection Profiles");
  const receiptIndex = fixture.receipts.length;
  await page.getByRole("button", { name: `Connect profile ${name}`, exact: true }).click();
  await expectReceipt(fixture, "profiles.connect", receiptIndex);
  await expect(page.getByLabel("Connection status", { exact: true })).toContainText("Connected");
}

async function startSubscription(
  page: Page,
  fixture: NatsBrowserFixture,
  subject: string,
): Promise<string> {
  await openNatsResource(page, "Live Subscription");
  await page.getByRole("textbox", { name: "Subject filter", exact: true }).fill(subject);
  const receiptIndex = fixture.receipts.length;
  await page.getByRole("button", { name: "Start subscription", exact: true }).click();
  const receipt = await expectReceipt(fixture, "subscription.start", receiptIndex);
  const response = receipt.response;
  if (!response.ok || response.command !== "subscription.start")
    throw new Error("The NATS server did not confirm subscription interest.");
  await expect(page.getByLabel("Subscription status", { exact: true })).toContainText("Streaming");
  const generation = response.result.subscription.generation;
  if (generation === null) throw new Error("The confirmed NATS subscription has no generation.");
  return generation;
}

async function openPublisher(fixture: NatsBrowserFixture): Promise<NatsConnection> {
  // The pinned server is already ready. Reconfirm this separate real SDK client's TLS/PONG
  // after browser startup, allowing bounded retries of the fixture's one-second dial on busy hosts.
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    try {
      return await fixture.server.publisher();
    } catch {
      if (Date.now() < deadline) await delay(100);
    }
  }
  throw new Error("The independent NATS publisher did not become ready within its local deadline.");
}

function natsGrid(page: Page): Locator {
  return page.getByRole("grid", { name: "NATS records", exact: true });
}

async function inspectSubject(page: Page, subject: string): Promise<Locator> {
  const cell = natsGrid(page).getByRole("gridcell", { name: subject, exact: true });
  await expect(cell).toBeVisible({ timeout: 10_000 });
  await cell.click();
  const inspector = page.getByRole("complementary", { name: "Record inspector", exact: true });
  await expect(inspector).toBeVisible();
  return inspector;
}

async function expectProductFrame(page: Page): Promise<void> {
  // A compact modal drawer temporarily hides the background landmarks from assistive technology.
  await expect(page.getByRole("banner", { includeHidden: true })).toHaveCount(1);
  await expect(page.getByRole("main", { includeHidden: true })).toHaveCount(1);
  await expect(page.getByRole("contentinfo", { includeHidden: true })).toHaveCount(1);
  await expect(
    page.getByRole("banner", { name: "StreamSkope application bar", includeHidden: true }),
  ).toBeVisible();
  await expectNoHorizontalOverflow(page);
}

function expectSafeDiagnostics(
  fixture: NatsBrowserFixture,
  diagnostics: ReturnType<typeof observeBrowserDiagnostics>,
): void {
  // Check before an ordinary failure diff can print a console message.
  if (fixture.containsSensitive(diagnostics.problems))
    throw new Error("Browser diagnostics exposed private NATS profile material.");
  expect(diagnostics.problems).toEqual([]);
}

test.describe("real Core NATS browser workspace", () => {
  // Covers browser/page setup too; setting the limit inside a callback is too late for fixtures.
  test.describe.configure({ timeout: 120_000 });
  test("inspects original wildcard records, confirms stop, retains edited credentials and switches providers", async ({
    page,
    natsBrowser: fixture,
  }, testInfo) => {
    test.setTimeout(120_000);
    const diagnostics = observeBrowserDiagnostics(page);
    await page.setViewportSize({ width: 1440, height: 900 });
    await openProduct(page, fixture, diagnostics);

    // Keep an actual Kafka resource visit to check restoration across repeated provider switches.
    const kafkaNavigation = page.getByRole("navigation", { name: "StreamSkope resources" });
    await kafkaNavigation.getByRole("button", { name: "Connection Profiles", exact: true }).click();
    await expect(page.getByRole("main", { name: "Connection Profiles page" })).toBeVisible();
    await selectProvider(page, "Core NATS");
    await expect(page.getByRole("main")).toContainText(/session/iu);

    // An unsubmitted editor closes with Escape and returns to its admitting control.
    const addProfile = page.getByRole("button", { name: "Add NATS profile" });
    await addProfile.click();
    await expect(page.getByRole("textbox", { name: "Profile name", exact: true })).toBeFocused();
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await page.keyboard.press("Escape");
    await expect(addProfile).toBeFocused();

    const profileName = "Browser verified NATS";
    await createProfile(page, fixture, profileName);
    await expectProductFrame(page);
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await connectProfile(page, fixture, profileName);
    const firstGeneration = await startSubscription(page, fixture, "qualification.*");
    const publisher = await openPublisher(fixture);
    const originalJson = ' {\n  "message": "Grüße 🚀",\n  "count": 1\n}\n';
    const jsonSubject = `qualification.json-${"long-subject-".repeat(24)}`;
    const metadata = headers();
    metadata.append("Trace", "one");
    metadata.append("Trace", "two");
    metadata.append("trace", "case-sensitive");
    publisher.publish(jsonSubject, originalJson);
    publisher.publish("qualification.binary", Uint8Array.from([0, 255, 128, 1]), {
      reply: "reply.qualification",
      headers: metadata,
    });
    publisher.publish("qualification.empty", new Uint8Array());
    await publisher.flush();
    await expect.poll(() => fixture.records().length, { timeout: 10_000 }).toBe(3);
    for (const subject of [jsonSubject, "qualification.binary", "qualification.empty"])
      await expect(
        natsGrid(page).getByRole("gridcell", { name: subject, exact: true }),
      ).toBeVisible();
    await expectProductFrame(page);
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);

    const jsonInspector = await inspectSubject(page, jsonSubject);
    // textContent preserves original whitespace; Playwright's toHaveText normalizes whitespace.
    expect(await jsonInspector.getByLabel("Original payload", { exact: true }).textContent()).toBe(
      originalJson,
    );
    await jsonInspector.getByRole("button", { name: "Pretty JSON", exact: true }).click();
    await expect(jsonInspector.getByLabel("Pretty JSON payload", { exact: true })).toContainText(
      '"message": "Grüße 🚀"',
    );
    await jsonInspector.getByRole("button", { name: "Original", exact: true }).click();
    expect(await jsonInspector.getByLabel("Original payload", { exact: true }).textContent()).toBe(
      originalJson,
    );
    await expect(jsonInspector.locator('[data-property-label="Payload bytes"] dd')).toHaveText(
      String(new TextEncoder().encode(originalJson).byteLength),
    );
    await expect(jsonInspector).toContainText("Host received (UTC)");
    const jsonRecord = fixture.records().find((record) => record.subject === jsonSubject);
    if (jsonRecord === undefined)
      throw new Error("The original JSON record was not delivered by the real server.");
    await expect(jsonInspector.locator('[data-property-label="Received at"] dd')).toHaveText(
      jsonRecord.receivedAt,
    );
    expect(Number.isFinite(Date.parse(jsonRecord.receivedAt))).toBe(true);
    expect(jsonRecord).toMatchObject({
      generation: firstGeneration,
      payload: { encoding: "utf8", data: originalJson },
      payloadBytes: new TextEncoder().encode(originalJson).byteLength,
      timestampProvenance: "host-received",
    });

    const binaryInspector = await inspectSubject(page, "qualification.binary");
    await expect(binaryInspector.getByLabel("Original payload", { exact: true })).toHaveText(
      "AP+AAQ==",
    );
    await expect(binaryInspector.locator('[data-property-label="Payload bytes"] dd')).toHaveText(
      "4",
    );
    await expect(binaryInspector).toContainText("reply.qualification");
    await expect(binaryInspector).toContainText("Base64 (binary)");
    const headerEvidence = binaryInspector.getByLabel("Record headers", { exact: true });
    expect(await headerEvidence.locator("dt").allTextContents()).toEqual(["Trace", "trace"]);
    expect(await headerEvidence.locator("dd").allTextContents()).toEqual([
      "one",
      "two",
      "case-sensitive",
    ]);
    expect(
      fixture.records().find((record) => record.subject === "qualification.binary"),
    ).toMatchObject({
      reply: "reply.qualification",
      headers: [
        { name: "Trace", values: ["one", "two"] },
        { name: "trace", values: ["case-sensitive"] },
      ],
      headersTruncated: false,
      payload: { encoding: "base64", data: "AP+AAQ==" },
      payloadBytes: 4,
      timestampProvenance: "host-received",
    });
    const emptyInspector = await inspectSubject(page, "qualification.empty");
    await expect(emptyInspector.getByLabel("Original payload", { exact: true })).toHaveText("");
    await expect(emptyInspector).toContainText("0 bytes");
    await expect(emptyInspector).toContainText("the payload is present");
    expect(
      fixture.records().find((record) => record.subject === "qualification.empty"),
    ).toMatchObject({
      payload: { encoding: "utf8", data: "" },
      payloadBytes: 0,
    });

    // Four actual product visual states, after the credential editor has unmounted.
    await page.getByRole("button", { name: "Close record inspector", exact: true }).click();
    for (const viewport of [
      { width: 1440, height: 900 },
      { width: 800, height: 600 },
    ]) {
      await page.setViewportSize(viewport);
      for (const theme of ["Light", "Dark"] as const) {
        await page.getByRole("button", { name: "Theme", exact: true }).click();
        await page.getByRole("menuitem", { name: theme, exact: true }).click();
        await inspectSubject(page, jsonSubject);
        await expectPrivateEditorCleared(page, fixture);
        await expectProductFrame(page);
        expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
        await page.screenshot({
          animations: "disabled",
          path: testInfo.outputPath(`nats-${String(viewport.width)}-${theme.toLowerCase()}.png`),
        });
        await page.getByRole("button", { name: "Close record inspector", exact: true }).click();
      }
    }
    await page.setViewportSize({ width: 1440, height: 900 });

    // Grid keyboard selection and inspector dismissal return keyboard ownership to the grid.
    const firstCell = natsGrid(page).getByRole("gridcell", { name: jsonSubject, exact: true });
    await firstCell.focus();
    await page.keyboard.press("ArrowDown");
    await page.keyboard.press("Enter");
    await expect(page.getByRole("complementary", { name: "Record inspector" })).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.getByRole("complementary", { name: "Record inspector" })).toBeHidden();
    await expect
      .poll(() => natsGrid(page).evaluate((grid) => grid.contains(document.activeElement)))
      .toBe(true);

    const stopIndex = fixture.receipts.length;
    await page.getByRole("button", { name: "Stop subscription", exact: true }).click();
    await expectReceipt(fixture, "subscription.stop", stopIndex);
    await expect(page.getByLabel("Subscription status", { exact: true })).toContainText("Stopped");
    const stoppedRecords = await natsGrid(page).getByRole("row").allTextContents();
    publisher.publish("qualification.stopped-sentinel", "must-not-arrive-after-confirmed-stop");
    await publisher.flush();
    await delay(350);
    expect(publisher.isClosed()).toBe(false);
    expect(fixture.records().length).toBe(3);
    expect(await natsGrid(page).getByRole("row").allTextContents()).toEqual(stoppedRecords);

    const disconnectIndex = fixture.receipts.length;
    await page.getByRole("button", { name: "Disconnect", exact: true }).click();
    await expectReceipt(fixture, "connection.disconnect", disconnectIndex);
    await expect(page.getByLabel("Connection status", { exact: true })).toContainText(
      "Disconnected",
    );
    await openNatsResource(page, "Connection Profiles");
    await page.getByRole("button", { name: `Edit profile ${profileName}`, exact: true }).click();
    const editor = page.getByRole("dialog", { name: "Edit NATS profile" });
    expect(
      await editor
        .getByLabel("Token", { exact: true })
        .evaluate((element) => element instanceof HTMLInputElement && element.value === ""),
    ).toBe(true);
    expect(
      await editor
        .getByRole("textbox", { name: "CA certificate PEM", exact: true })
        .evaluate((element) => element instanceof HTMLTextAreaElement && element.value === ""),
    ).toBe(true);
    const editIndex = fixture.receipts.length;
    await editor.getByRole("button", { name: "Save profile", exact: true }).click();
    await expectReceipt(fixture, "profiles.update", editIndex);
    await expectPrivateEditorCleared(page, fixture);
    await connectProfile(page, fixture, profileName);
    const nextGeneration = await startSubscription(page, fixture, "qualification.*");
    expect(nextGeneration).not.toBe(firstGeneration);
    await expect(
      natsGrid(page).getByRole("gridcell", { name: jsonSubject, exact: true }),
    ).toHaveCount(0);
    publisher.publish("qualification.fresh", "fresh-generation");
    await publisher.flush();
    await expect(
      natsGrid(page).getByRole("gridcell", { name: "qualification.fresh", exact: true }),
    ).toBeVisible();

    for (let cycle = 0; cycle < 2; cycle += 1) {
      const switchIndex = fixture.receipts.length;
      const recordsBeforeSwitch = fixture.records().length;
      await selectProvider(page, "Kafka");
      const stopped = await expectReceipt(fixture, "subscription.stop", switchIndex);
      const disconnected = await expectReceipt(fixture, "connection.disconnect", switchIndex);
      expect(stopped.completed).toBeLessThan(disconnected.admitted);
      await expect(page.getByRole("main", { name: "Connection Profiles page" })).toBeVisible();
      publisher.publish("qualification.switched-sentinel", "must-not-arrive-in-retired-workspace");
      await publisher.flush();
      await delay(350);
      expect(publisher.isClosed()).toBe(false);
      expect(fixture.records().length).toBe(recordsBeforeSwitch);
      await selectProvider(page, "Core NATS");
      await expect(page.getByLabel("Connection status", { exact: true })).toContainText(
        "Disconnected",
      );
      if (cycle === 0) {
        await connectProfile(page, fixture, profileName);
        const switchedGeneration = await startSubscription(page, fixture, "qualification.*");
        expect(switchedGeneration).not.toBe(nextGeneration);
        publisher.publish("qualification.after-switch", "resumed-new-generation");
        await publisher.flush();
        await expect(
          natsGrid(page).getByRole("gridcell", { name: "qualification.after-switch", exact: true }),
        ).toBeVisible();
      }
    }
    await expectPrivateEditorCleared(page, fixture);
    expectSafeDiagnostics(fixture, diagnostics);
  });

  test("shows safe real authentication and certificate failures and permits a valid recovery", async ({
    page,
    natsBrowser: fixture,
  }) => {
    test.setTimeout(120_000);
    const diagnostics = observeBrowserDiagnostics(page);
    await openProduct(page, fixture, diagnostics);
    await selectProvider(page, "Core NATS");
    for (const failure of [
      {
        name: "Wrong token",
        token: `${fixture.server.token}-invalid`,
        code: "authentication",
        summary: "NATS rejected the supplied authentication.",
      },
      {
        name: "Unrelated CA",
        caPem: fixture.server.untrustedCaPem,
        code: "tls",
        summary: "NATS certificate verification failed.",
      },
    ]) {
      await createProfile(page, fixture, failure.name, failure);
      const receiptIndex = fixture.receipts.length;
      await page
        .getByRole("button", { name: `Connect profile ${failure.name}`, exact: true })
        .click();
      const receipt = await expectReceipt(fixture, "profiles.connect", receiptIndex, false);
      if (receipt.response.ok) throw new Error("The invalid NATS profile unexpectedly connected.");
      expect(receipt.response.error.code).toBe(failure.code);
      await expect(page.getByLabel("Connection status", { exact: true })).toContainText("Failed");
      await expect(page.getByRole("alert").filter({ hasText: failure.summary })).toBeVisible();
      await expectPrivateEditorCleared(page, fixture);
    }
    await createProfile(page, fixture, "Recovered verified NATS");
    await connectProfile(page, fixture, "Recovered verified NATS");
    await startSubscription(page, fixture, "qualification.*");
    const publisher = await openPublisher(fixture);
    publisher.publish("qualification.recovered", "valid-profile-after-safe-failures");
    await publisher.flush();
    await expect(
      natsGrid(page).getByRole("gridcell", { name: "qualification.recovered", exact: true }),
    ).toBeVisible();
    expectSafeDiagnostics(fixture, diagnostics);
  });
});

test.describe("real Core NATS browser permissions", () => {
  test.describe.configure({ timeout: 120_000 });
  test.use({ natsAuthentication: "anonymous-restricted" });

  test("keeps verified transport connected when the server denies a subscription", async ({
    page,
    natsBrowser: fixture,
  }) => {
    test.setTimeout(120_000);
    const diagnostics = observeBrowserDiagnostics(page);
    await openProduct(page, fixture, diagnostics);
    await selectProvider(page, "Core NATS");
    await createProfile(page, fixture, "Restricted verified NATS", { anonymous: true });
    await connectProfile(page, fixture, "Restricted verified NATS");
    await openNatsResource(page, "Live Subscription");
    await page
      .getByRole("textbox", { name: "Subject filter", exact: true })
      .fill("qualification.denied");
    const receiptIndex = fixture.receipts.length;
    await page.getByRole("button", { name: "Start subscription", exact: true }).click();
    const receipt = await expectReceipt(fixture, "subscription.start", receiptIndex, false);
    if (receipt.response.ok) throw new Error("The denied NATS subscription unexpectedly started.");
    expect(receipt.response.error.code).toBe("permission");
    await expect(page.getByLabel("Connection status", { exact: true })).toContainText("Connected");
    await expect(page.getByLabel("Subscription status", { exact: true })).toContainText("Failed");
    await expect(
      page.getByRole("alert").filter({ hasText: "NATS denied the requested subscription." }),
    ).toBeVisible();
    await expectPrivateEditorCleared(page, fixture);

    await startSubscription(page, fixture, "qualification.allowed");
    const publisher = await openPublisher(fixture);
    publisher.publish("qualification.allowed", "server-authorized-subscription");
    await publisher.flush();
    await expect(
      natsGrid(page).getByRole("gridcell", { name: "qualification.allowed", exact: true }),
    ).toBeVisible();
    expectSafeDiagnostics(fixture, diagnostics);
  });
});
