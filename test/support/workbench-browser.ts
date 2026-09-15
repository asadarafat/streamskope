import { expect, type ConsoleMessage, type Locator, type Page } from "@playwright/test";

export interface BrowserDiagnostics {
  readonly problems: string[];
}

export interface BrowserTypography {
  readonly fontFamily: string;
  readonly fontSize: string;
  readonly fontWeight: string;
  readonly lineHeight: string;
}

export async function expectWorkbenchReady(page: Page): Promise<void> {
  const navigation = page.getByRole("navigation", { name: "StreamSkope resources" });
  await expect(page.getByRole("banner", { name: "StreamSkope application bar" })).toContainText(
    "StreamSkope",
  );
  if (await navigation.isVisible()) await expect(navigation).toBeVisible();
}

export async function openWorkbenchResource(
  page: Page,
  resource: "Connection Profiles" | "Consumer Groups" | "Overview" | "Topics",
): Promise<void> {
  const navigation = page.getByRole("navigation", { name: "StreamSkope resources" });
  if (!(await navigation.isVisible())) {
    await page.getByRole("button", { name: "Open Kafka resources" }).click();
  }
  await expect(navigation).toBeVisible();
  await navigation.getByRole("button", { name: resource }).click();
}

export async function openTopicDetail(page: Page, topic: string): Promise<void> {
  const detail = page.getByRole("main", { name: "Topic detail page" });
  if (!(await detail.isVisible())) {
    const inventory = page.getByRole("main", { name: "Topics page" });
    if (!(await inventory.isVisible())) {
      await openWorkbenchResource(page, "Topics");
    }
    await page.getByRole("button", { exact: true, name: topic }).click();
  }
  await expect(detail.getByRole("heading", { exact: true, name: topic })).toBeVisible();
}

export async function fetchTopicMessages(page: Page, topic: string): Promise<void> {
  await openTopicDetail(page, topic);
  const readAction = page.getByRole("button", {
    name: new RegExp(
      `^(?:Start tail|Load messages) ${topic.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&")}$`,
      "u",
    ),
  });
  if ((await readAction.count()) > 0 && (await readAction.isEnabled())) {
    await readAction.click();
  }
}

export function observeBrowserDiagnostics(page: Page): BrowserDiagnostics {
  const problems: string[] = [];
  page.on("console", (message: ConsoleMessage) => {
    if (message.type() === "error" || message.type() === "warning") {
      problems.push(`${message.type()}: ${message.text()}`);
    }
  });
  page.on("pageerror", (error) => {
    problems.push(`pageerror: ${error.message}`);
  });
  return { problems };
}

export async function readTypography(locator: Locator): Promise<BrowserTypography> {
  return locator.evaluate((element) => {
    const style = getComputedStyle(element);
    return {
      fontFamily: style.fontFamily,
      fontSize: style.fontSize,
      fontWeight: style.fontWeight,
      lineHeight: style.lineHeight,
    };
  });
}

export async function expectStackedAtSameInset(upper: Locator, lower: Locator): Promise<void> {
  const [upperBounds, lowerBounds] = await Promise.all([upper.boundingBox(), lower.boundingBox()]);
  if (upperBounds === null || lowerBounds === null) {
    throw new Error("Expected both stacked elements to have visible bounds.");
  }
  expect(lowerBounds.y).toBeGreaterThan(upperBounds.y + upperBounds.height);
  expect(Math.abs(lowerBounds.x - upperBounds.x)).toBeLessThanOrEqual(1);
}

export async function expectNoHorizontalOverflow(page: Page): Promise<void> {
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(
    true,
  );
}

export async function expectRawLogEvidence(
  activity: Locator,
  operation: string,
  expectedDetail: string,
  object?: string,
): Promise<void> {
  const rawLog = activity.getByRole("log", { name: "Raw activity log" });
  await expect(rawLog).toContainText(`operation="${operation}"`);
  if (object !== undefined) {
    await expect(rawLog).toContainText(`object="${object}"`);
  }
  await expect(rawLog).toContainText(expectedDetail);
}

export async function expectRawLogFields(
  activity: Locator,
  fields: ReadonlyArray<readonly [label: string, value: string]>,
): Promise<void> {
  const rawLog = activity.getByRole("log", { name: "Raw activity log" });
  for (const [label, value] of fields) {
    await expect(rawLog).toContainText(`${label}: ${value}`);
  }
}

export async function openTopicTask(
  page: Page,
  task: "Configuration" | "Latency" | "Messages" | "Monitor" | "Rules",
): Promise<void> {
  await page
    .getByRole("tablist", { name: "Topic sections" })
    .getByRole("tab", { name: task })
    .click();
}

export async function openProfileActions(page: Page, profileName: string): Promise<Locator> {
  await page.getByRole("button", { name: `More actions for profile ${profileName}` }).click();
  const menu = page.getByRole("menu", { name: `Profile actions for ${profileName}` });
  await expect(menu).toBeVisible();
  return menu;
}

export async function openProfileAction(
  page: Page,
  profileName: string,
  action: "Cluster detail" | "Delete" | "Edit",
): Promise<void> {
  const menu = await openProfileActions(page, profileName);
  await menu.getByRole("menuitem", { name: action }).click();
}

export async function openActivity(page: Page): Promise<Locator> {
  const activity = page.getByRole("complementary", { name: "Activity log" });
  if (!(await activity.isVisible())) {
    await page.getByRole("button", { name: "Expand Activity" }).click();
  }
  await expect(activity).toBeVisible();
  return activity;
}

export async function collapseActivity(page: Page): Promise<void> {
  const activity = page.getByRole("complementary", { name: "Activity log" });
  if (await activity.isVisible()) {
    await page.getByRole("button", { name: "Collapse Activity" }).click();
  }
  await expect(activity).toBeHidden();
}

export async function assertPersistentWorkbenchBars(page: Page): Promise<void> {
  expect(await page.evaluate(() => globalThis.scrollY)).toBe(0);
  const navigation = page.getByRole("navigation", { name: "StreamSkope resources" });
  const main = page.getByRole("main");
  for (const [name, locator] of [
    ["StreamSkope resources", navigation],
    ["active resource page", main],
  ] as const) {
    const bounds = await locator.boundingBox();
    if (bounds === null) {
      throw new Error(`${name} has no visible bounds.`);
    }
    expect(bounds.y).toBeGreaterThanOrEqual(0);
  }
}
