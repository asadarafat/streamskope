/* global document, innerWidth, process, URL */
import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import { resolve } from "node:path";
import { expect } from "@playwright/test";

const stableTag = /^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/u;
const versionOrder = new Intl.Collator("en", { numeric: true });

export function isReleasePreview(tag, desktopRelease, metadata = "") {
  if (/^release_status:\s*(?:pending|"pending"|'pending')\s*$/mu.test(metadata)) return true;
  const core = (version) => /^v(\d+\.\d+\.\d+)/u.exec(version)?.[1];
  const candidate = core(tag);
  const published = core(desktopRelease);
  assert(candidate && published, "Release status uses exact version tags");
  return versionOrder.compare(candidate, published) > 0;
}

async function checkReleaseNavigation(page, base, evidence, accessible) {
  const [configuration, files] = await Promise.all([
    readFile("website/zensical.toml", "utf8"),
    readdir("website/docs/releases"),
  ]);
  const desktopRelease = /^desktop_release = "([^"]+)"$/mu.exec(configuration)?.[1];
  assert(desktopRelease, "Release navigation has a documented desktop baseline");
  const records = await Promise.all(
    files
      .filter((file) => /^v.+\.md$/u.test(file))
      .map(async (file) => {
        const source = await readFile(`website/docs/releases/${file}`, "utf8");
        const metadata = /^---\r?\n([\s\S]*?)\r?\n---/u.exec(source)?.[1] ?? "";
        const tag = file.slice(0, -3);
        return { tag, pending: isReleasePreview(tag, desktopRelease, metadata) };
      }),
  );
  const recent = records
    .filter(({ tag, pending }) => stableTag.test(tag) && !pending)
    .sort((a, b) => versionOrder.compare(b.tag, a.tag))
    .slice(0, 5)
    .map(({ tag }) => tag);
  assert(recent.length, "The sidebar offers published stable release notes");
  const older = records.find(({ tag, pending }) => !pending && !recent.includes(tag))?.tag;
  assert(older, "An archived release exercises navigation outside the recent five");
  const published = process.env.STREAMSKOPE_DOCS_PUBLISH === "1";
  const preview = !published && files.includes("unreleased.md");
  const expectedLeaves = [...(preview ? ["Unreleased"] : []), ...recent, "See all releases"];
  const sidebar = page.locator(".md-sidebar--primary");
  const branch = sidebar.locator("[data-sk-releases]");
  const releaseNav = branch.getByRole("navigation", { name: "Releases", exact: true });
  const toggle = branch.getByRole("button", { name: "Releases", exact: true });
  const drawer = page.locator("#__drawer");
  const trigger = page.getByRole("button", { name: "Open navigation", exact: true });
  async function openNavigation(mobile) {
    if (mobile && !(await drawer.isChecked())) {
      await trigger.focus();
      await page.keyboard.press("Enter");
      await expect(drawer).toBeChecked();
    }
  }
  async function checkLeaves() {
    const leaves = releaseNav.locator(":scope > ul.md-nav__list > li > a.md-nav__link");
    assert.deepEqual(
      (await leaves.allTextContents()).map((title) => title.trim()),
      expectedLeaves,
      "Release sidebar contains five stable versions in SemVer order and a final overview link",
    );
    for (const { tag } of records.filter(({ tag }) => !recent.includes(tag))) {
      await expect(sidebar.getByRole("link", { name: tag, exact: true })).toHaveCount(0);
    }
  }
  async function checkBreadcrumbs(tag) {
    const breadcrumbs = page.locator(".md-path");
    await expect(breadcrumbs).toBeVisible();
    assert.deepEqual(
      (await breadcrumbs.locator(".md-path__item").allTextContents()).map((text) => text.trim()),
      ["Home", "Releases", tag],
      "Every release retains its full breadcrumb ancestry and exact current tag",
    );
    await expect(breadcrumbs.locator('[aria-current="page"]')).toHaveText(tag);
    const overview = breadcrumbs.getByRole("link", { name: "Releases", exact: true });
    assert.equal(new URL(await overview.getAttribute("href"), page.url()).href, base + "releases/");
  }
  for (const width of [1440, 390]) {
    const mobile = width < 1220;
    await page.setViewportSize({ width, height: 900 });
    for (const theme of ["light", "dark"]) {
      await page.goto(base);
      const scheme = await page.locator("body").getAttribute("data-md-color-scheme");
      if ((scheme === "slate") !== (theme === "dark"))
        await page.locator(`label[title="Switch to ${theme} mode"]`).click();
      await openNavigation(mobile);
      const title = branch.locator(".md-nav__container > a");
      await expect(title).toHaveText("Releases");
      await title.click();
      await page.waitForURL(base + "releases/");
      await expect(page.locator("h1").first()).toHaveText(/^Release history\s*¶?$/u);
      const history = page.getByRole("region", { name: "Release history", exact: true });
      for (const { tag, pending } of records) {
        await expect(history.getByRole("link", { name: tag, exact: true })).toHaveCount(
          published && pending ? 0 : 1,
        );
      }
      if (published) {
        await expect(page.locator('a[href*="unreleased/"]')).toHaveCount(0);
        await expect(
          history.locator("td:first-child").getByText("Preview", { exact: true }),
        ).toHaveCount(0);
      }
      assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
      await accessible();
      await page.screenshot({ path: resolve(evidence, `release-history-${width}-${theme}.png`) });
      await openNavigation(mobile);
      const state = branch.locator(".md-nav__toggle");
      await expect(state).toBeChecked();
      await expect(toggle).toHaveAttribute("aria-expanded", "true");
      await toggle.focus();
      await page.keyboard.press("Space");
      await expect(state).not.toBeChecked();
      await expect(toggle).toHaveAttribute("aria-expanded", "false");
      await expect(releaseNav).toHaveAttribute("aria-expanded", "false");
      await page.keyboard.press("Enter");
      await expect(state).toBeChecked();
      await expect(toggle).toHaveAttribute("aria-expanded", "true");
      await checkLeaves();

      await releaseNav.getByRole("link", { name: recent[0], exact: true }).click();
      await page.waitForURL(base + `releases/${recent[0]}/`);
      await openNavigation(mobile);
      const current = releaseNav.getByRole("link", { name: recent[0], exact: true });
      await expect(current).toHaveAttribute("aria-current", "page");
      await expect(current).toHaveClass(/md-nav__link--active/u);
      await checkBreadcrumbs(recent[0]);
      await checkLeaves();
      assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
      await accessible();
      await page.screenshot({ path: resolve(evidence, `release-current-${width}-${theme}.png`) });

      await releaseNav.getByRole("link", { name: "See all releases", exact: true }).click();
      await page.waitForURL(base + "releases/");
      await history.getByRole("link", { name: older, exact: true }).click();
      await page.waitForURL(base + `releases/${older}/`);
      await openNavigation(mobile);
      await expect(sidebar.getByRole("link", { name: older, exact: true })).toHaveCount(0);
      await expect(toggle).toHaveAttribute("aria-expanded", "true");
      await checkBreadcrumbs(older);
      await checkLeaves();
      assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
      await accessible();
      await page.screenshot({ path: resolve(evidence, `release-archive-${width}-${theme}.png`) });
      if (mobile) await page.keyboard.press("Escape");
      await page.locator(".md-path").getByRole("link", { name: "Releases", exact: true }).click();
      await page.waitForURL(base + "releases/");
    }
  }
}

export async function checkNavigation(page, base, evidence, accessible) {
  for (const width of [1440, 1000, 768, 390, 320]) {
    await page.setViewportSize({ width, height: 900 });
    for (const theme of ["light", "dark"]) {
      await page.goto(base);
      const scheme = await page.locator("body").getAttribute("data-md-color-scheme");
      if ((scheme === "slate") !== (theme === "dark"))
        await page.locator(`label[title="Switch to ${theme} mode"]`).click();

      const brand = page.getByRole("link", { name: "StreamSkope home", exact: true });
      await expect(brand.locator("img")).toBeVisible();
      await expect(brand.getByText("StreamSkope", { exact: true })).toBeVisible();
      const brandBox = await brand.boundingBox();
      const toolsBox = await page.locator(".sk-header-tools").boundingBox();
      assert(
        brandBox.x + brandBox.width <= toolsBox.x + 1,
        "Brand does not overlap header controls",
      );

      const mobile = width < 1220;
      const sidebar = page.locator(".md-sidebar--primary");
      const navigation = sidebar.getByRole("navigation", { name: "Navigation", exact: true });
      const trigger = page.getByRole("button", { name: "Open navigation", exact: true });
      const drawer = page.locator("#__drawer");
      const sectionToggle = (name) =>
        sidebar.locator("label.md-nav__link[for]").filter({ hasText: name });
      async function openDrawer() {
        await trigger.focus();
        await page.keyboard.press("Enter");
        await expect(drawer).toBeChecked();
        await expect(trigger).toHaveAttribute("aria-expanded", "true");
        await expect(sidebar).toHaveJSProperty("inert", false);
        await expect(navigation).toBeInViewport();
      }
      async function closedDrawer() {
        await expect(drawer).not.toBeChecked();
        await expect(trigger).toHaveAttribute("aria-expanded", "false");
        await expect(sidebar).toHaveJSProperty("inert", true);
      }

      await expect(page.getByRole("button", { name: "Browse docs", exact: true })).toHaveCount(0);
      await expect(sidebar).not.toHaveAttribute("hidden");
      if (mobile) {
        await expect(trigger).toBeVisible();
        await expect(trigger).toHaveAttribute("aria-controls", "documentation-navigation");
        await expect(sidebar).toHaveAttribute("id", "documentation-navigation");
        await closedDrawer();
        await openDrawer();
        const close = sidebar.getByRole("button", { name: "Close navigation", exact: true });
        await expect(close).toBeFocused();
        await page.keyboard.press("Shift+Tab");
        await expect(navigation.getByRole("link", { name: "About", exact: true })).toBeFocused();
        await page.keyboard.press("Tab");
        await expect(close).toBeFocused();
      } else {
        await expect(trigger).toBeHidden();
        await expect(sidebar).toHaveJSProperty("inert", false);
        await expect(navigation).toBeInViewport();
        const sidebarBox = await sidebar.boundingBox();
        const contentBox = await page.locator(".md-content").boundingBox();
        assert(sidebarBox.x + sidebarBox.width <= contentBox.x + 1, "Home has a left sidebar");
      }
      await expect(navigation.getByRole("link", { name: "Home", exact: true })).toHaveAttribute(
        "aria-current",
        "page",
      );

      // The native section controls expose the same documentation on Home and guide pages.
      for (const [name, links] of [
        [
          "Plugins",
          [
            "Plugin overview",
            "Versioning and compatibility",
            "Install without GitHub",
            "EDA Connector",
            "NSP Connector",
          ],
        ],
        [
          "Operate safely",
          ["Change topic configuration", "Run a latency probe", "Configure certificate trust"],
        ],
      ]) {
        const toggle = sectionToggle(name);
        await toggle.focus();
        await page.keyboard.press("Enter");
        const section = navigation.getByRole("navigation", { name, exact: true });
        const sectionState = page.locator(`#${await toggle.getAttribute("for")}`);
        await expect(sectionState).toBeChecked();
        for (const name of links) {
          const link = section.getByRole("link", { name, exact: true });
          await link.scrollIntoViewIfNeeded();
          await expect(link).toBeInViewport();
        }
        await toggle.focus();
        await page.keyboard.press("Enter");
        await expect(sectionState).not.toBeChecked();
      }

      await sectionToggle("Start here").focus();
      await page.keyboard.press("Enter");
      const connections = navigation.getByRole("link", { name: "Connect your Kafka", exact: true });
      await connections.focus();
      if (mobile) {
        await page.keyboard.press("Escape");
        await closedDrawer();
        await expect(trigger).toBeFocused();
        await openDrawer();
      }
      assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
      await accessible();
      await page.screenshot({ path: resolve(evidence, `home-navigation-${width}-${theme}.png`) });
      await connections.click();
      await page.waitForURL(/\/guide\/connections\//);
      await expect(page.locator(".md-path")).toBeVisible();
      if (mobile) {
        await closedDrawer();
        await openDrawer();
      } else {
        await expect(navigation).toBeInViewport();
      }
      await expect(connections).toHaveAttribute("aria-current", "page");
      await expect(connections).toHaveClass(/md-nav__link--active/u);
      await expect(sectionToggle("Plugins")).toBeVisible();
      await expect(sectionToggle("Operate safely")).toBeVisible();
      assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
      await accessible();
      await page.screenshot({ path: resolve(evidence, `navigation-${width}-${theme}.png`) });

      if (mobile) {
        await sidebar.getByRole("button", { name: "Close navigation", exact: true }).click();
        await closedDrawer();
        await expect(trigger).toBeFocused();
      }
      if (width < 960) {
        const mobileSearch = page.getByRole("button", { name: "Search", exact: true });
        await trigger.focus();
        for (let step = 0; step < 5; step++) {
          await page.keyboard.press("Tab");
          if (await mobileSearch.evaluate((element) => element === document.activeElement)) break;
        }
        await expect(mobileSearch).toBeFocused();
        await page.keyboard.press("Enter");
        await expect(page.getByRole("combobox", { name: "Search documentation" })).toBeVisible();
        await page.keyboard.press("Escape");
      }
      if (width === 1000 && theme === "light") {
        await openDrawer();
        await page.setViewportSize({ width: 1220, height: 900 });
        await expect(trigger).toBeHidden();
        await expect(drawer).not.toBeChecked();
        await expect(sidebar).toHaveJSProperty("inert", false);
        await expect(connections).toBeFocused();
        await page.setViewportSize({ width: 1219, height: 900 });
        await closedDrawer();
        await expect(trigger).toBeFocused();
        await openDrawer();
        await page.keyboard.press("Control+k");
        await closedDrawer();
        const search = page.getByRole("combobox", { name: "Search documentation" });
        await expect(search).toBeVisible();
        await expect(search).toBeFocused();
        await page.keyboard.press("Escape");
        await page.setViewportSize({ width, height: 900 });
      }
    }
  }
  await checkReleaseNavigation(page, base, evidence, accessible);
  await page.goto(base + "start/compatibility/");
  const table = page.getByRole("region", { name: /^Scrollable table: Capability,/u });
  await table.scrollIntoViewIfNeeded();
  await table.focus();
  await expect(table).toBeFocused();
  await page.keyboard.press("ArrowRight");
  await expect.poll(() => table.evaluate((element) => element.scrollLeft)).toBeGreaterThan(0);
}
