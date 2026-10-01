/* global document, innerWidth */
import assert from "node:assert/strict";
import { resolve } from "node:path";
import { expect } from "@playwright/test";

export async function checkNavigation(page, base, evidence, accessible) {
  for (const width of [1440, 1000, 768, 390, 320]) {
    await page.setViewportSize({ width, height: 900 });
    for (const theme of ["light", "dark"]) {
      await page.goto(base);
      const scheme = await page.locator("body").getAttribute("data-md-color-scheme");
      if ((scheme === "slate") !== (theme === "dark"))
        await page.locator(`label[title="Switch to ${theme} mode"]`).click();

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
          ["Plugin overview", "Versioning and compatibility", "EDA Capture", "NSP Capture"],
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
  await page.goto(base + "start/compatibility/");
  const table = page.getByRole("region", { name: /^Scrollable table: Capability,/u });
  await table.scrollIntoViewIfNeeded();
  await table.focus();
  await expect(table).toBeFocused();
  await page.keyboard.press("ArrowRight");
  await expect.poll(() => table.evaluate((element) => element.scrollLeft)).toBeGreaterThan(0);
}
