/* global document */
import { expect } from "@playwright/test";

export async function prepareAccessibilityAudit(page) {
  await page.locator('[data-sk-search="ready"]').waitFor({ state: "attached" });
  // Inspect a settled animation frame; every actual accessibility finding fails.
  await page.waitForFunction(() => {
    const modal = document
      .querySelector('[data-sk-search="ready"]')
      ?.shadowRoot?.querySelector(".l");
    return (
      modal &&
      modal.getAnimations().every((animation) => animation.playState !== "running") &&
      [...document.querySelectorAll(".md-sidebar--primary, .md-overlay")].every((element) =>
        element
          .getAnimations({ subtree: true })
          .every((animation) => animation.playState !== "running"),
      )
    );
  });
}

export async function checkSearchKeyboard(page, accessible) {
  const input = page.getByRole("combobox", { name: "Search documentation" });
  const results = page.getByRole("listbox", { name: "Search results" });
  await input.fill("Secret retrieval");
  await expect(results.getByRole("option").first()).toBeVisible();
  await expect(input).toHaveAttribute("aria-expanded", "true");
  await expect(input).toHaveAttribute("aria-controls", await results.getAttribute("id"));
  const first = await input.getAttribute("aria-activedescendant");
  await input.press("ArrowDown");
  await expect(input).not.toHaveAttribute("aria-activedescendant", first);
  await expect(input).toHaveAttribute(
    "aria-activedescendant",
    await results.getByRole("option", { selected: true }).getAttribute("id"),
  );
  await accessible();

  const filterButton = page.getByRole("button", { name: "Search filters", exact: true });
  await filterButton.focus();
  await page.keyboard.press("Enter");
  await expect(filterButton).toHaveAttribute("aria-expanded", "true");
  const filters = page.getByRole("region", { name: "Search filters", exact: true });
  await expect(filters).toBeVisible();
  await accessible();
  await filters.focus();
  await page.keyboard.press("Tab");
  const close = page.getByRole("button", { name: "Close search" });
  await expect(close).toBeFocused();
  await page.keyboard.press("Shift+Tab");
  await expect(filters).toBeFocused();
  await filterButton.focus();
  await page.keyboard.press("Enter");
  await expect(filterButton).toHaveAttribute("aria-expanded", "false");
  await expect(filters).toHaveCount(0);

  await input.fill("no-such-document-4f0719");
  await expect(results.getByRole("option")).toHaveCount(0);
  await expect(input).toHaveAttribute("aria-expanded", "false");
  await expect(input).not.toHaveAttribute("aria-activedescendant", /.+/);
  await accessible();
  await close.focus();
  await page.keyboard.press("Enter");
  await expect(input).toHaveCount(0);
}
