/* global process, document, window, console, innerWidth, navigator, URL */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { promisify } from "node:util";
import { chromium, expect } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";
import { checkNavigation, isReleasePreview } from "./navigation.mjs";
import { serveSite } from "./server.mjs";
import { checkMedia } from "./media.mjs";
import { prepareAccessibilityAudit, checkSearchKeyboard } from "./accessibility.mjs";

const root = resolve("dist/site");
const evidence = resolve(".artifacts/website");
const startedAt = new Date().toISOString();
const desktopRelease = /^desktop_release = "([^"]+)"$/mu.exec(
  await readFile("website/zensical.toml", "utf8"),
)?.[1];
assert(desktopRelease, "The site declares its documented StreamSkope release");
const developmentSource =
  JSON.parse(await readFile("package.json", "utf8")).version === "0.0.0-dev";
const published = process.env.STREAMSKOPE_DOCS_PUBLISH === "1";
const pluginPublications = published
  ? JSON.parse(await readFile(resolve(root, "plugin-publications.json"), "utf8"))
  : undefined;
const prefix = "/streamskope/";
const urlIndex = process.argv.indexOf("--url");
let base = urlIndex < 0 ? undefined : process.argv[urlIndex + 1];
let server;
let browser;
// Audit every rendered documentation page, including unlisted historical evidence.
const standaloneRoutes = new Set([
  "intro/index.html",
  "launch/index.html",
  "guide/eda/index.html",
  "guide/nsp/index.html",
]);
const routes = (await readdir(root, { recursive: true }))
  .filter((file) => file.endsWith("index.html") && !standaloneRoutes.has(file))
  .map((file) => file.slice(0, -"index.html".length))
  .sort();
try {
  await mkdir(evidence, { recursive: true });
  if (!base) {
    server = await serveSite(root, prefix);
    base = `http://127.0.0.1:${server.address().port}${prefix}`;
  }
  if (!base.endsWith("/")) base += "/";
  browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  context.setDefaultTimeout(10_000);
  const page = await context.newPage();
  for (const privateRoute of ["contribute/development/", "contribute/website/"]) {
    assert.equal(
      (await context.request.get(base + privateRoute)).status(),
      404,
      "Developer notes must not be published in the user documentation",
    );
  }
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("response", (response) => {
    if (response.url().startsWith(base) && response.status() >= 400) {
      errors.push(`${response.status()} ${response.url()}`);
    }
  });
  async function accessible() {
    await prepareAccessibilityAudit(page);
    const results = await new AxeBuilder({ page })
      .withTags(["wcag2a", "wcag2aa", "wcag21aa", "wcag22aa"])
      .analyze();
    assert.deepEqual(
      results.violations.map(({ id, nodes }) => ({
        id,
        targets: nodes.map((node) => node.target),
      })),
      [],
    );
  }
  async function browserQuickstart() {
    const quickstart = base + "start/containerlab/";
    await page.setViewportSize({ width: 1440, height: 1000 });
    await page.goto(quickstart);
    const content = page.locator(".md-content__inner");
    assert.deepEqual(
      (await content.getByRole("heading", { level: 2 }).allTextContents())
        .map((text) => text.replace(/\s*¶$/u, "").trim())
        .filter((text) => /^\d+\./u.test(text)),
      ["1. Install", "2. Create the vault", "3. Connect a broker"],
      "Browser onboarding has three outcome-oriented steps",
    );
    assert.doesNotMatch(
      await content.innerText(),
      /\b(?:Node(?:\.js)?|npm)\b|build from source|https?:\/\/[^\s]+:8080\b/iu,
      "Browser quickstart uses the installed host URL without a development toolchain",
    );
    const breadcrumbs = page.locator(".md-path");
    await expect(breadcrumbs).toBeVisible();
    await expect(breadcrumbs.getByRole("link", { name: "Home", exact: true })).toBeVisible();
    await expect(breadcrumbs).toContainText("Start here");
    const installer = page.getByRole("region", {
      name: "Browser workbench installation",
      exact: true,
    });
    await expect(installer).toHaveAttribute("data-desktop-release", desktopRelease);
    if (!published) {
      await expect(installer.locator("code")).toHaveCount(0);
      assert.doesNotMatch(
        await installer.innerHTML(),
        /releases\/download\/[^"\s]+\/install-browser-workbench\.sh|curl\s+-f/iu,
        "A source preview cannot invent a published installer command",
      );
    } else if ((await installer.locator("code").count()) > 0) {
      assert.equal(
        (await installer.locator("code").innerText()).trim(),
        "curl -fsSL https://github.com/asadarafat/streamskope/releases/latest/download/install-browser-workbench.sh | sudo -E bash",
        "Published onboarding uses the latest release installer entry point",
      );
    }
    await expect(installer.locator('a[href$="browser-deployment/"]')).toBeVisible();
    const everyday = content.locator('a[href$="browser-host/"]').first();
    await expect(everyday).toBeVisible();
    await everyday.click();
    await page.waitForURL(base + "guide/browser-host/");
    await expect(page.locator(".md-path")).toContainText("Operate safely");
    await expect(
      page
        .locator(".md-sidebar--primary")
        .getByRole("link", { name: "Use the browser workbench", exact: true }),
    ).toHaveAttribute("aria-current", "page");
    const advanced = content.locator('a[href$="browser-deployment/"]').first();
    await expect(advanced).toBeVisible();
    await advanced.click();
    await page.waitForURL(base + "guide/browser-deployment/");
    await expect(page.locator(".md-path")).toContainText("Advanced");
    await expect(page.locator('.md-sidebar--primary a[aria-current="page"]')).toHaveCount(1);
    await page.goto(base + "guide/recovery/");
    await content
      .getByRole("link", { name: "browser host backup and restore", exact: true })
      .click();
    await page.waitForURL(base + "guide/browser-host/#back-up-and-restore");
    await expect(page.locator('[id="back-up-and-restore"]')).toHaveCount(1);
    await page.goto(quickstart + "#back-up-and-restore");
    await page.waitForURL(quickstart + "#back-up-and-restore");
    await expect(page.locator('[id="back-up-and-restore"]')).toHaveCount(1);
    await content.getByRole("link", { name: "complete backup", exact: true }).click();
    await page.waitForURL(base + "guide/browser-deployment/#back-up-and-restore");
    await expect(page.locator('[id="back-up-and-restore"]')).toHaveCount(1);

    // Exercise the actual template and site enhancement with synthetic uploaded
    // asset metadata. This render fixture does not claim a release was published.
    const fixtureTag = "v0.10.1";
    const releaseRoot = `https://github.com/asadarafat/streamskope/releases/download/${fixtureTag}`;
    const installerUrl = `${releaseRoot}/install-browser-workbench.sh`;
    const command =
      "curl -fsSL https://github.com/asadarafat/streamskope/releases/latest/download/install-browser-workbench.sh | sudo -E bash";
    const fixture = {
      available: true,
      installer_available: true,
      tag: fixtureTag,
      version: fixtureTag.slice(1),
      release_url: `https://github.com/asadarafat/streamskope/releases/tag/${fixtureTag}`,
      checksum_url: `${releaseRoot}/SHA256SUMS`,
      installer_url: installerUrl,
      install_command: command,
      assets: [],
    };
    const python = resolve(
      ".cache/zensical",
      process.platform === "win32" ? "Scripts/python.exe" : "bin/python",
    );
    const { stdout: rendered } = await promisify(execFile)(python, [
      "-c",
      [
        "import json, sys",
        "from jinja2 import Environment, FileSystemLoader, StrictUndefined",
        "environment = Environment(loader=FileSystemLoader('website/overrides'), autoescape=True, undefined=StrictUndefined)",
        "environment.filters['url'] = lambda path: '../../' + path",
        "print(environment.get_template('partials/browser-installer.html').render(config={'extra': {'container_downloads': json.loads(sys.argv[1]), 'documentation': {'status': 'Published documentation'}}}))",
      ].join("\n"),
      JSON.stringify(fixture),
    ]);
    const regionPattern = /<section\b[^>]*class="sk-browser-installer"[^>]*>[\s\S]*?<\/section>/u;
    await page.route(quickstart, async (route) => {
      const response = await route.fetch();
      const html = await response.text();
      assert.equal(
        html.match(new RegExp(regionPattern.source, "gu"))?.length,
        1,
        "The fixture replaces exactly one installer region before site scripts initialize",
      );
      await route.fulfill({ response, body: html.replace(regionPattern, rendered.trim()) });
    });
    await context.grantPermissions(["clipboard-read", "clipboard-write"], {
      origin: new URL(base).origin,
    });
    try {
      for (const width of [1440, 390, 320]) {
        await page.setViewportSize({ width, height: 900 });
        for (const theme of ["light", "dark"]) {
          await page.goto(quickstart);
          const scheme = await page.locator("body").getAttribute("data-md-color-scheme");
          if ((scheme === "slate") !== (theme === "dark"))
            await page.locator(`label[title="Switch to ${theme} mode"]`).click();
          await expect(installer).toHaveAttribute("data-desktop-release", fixtureTag);
          await expect(installer.locator("code")).toHaveText(command);
          await expect(installer).toContainText("A new installation uses the latest release.");
          await expect(installer).toContainText("preserves the installed version and private data");
          await installer.scrollIntoViewIfNeeded();
          const copy = installer.getByRole("button", { name: "Copy to clipboard", exact: true });
          await expect(copy).toBeVisible();
          await copy.click();
          assert.equal(await page.evaluate(() => navigator.clipboard.readText()), command);
          assert(
            await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),
            "The long installer command cannot widen the page",
          );
          await accessible();
          await page.screenshot({
            path: resolve(evidence, `browser-installer-fixture-${width}-${theme}.png`),
          });
        }
      }
      await writeFile(
        resolve(evidence, "browser-installer-render-fixture.json"),
        JSON.stringify(
          { kind: "synthetic-template-fixture", tag: fixtureTag, installerUrl },
          null,
          2,
        ),
      );
    } finally {
      await page.unroute(quickstart);
    }
    // Qualify the actual source/release page too; fixture availability is isolated.
    for (const [route, label] of [
      [quickstart, "browser-quickstart"],
      [base + "guide/browser-host/", "browser-host"],
      [base + "guide/browser-deployment/", "browser-deployment"],
    ]) {
      for (const width of [1440, 390, 320]) {
        await page.setViewportSize({ width, height: 900 });
        for (const theme of ["light", "dark"]) {
          await page.goto(route);
          const scheme = await page.locator("body").getAttribute("data-md-color-scheme");
          if ((scheme === "slate") !== (theme === "dark"))
            await page.locator(`label[title="Switch to ${theme} mode"]`).click();
          if (route === quickstart) {
            await expect(installer).toHaveAttribute("data-desktop-release", desktopRelease);
            if (!published) await expect(installer.locator("code")).toHaveCount(0);
          }
          assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
          await accessible();
          await page.screenshot({ path: resolve(evidence, `${label}-${width}-${theme}.png`) });
        }
      }
    }
  }
  async function publishedPluginAvailability() {
    // Exercise the published layout even when PR qualification builds a preview.
    // These package values are a render fixture, not publication evidence.
    const sourcePlugins = await Promise.all(
      ["eda", "nsp"].map(async (system) => {
        const manifest = JSON.parse(await readFile(`plugins/${system}/manifest.json`, "utf8"));
        return {
          name: manifest.name,
          system,
          api: manifest.apiVersion,
          availability: "published",
          published_version: "0.1.2",
          published_api: manifest.apiVersion,
          checked_at: "2026-10-08T00:00:00Z",
        };
      }),
    );
    const python = resolve(
      ".cache/zensical",
      process.platform === "win32" ? "Scripts/python.exe" : "bin/python",
    );
    for (const plugin of ["eda", "nsp"]) {
      const { stdout: rendered } = await promisify(execFile)(python, [
        "-c",
        [
          "import json, sys",
          "from jinja2 import Environment, FileSystemLoader, StrictUndefined",
          "environment = Environment(loader=FileSystemLoader('website/overrides'), autoescape=True, undefined=StrictUndefined)",
          "environment.filters['url'] = lambda path: '../../' + path",
          "print(environment.get_template('partials/plugin-availability.html').render(config={'extra': {'source_plugins': json.loads(sys.argv[1]), 'documentation': {'status': 'Published documentation', 'revision': '1234567890abcdef1234567890abcdef12345678'}}}, page={'meta': {'plugin_scope': sys.argv[2]}}))",
        ].join("\n"),
        JSON.stringify(sourcePlugins),
        plugin,
      ]);
      const target = base + `plugins/${plugin}/`;
      const pattern =
        /<aside\b[^>]*class="sk-version sk-plugin-availability"[^>]*>[\s\S]*?<\/aside>/u;
      await page.route(target, async (route) => {
        const response = await route.fetch();
        const html = await response.text();
        assert.equal(html.match(new RegExp(pattern.source, "gu"))?.length, 1);
        await route.fulfill({ response, body: html.replace(pattern, rendered.trim()) });
      });
      try {
        for (const width of [390, 320]) {
          await page.setViewportSize({ width, height: 900 });
          for (const theme of ["light", "dark"]) {
            await page.goto(target);
            const scheme = await page.locator("body").getAttribute("data-md-color-scheme");
            if ((scheme === "slate") !== (theme === "dark"))
              await page.locator(`label[title="Switch to ${theme} mode"]`).click();
            const notice = page.getByRole("complementary", { name: "Plugin availability" });
            await notice.locator("summary").click();
            await expect(notice.locator("details")).toHaveAttribute("open", "");
            await expect(notice).toContainText("Catalog checked 2026-10-08T00:00:00Z");
            const compatibility = notice.getByRole("link", {
              name: "check compatibility",
              exact: true,
            });
            await compatibility.scrollIntoViewIfNeeded();
            await page.mouse.move(0, 0);
            const settledColor = async () => {
              await page.waitForFunction(() =>
                [...document.querySelectorAll(".sk-plugin-availability a")].every((element) =>
                  element.getAnimations().every((animation) => animation.playState !== "running"),
                ),
              );
              return compatibility.evaluate((element) => window.getComputedStyle(element).color);
            };
            if (theme === "dark")
              assert.equal(
                await settledColor(),
                "rgb(164, 188, 255)",
                "Dark normal links use the accessible site accent",
              );
            else await settledColor();
            assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
            await accessible();
            await page.screenshot({
              path: resolve(
                evidence,
                `plugin-availability-fixture-${plugin}-${width}-${theme}.png`,
              ),
            });
            const accent = theme === "dark" ? "rgb(164, 188, 255)" : "rgb(36, 79, 198)";
            await compatibility.hover();
            assert.equal(await settledColor(), accent, "Hover retains the accessible site accent");
            await compatibility.focus();
            await page.mouse.move(0, 0);
            assert.equal(
              await settledColor(),
              accent,
              "Keyboard focus retains the accessible site accent",
            );
          }
        }
      } finally {
        await page.unroute(target);
      }
    }
    await writeFile(
      resolve(evidence, "plugin-availability-render-fixture.json"),
      JSON.stringify(
        {
          kind: "synthetic-template-fixture",
          plugins: ["eda", "nsp"],
          widths: [390, 320],
          themes: ["light", "dark"],
        },
        null,
        2,
      ),
    );
  }
  for (const route of routes) {
    assert.equal((await page.goto(base + route)).status(), 200, route);
    await page.locator("h1").first().waitFor();
    assert(await page.locator("h1").first().innerText());
    const versionNotice = page.getByRole("complementary", { name: "Documentation version" });
    assert(await versionNotice.isVisible(), `${route}: release context is visible`);
    if (published) {
      await expect(versionNotice.locator("p").first()).toHaveText(
        `Documentation for StreamSkope ${desktopRelease}.`,
      );
    } else {
      await expect(versionNotice).toContainText("Development documentation");
    }
    const sourcePath = route ? route.replace(/\/$/u, "") : "index";
    const source = await readFile(`website/docs/${sourcePath}.md`, "utf8").catch(() =>
      readFile(`website/docs/${route}index.md`, "utf8"),
    );
    const metadata = /^---\r?\n([\s\S]*?)\r?\n---/u.exec(source)?.[1] ?? "";
    const unreleased = /^unreleased:\s*true\s*$/mu.test(metadata);
    const pluginScope = /^plugin_scope:\s*(all|eda|nsp)\s*$/mu.exec(metadata)?.[1];
    const releaseTag = /^releases\/(v[^/]+)\/$/u.exec(route)?.[1];
    const previewRelease =
      releaseTag !== undefined && isReleasePreview(releaseTag, desktopRelease, metadata);
    const expectedRelease = previewRelease
      ? `Release preview: ${releaseTag}`
      : !unreleased && releaseTag !== undefined && releaseTag !== desktopRelease
        ? `Historical release notes: ${releaseTag}`
        : desktopRelease;
    assert((await versionNotice.innerText()).includes(expectedRelease), route);
    assert.equal(
      (await versionNotice.innerText()).includes("Unreleased source changes"),
      releaseTag === undefined && (unreleased || developmentSource),
      `${route}: the version notice matches its documented release status`,
    );
    assert.equal(
      (await versionNotice.innerText()).includes("Documentation for StreamSkope"),
      published,
      `${route}: only a published release snapshot claims a documented StreamSkope version`,
    );
    if (published) {
      assert.doesNotMatch(
        await versionNotice.innerText(),
        /Unreleased source changes|version not yet assigned/u,
      );
    }
    const pluginNotice = page.getByRole("complementary", { name: "Plugin availability" });
    if (pluginScope !== undefined) {
      await expect(pluginNotice).toBeVisible();
      await expect(pluginNotice).toContainText("Desktop and plugins release independently");
      await expect(pluginNotice).toContainText("This guide describes source plugin behavior");
      const comparison = pluginNotice.locator("summary");
      await comparison.click();
      for (const plugin of pluginScope === "all" ? ["eda", "nsp"] : [pluginScope]) {
        const manifest = JSON.parse(await readFile(`plugins/${plugin}/manifest.json`, "utf8"));
        const row = pluginNotice.getByRole("row", { name: new RegExp(manifest.name, "u") });
        await expect(row).toContainText(`API ${manifest.apiVersion} source`);
        if (!published) await expect(row).toContainText("Availability not checked in this preview");
      }
      if (published) {
        await expect(versionNotice).toContainText(
          `Documentation for StreamSkope ${desktopRelease}`,
        );
        await expect(pluginNotice).toContainText("Catalog checked");
        await expect(pluginNotice).not.toContainText("Availability not checked in this preview");
      }
      await comparison.click();
    } else {
      await expect(pluginNotice).toHaveCount(0);
    }
    const portableDownloads = page.getByRole("region", { name: "Portable plugin downloads" });
    if (route === "plugins/offline/") {
      await expect(portableDownloads).toBeVisible();
      for (const plugin of ["eda", "nsp"]) {
        const manifest = JSON.parse(await readFile(`plugins/${plugin}/manifest.json`, "utf8"));
        const row = portableDownloads.getByRole("row", { name: new RegExp(manifest.name, "u") });
        const publication = pluginPublications?.packages.find((entry) => entry.id === manifest.id);
        const download = row.getByRole("link", { name: "Download signed file", exact: true });
        if (publication?.portable) {
          await expect(row).toHaveAttribute("data-portable-status", "published");
          await expect(download).toHaveAttribute("href", publication.portable.url);
          await expect(row).toContainText(publication.portable.name);
          await expect(row).toContainText(publication.portable.publisher);
          await expect(
            row.getByRole("link", { name: "Plugin release notes and assets" }),
          ).toHaveAttribute("href", publication.release_url);
          await expect(row).toContainText(`API ${publication.api}`);
        } else {
          await expect(download).toHaveCount(0);
          await expect(row).toHaveAttribute(
            "data-portable-status",
            published ? "unavailable" : "unchecked",
          );
          await expect(row).toContainText(
            published
              ? publication
                ? "No signed portable file for the selected compatible catalog package when checked"
                : "No compatible catalog package was found when checked"
              : "Portable availability not checked in this preview",
          );
        }
      }
    } else {
      await expect(portableDownloads).toHaveCount(0);
    }
    if (route === "plugins/versioning/") {
      for (const plugin of ["eda", "nsp"]) {
        const manifest = JSON.parse(await readFile(`plugins/${plugin}/manifest.json`, "utf8"));
        assert(
          (
            await page.getByRole("region", { name: "Source plugin declarations" }).innerText()
          ).includes(manifest.version === "0.0.0-dev" ? "Development" : manifest.version),
        );
      }
    }
    assert.doesNotMatch(await page.locator("body").innerText(), /Kubus|TopoViewer|FIELD GUIDE/i);
  }
  await browserQuickstart();
  await publishedPluginAvailability();
  for (const width of [1440, 390, 320]) {
    await page.setViewportSize({ width, height: 900 });
    await page.goto(base + "start/installation/#download");
    const downloads = page.locator(".sk-downloads");
    await expect(downloads).toHaveAttribute("data-desktop-release", desktopRelease);
    const version = desktopRelease.slice(1).split("+")[0];
    const releaseRoot = `https://github.com/asadarafat/streamskope/releases/download/${encodeURIComponent(desktopRelease)}`;
    for (const suffix of ["darwin-arm64.dmg", "win32-x64-Setup.exe", "linux-x64.AppImage"]) {
      const name = `StreamSkope-${version}-${suffix}`;
      const link = downloads.getByRole("link", { name, exact: true });
      await expect(link).toBeVisible();
      await expect(link).toHaveAttribute("href", `${releaseRoot}/${name}`);
    }
    await expect(downloads.getByRole("link", { name: "Download SHA256SUMS" })).toHaveAttribute(
      "href",
      `${releaseRoot}/SHA256SUMS`,
    );
    await expect(downloads.getByRole("link", { name: "Read the release notes" })).toHaveAttribute(
      "href",
      `../../releases/${desktopRelease}/`,
    );
    await accessible();
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
    await page.screenshot({ path: resolve(evidence, `downloads-${width}.png`) });
  }
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(base);
  assert.equal(
    await page.getByRole("link", { name: "Buy me a coffee", exact: true }).getAttribute("href"),
    "https://buymeacoffee.com/asadarafat",
  );
  assert.equal(await page.getByRole("link", { name: /transcript|Download.*video/i }).count(), 0);
  assert.doesNotMatch(
    await page.locator("body").innerText(),
    /Made with Zensical|Recorded in StreamSkope/,
  );
  await page.getByRole("link", { name: "Start here", exact: true }).click();
  await page.waitForURL(/\/start\/quickstart\//);
  assert.equal(
    await page.locator(".md-content__inner ol").first().locator(":scope > li").count(),
    4,
    "Desktop quickstart starts with four installation steps",
  );
  assert(
    (await page.locator(".md-content__inner ol li").count()) >= 6,
    "Quickstart offers installation, connection and reading steps",
  );
  assert.doesNotMatch(
    await page.locator(".md-content__inner").innerText(),
    /npm ci|npm run dev|Containerlab/,
    "Desktop onboarding must not require the development toolchain",
  );
  await page
    .locator(".md-content__inner")
    .getByRole("link", { name: "Download the installer", exact: true })
    .click();
  await page.waitForURL(/\/start\/installation\/#download$/);
  await page
    .locator(".md-content__inner")
    .getByRole("link", { name: /Continue the desktop quickstart/ })
    .click();
  await page.waitForURL(/\/start\/quickstart\/#2-connect-your-kafka$/);
  await page.locator('.md-content__inner a[href$="/guide/messages/"]').click();
  await page.waitForURL(/\/guide\/messages\//);
  await page.locator('.md-content__inner a[href$="/data-handling/"]').click();
  await page.waitForURL(/\/guide\/data-handling\//);
  await page.locator('.md-content__inner a[href$="/recovery/#find-your-application-data"]').click();
  await page.waitForURL(/\/guide\/recovery\/#find-your-application-data$/);
  await page.goto(base + "guide/connections/");
  await page
    .locator(".md-content__inner")
    .getByRole("link", { name: "Connect via EDA", exact: true })
    .click();
  await page.waitForURL(/\/plugins\/eda\//);
  await page
    .locator(".md-content__inner")
    .getByRole("link", { name: "Security and permissions", exact: true })
    .click();
  await page.waitForURL(/\/guide\/security\/#eda-authorization$/);
  for (const [plugin, heading] of [
    ["eda", "EDA Connector"],
    ["nsp", "NSP Connector"],
  ]) {
    await page.goto(base + "plugins/");
    await page
      .locator(".md-content__inner")
      .getByRole("link", { name: heading, exact: true })
      .click();
    await page.waitForURL(new RegExp(`/plugins/${plugin}/$`, "u"));
    assert.equal(await page.getByRole("heading", { level: 1, name: heading }).count(), 1);
    assert(await page.getByRole("heading", { name: /^Call flow/ }).isVisible());
    const callFlow = page
      .locator(".md-content__inner pre")
      .filter({ hasText: "Desktop plugin/host" });
    assert.equal(await callFlow.count(), 1, `${plugin}: one ASCII call flow is present`);
    assert(await callFlow.isVisible());
    for (const width of [1440, 390, 320]) {
      await page.setViewportSize({ width, height: 900 });
      assert(
        await callFlow
          .locator("code")
          .evaluate((element) => element.scrollWidth <= element.clientWidth),
        `${plugin}: mobile call flow fits without sideways scrolling`,
      );
      assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
      await accessible();
      await page.screenshot({ path: resolve(evidence, `plugin-${plugin}-${width}.png`) });
      if (width === 320) {
        const comparison = page
          .getByRole("complementary", { name: "Plugin availability" })
          .locator("summary");
        await comparison.click();
        assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
        await accessible();
        await page.screenshot({
          path: resolve(evidence, `plugin-availability-${plugin}-${width}.png`),
        });
        await comparison.click();
      }
    }
  }
  await page.goto(base + "plugins/");
  await page
    .locator(".md-content__inner")
    .getByRole("link", { name: "Versioning and compatibility", exact: true })
    .click();
  await page.waitForURL(/\/plugins\/versioning\/$/u);
  assert.equal(
    await page
      .getByRole("heading", { level: 1, name: "Plugin versioning and compatibility" })
      .count(),
    1,
  );
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: 900 });
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
    await accessible();
    await page.screenshot({ path: resolve(evidence, `plugin-versioning-${width}.png`) });
  }
  // Old bookmarks must keep their exact section when the guide moves to Plugins.
  for (const [plugin, fragment] of [
    ["eda", "start-temporary-capture"],
    ["eda", "stop-update-and-resume"],
    ["eda", "capture-from-eda"],
    ["nsp", "refresh-cancel-and-recover"],
  ]) {
    await page.goto(`${base}guide/${plugin}/#${fragment}`);
    await page.waitForURL(`${base}plugins/${plugin}/#${fragment}`);
    assert.equal(await page.locator(`[id="${fragment}"]`).count(), 1);
  }
  await checkNavigation(page, base, evidence, accessible);
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(base);
  await page.locator('label[title="Switch to light mode"]').click();
  assert.equal(
    await page
      .getByRole("heading", { level: 1, name: "Connect Kafka. Read your first message." })
      .count(),
    1,
    "Homepage leads with the first-use outcome",
  );
  assert(
    (await page.locator(".launch-frame").boundingBox()).y <
      (await page.locator(".product-shot").boundingBox()).y,
    "Approved intro precedes supporting screenshot",
  );
  for (const [route, subjects] of [
    ["", ["messages"]],
    ["guide/messages/", ["topics", "messages"]],
    ["guide/operations/", ["consumers"]],
    ["guide/connections/", ["profiles"]],
  ]) {
    await page.goto(base + route);
    for (const theme of ["light", "dark"]) {
      if (theme === "dark") await page.locator('label[title="Switch to dark mode"]').click();
      for (const subject of subjects) {
        const suffix = theme === "dark" ? "-dark" : "";
        const visible = page.locator(`.md-content__inner img[data-sk-light="${subject}.png"]`);
        assert.equal(await visible.count(), 1, `${route}: one ${subject} image`);
        await expect(visible).toHaveAttribute(
          "src",
          new RegExp(`/${subject}${suffix}\\.png$`, "u"),
        );
        assert(await visible.isVisible());
        await visible.scrollIntoViewIfNeeded();
        await visible.evaluate((img) => img.decode());
        assert(await visible.evaluate((img) => img.complete && img.naturalWidth > 0));
        assert(await visible.getAttribute("width"));
        assert(await visible.getAttribute("height"));
      }
    }
    await page.reload();
    assert.equal(await page.locator("body").getAttribute("data-md-color-scheme"), "slate");
    for (const subject of subjects)
      assert(await page.locator(`img[src$="/${subject}-dark.png"]`).isVisible());
    await page.locator('label[title="Switch to light mode"]').click();
  }
  await page.goto(base);
  await page.locator(".launch-player").scrollIntoViewIfNeeded();
  const themedFilm = await (await page.locator(".launch-player").elementHandle()).contentFrame();
  assert.equal(
    await themedFilm.locator("video").count(),
    1,
    "Homepage intro uses an actual MP4 player",
  );
  await page.locator('label[title="Switch to dark mode"]').click();
  await themedFilm.waitForFunction(() => document.documentElement.dataset.theme === "dark");
  assert(
    (await themedFilm.locator("video").getAttribute("src")).endsWith("streamskope-intro-dark.mp4"),
  );
  await page.locator('label[title="Switch to light mode"]').click();

  if (process.argv.includes("--media")) await checkMedia(base, evidence);
  const standalone = await context.newPage();
  await standalone.emulateMedia({ colorScheme: "dark" });
  await standalone.goto(base + "intro/");
  await standalone.waitForFunction(() => document.documentElement.dataset.theme === "dark");
  await standalone.emulateMedia({ colorScheme: "light" });
  await standalone.waitForFunction(() => document.documentElement.dataset.theme === "light");
  await standalone.close();
  await page.goto(base);
  await page.locator(".md-search__button").click();
  const search = page.getByRole("combobox", { name: "Search documentation" });
  await search.fill("Secret retrieval");
  // Record the actual search audit; there are no rule or widget exemptions.
  await prepareAccessibilityAudit(page);
  const searchAudit = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21aa", "wcag22aa"])
    .analyze();
  await writeFile(
    resolve(evidence, "search-accessibility.json"),
    JSON.stringify(searchAudit.violations, null, 2),
  );
  assert.deepEqual(
    searchAudit.violations.map(({ id }) => id),
    [],
  );
  await page
    .getByRole("option", { name: /Reuse connection secrets/i })
    .first()
    .click();
  await page.waitForURL(/\/guide\/secret-retrieval\//);
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: 900 });
    for (const theme of ["light", "dark"]) {
      await page.goto(base);
      const scheme = await page.locator("body").getAttribute("data-md-color-scheme");
      if ((scheme === "slate") !== (theme === "dark"))
        await page.locator(`label[title="Switch to ${theme} mode"]`).click();
      const opener = page.locator(width < 960 ? ".sk-search-trigger" : ".md-search__button");
      await opener.click();
      await checkSearchKeyboard(page, accessible);
      await expect(opener).toBeFocused();
      await opener.click();
      const query = page.getByRole("combobox", { name: "Search documentation" });
      await page.keyboard.press("Escape");
      await expect(query).toHaveCount(0);
      await expect(opener).toBeFocused();
      await opener.click();
      await query.fill("Secret retrieval");
      await expect(
        page.getByRole("option", { name: /Reuse connection secrets/i }).first(),
      ).toBeVisible();
      await query.press("Enter");
      await page.waitForURL(/\/guide\/secret-retrieval\//);
    }
  }
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(base);
  await page.locator('label[title="Switch to light mode"]').click();
  await accessible();
  await page.evaluate(() => {
    const host = document.createElement("div");
    host.id = "first-party-audit-fixture";
    host.attachShadow({ mode: "open" }).innerHTML =
      '<button style="width:40px;height:40px"></button>';
    document.body.append(host);
  });
  const regressionAudit = await new AxeBuilder({ page }).withRules(["button-name"]).analyze();
  assert(
    regressionAudit.violations.some(({ nodes }) =>
      nodes.some(({ target }) => JSON.stringify(target).includes("first-party-audit-fixture")),
    ),
    "An unrelated shadow-root accessibility defect must fail qualification",
  );
  await page.locator("#first-party-audit-fixture").evaluate((element) => element.remove());
  await page.screenshot({ path: resolve(evidence, "home-light.png"), fullPage: true });
  await page.locator('label[title="Switch to dark mode"]').click();
  assert.equal(await page.locator("body").getAttribute("data-md-color-scheme"), "slate");
  await accessible();
  await page.screenshot({ path: resolve(evidence, "home-dark.png"), fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  assert(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth));
  await page.locator(".launch-player").scrollIntoViewIfNeeded();
  const playerFrame = page.frameLocator(".launch-player");
  assert(
    await playerFrame
      .locator("video")
      .evaluate((element) => element.getBoundingClientRect().bottom <= window.innerHeight),
    "Mobile launch controls must not be clipped",
  );
  await page.screenshot({ path: resolve(evidence, "home-mobile.png"), fullPage: true });
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.getByRole("button", { name: "Search", exact: true }).filter({ visible: true }).click();
  await page.getByRole("combobox").fill("Secret retrieval");
  await page
    .getByRole("option", { name: /Reuse connection secrets/i })
    .first()
    .waitFor();
  await page.keyboard.press("Escape");
  const imageContext = await browser.newContext({ colorScheme: "light" });
  const imagePage = await imageContext.newPage();
  const imageRequests = [];
  imagePage.on("request", (request) => {
    if (request.url().endsWith(".png")) imageRequests.push(request.url());
  });
  await imagePage.goto(base + "guide/messages/");
  for (const img of await imagePage.locator("img[data-sk-light]").all()) {
    await img.scrollIntoViewIfNeeded();
    await img.evaluate((element) => element.decode());
  }
  assert(imageRequests.some((url) => url.endsWith("messages.png")));
  assert(
    !imageRequests.some((url) => url.endsWith("-dark.png")),
    "Hidden image themes are not downloaded",
  );
  await imageContext.close();
  await page.goto(base + "launch/");
  await page.waitForURL(/\/intro\//);
  assert.deepEqual(errors, []);
  await writeFile(
    resolve(evidence, "browser-checks.json"),
    JSON.stringify(
      {
        schemaVersion: 1,
        outcome: "passed",
        startedAt,
        completedAt: new Date().toISOString(),
        routes: routes.length,
        media: process.argv.includes("--media") ? "passed" : "skipped",
      },
      null,
      2,
    ) + "\n",
  );
  console.log(
    `Documentation browser checks passed: ${routes.length} routes, search, themes, mobile, accessibility; media=${process.argv.includes("--media") ? "passed" : "skipped"}. Evidence: ${evidence}`,
  );
} finally {
  await browser?.close();
  if (server) await new Promise((done) => server.close(done));
}
