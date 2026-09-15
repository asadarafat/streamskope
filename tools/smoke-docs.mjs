/* global process, URL, document, window, console, innerWidth */
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve, extname, sep } from "node:path";
import { chromium, firefox } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";

const root = resolve("dist/site");
const evidence = resolve(".artifacts/website");
const prefix = "/streamskope/";
const urlIndex = process.argv.indexOf("--url");
let base = urlIndex < 0 ? undefined : process.argv[urlIndex + 1];
let server;
let browser;
let videoBrowser;
const mime = {
  ".html": "text/html",
  ".js": "text/javascript",
  ".css": "text/css",
  ".json": "application/json",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".mp3": "audio/mpeg",
  ".mp4": "video/mp4",
};
const routes = [
  "",
  "start/installation/",
  "start/quickstart/",
  "guide/connections/",
  "guide/secret-retrieval/",
  "guide/messages/",
  "guide/launch-film/",
  "guide/operations/",
  "guide/schema-registry/",
  "guide/governance/",
  "guide/troubleshooting/",
  "about/",
];
// Zensical 0.0.62 creates its search shadow root even while closed. These known
// rules apply only inside that upstream shadow DOM, never to our page/player.
const knownSearchRules = new Set([
  "aria-required-attr",
  "button-name",
  "scrollable-region-focusable",
  "color-contrast",
]);
function unexpectedAccessibility(violations) {
  return violations.filter(
    ({ id, nodes }) =>
      !knownSearchRules.has(id) || nodes.some((node) => !Array.isArray(node.target[0])),
  );
}
try {
  await mkdir(evidence, { recursive: true });
  if (!base) {
    server = createServer(async (request, response) => {
      try {
        const path = decodeURIComponent(new URL(request.url, "http://localhost").pathname);
        assert(path.startsWith(prefix));
        const file = resolve(
          root,
          path.slice(prefix.length) + (path.endsWith("/") ? "index.html" : ""),
        );
        assert(file.startsWith(root + sep));
        response.setHeader("Content-Type", mime[extname(file)] ?? "application/octet-stream");
        const bytes = await readFile(file);
        response.setHeader("Accept-Ranges", "bytes");
        const range = request.headers.range?.match(/^bytes=(\d+)-(\d*)$/);
        if (range) {
          const start = Number(range[1]);
          const end = Math.min(range[2] ? Number(range[2]) : bytes.length - 1, bytes.length - 1);
          if (start > end || start >= bytes.length) {
            response.writeHead(416, { "Content-Range": `bytes */${bytes.length}` }).end();
            return;
          }
          response.writeHead(206, {
            "Content-Range": `bytes ${start}-${end}/${bytes.length}`,
            "Content-Length": end - start + 1,
          });
          response.end(bytes.subarray(start, end + 1));
        } else {
          response.setHeader("Content-Length", bytes.length);
          response.end(bytes);
        }
      } catch {
        response.writeHead(404).end("Not found");
      }
    });
    await new Promise((done) => server.listen(0, "127.0.0.1", done));
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
    const results = await new AxeBuilder({ page })
      .withTags(["wcag2a", "wcag2aa", "wcag21aa", "wcag22aa"])
      .analyze();
    assert.deepEqual(
      unexpectedAccessibility(results.violations).map(({ id, nodes }) => ({
        id,
        targets: nodes.map((node) => node.target),
      })),
      [],
    );
  }
  for (const route of routes) {
    assert.equal((await page.goto(base + route)).status(), 200, route);
    await page.locator("h1").first().waitFor();
    assert(await page.locator("h1").first().innerText());
    assert.doesNotMatch(await page.locator("body").innerText(), /Kubus|TopoViewer|FIELD GUIDE/i);
  }
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
    await page.locator(".md-content ol").first().locator(":scope > li").count(),
    4,
    "Launch steps remain in one ordered list across code blocks",
  );
  assert(
    (await page.locator(".md-content ol li").count()) >= 6,
    "Quickstart offers executable steps",
  );
  await page.locator('.md-content a[href$="/guide/messages/"]').click();
  await page.waitForURL(/\/guide\/messages\//);
  await page.goto(base);
  assert.equal(await page.getByRole("button", { name: "Browse docs", exact: true }).count(), 1);
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: 900 });
    for (const theme of ["light", "dark"]) {
      await page.goto(base);
      const scheme = await page.locator("body").getAttribute("data-md-color-scheme");
      if ((scheme === "slate") !== (theme === "dark"))
        await page.locator(`label[title="Switch to ${theme} mode"]`).click();
      const trigger = page.getByRole("button", { name: "Browse docs", exact: true });
      await trigger.focus();
      await page.keyboard.press("Enter");
      const directory = page.getByRole("navigation", { name: "Documentation directory" });
      await directory.getByRole("link", { name: "Connect your Kafka", exact: true }).waitFor();
      assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
      await accessible();
      await page.screenshot({ path: resolve(evidence, `directory-${width}-${theme}.png`) });
      await page.keyboard.press("Escape");
      assert(await trigger.evaluate((element) => element === document.activeElement));
      assert(!(await directory.isVisible()));
      await trigger.click();
      await directory.getByRole("link", { name: "Connect your Kafka", exact: true }).click();
      await page.waitForURL(/\/guide\/connections\//);
      await page.getByRole("button", { name: "Browse docs", exact: true }).click();
      assert.equal(
        await page
          .getByRole("navigation", { name: "Documentation directory" })
          .getByRole("link", { name: "Connect your Kafka", exact: true })
          .getAttribute("aria-current"),
        "page",
      );
    }
  }
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
        const visible = page.locator(
          `.md-content img[src$="/${subject}${suffix}.png#only-${theme}"]`,
        );
        assert.equal(await visible.count(), 1, `${route}: ${subject} ${theme} capture exists`);
        assert(await visible.isVisible(), `${route}: ${subject} follows ${theme} theme`);
        assert(await visible.evaluate((img) => img.complete && img.naturalWidth > 0));
        const hidden = page.locator(
          `.md-content img[src$="#only-${theme === "dark" ? "light" : "dark"}"]`,
        );
        for (const img of await hidden.all()) assert.equal(await img.isVisible(), false);
      }
    }
    await page.reload();
    assert.equal(await page.locator("body").getAttribute("data-md-color-scheme"), "slate");
    for (const subject of subjects)
      assert(await page.locator(`img[src$="/${subject}-dark.png#only-dark"]`).isVisible());
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

  // Firefox supplies H.264 decoding on the ARM64 test host; do not mock media.
  videoBrowser = await firefox.launch({ headless: true });
  const videoPage = await videoBrowser.newPage({ viewport: { width: 1280, height: 900 } });
  videoPage.setDefaultTimeout(20_000);
  await videoPage.goto(base);
  await videoPage.locator(".launch-player").scrollIntoViewIfNeeded();
  const film = await (await videoPage.locator(".launch-player").elementHandle()).contentFrame();
  await film.waitForFunction(() => document.querySelector("video")?.readyState >= 2);
  const video = film.locator("video");
  assert.equal(await video.evaluate((v) => v.paused), true);
  assert.deepEqual(
    await video.evaluate((v) => [v.videoWidth, v.videoHeight, Math.round(v.duration)]),
    [3840, 2160, 36],
  );
  await video.evaluate((v) => {
    window.checkedVideo = v;
    v.currentTime = 23.25;
    v.muted = true;
    v.volume = 0.35;
    v.playbackRate = 1.25;
  });
  await videoPage.locator('label[title="Switch to dark mode"]').click();
  await film.waitForFunction(() => {
    const v = document.querySelector("video");
    return (
      v.currentSrc.endsWith("streamskope-intro-dark.mp4") &&
      v.readyState >= 2 &&
      Math.abs(v.currentTime - 23.25) < 0.15
    );
  });
  assert.deepEqual(
    await video.evaluate((v) => [
      v.paused,
      v.muted,
      v.volume,
      v.playbackRate,
      v === window.checkedVideo,
    ]),
    [true, true, 0.35, 1.25, true],
  );
  assert.equal(await film.getByRole("link").count(), 0, "Player has no resource links");
  await film.locator("video").screenshot({ path: resolve(evidence, "intro-mp4-dark.png") });
  await video.evaluate((v) => v.play());
  await videoPage.locator('label[title="Switch to light mode"]').click();
  await film.waitForFunction(() => {
    const v = document.querySelector("video");
    return (
      v.currentSrc.endsWith("streamskope-intro-light.mp4") && !v.paused && v.currentTime > 23.25
    );
  });
  await video.evaluate((v) => v.pause());
  for (const title of ["Switch to dark mode", "Switch to light mode", "Switch to dark mode"])
    await videoPage.locator(`label[title="${title}"]`).click();
  await film.waitForFunction(() => {
    const v = document.querySelector("video");
    return (
      v.currentSrc.endsWith("streamskope-intro-dark.mp4") &&
      v.readyState >= 2 &&
      v.paused &&
      v.currentTime > 23
    );
  });
  await videoPage.reload();
  await videoPage.locator(".launch-player").scrollIntoViewIfNeeded();
  const restored = await (await videoPage.locator(".launch-player").elementHandle()).contentFrame();
  await restored.waitForFunction(() => {
    const v = document.querySelector("video");
    return v.currentSrc.endsWith("streamskope-intro-dark.mp4") && v.readyState >= 2 && v.paused;
  });
  for (const width of [1440, 390]) {
    await videoPage.setViewportSize({ width, height: width === 390 ? 844 : 1000 });
    for (const theme of ["dark", "light"]) {
      const scheme = await videoPage.locator("body").getAttribute("data-md-color-scheme");
      if ((scheme === "slate") !== (theme === "dark"))
        await videoPage.locator(`label[title="Switch to ${theme} mode"]`).click();
      await restored.waitForFunction((theme) => {
        const v = document.querySelector("video");
        return v.currentSrc.endsWith(`streamskope-intro-${theme}.mp4`) && v.readyState >= 2;
      }, theme);
      await restored.locator("video").evaluate((v) => {
        v.currentTime = 1;
      });
      await restored.waitForFunction(() => {
        const v = document.querySelector("video");
        return !v.seeking && Math.abs(v.currentTime - 1) < 0.1;
      });
      await videoPage.evaluate(() => window.scrollTo(0, 0));
      // Allow the sticky header to return from its scrolled state before capture.
      await videoPage.waitForTimeout(500);
      assert(await videoPage.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
      await videoPage.screenshot({
        path: resolve(evidence, `home-${width}-${theme}-final.png`),
        fullPage: true,
      });
    }
  }
  const failedMedia = await videoBrowser.newPage({ colorScheme: "dark" });
  await failedMedia.route("**/streamskope-intro-dark.mp4", (route) =>
    route.fulfill({ status: 404, body: "Unavailable" }),
  );
  await failedMedia.goto(base + "intro/");
  await failedMedia.getByRole("status").filter({ hasText: "could not be played" }).waitFor();
  assert.equal(await failedMedia.getByRole("link").count(), 0);
  await videoBrowser.close();
  videoBrowser = undefined;
  const standalone = await context.newPage();
  await standalone.emulateMedia({ colorScheme: "dark" });
  await standalone.goto(base + "intro/");
  await standalone.waitForFunction(() => document.documentElement.dataset.theme === "dark");
  await standalone.emulateMedia({ colorScheme: "light" });
  await standalone.waitForFunction(() => document.documentElement.dataset.theme === "light");
  await standalone.close();
  await page.goto(base);
  await page.locator(".md-search__button").click();
  const search = page.getByRole("combobox", { name: "" });
  await search.fill("Secret retrieval");
  // Record the pinned upstream widget's defects; do not claim full WCAG AA.
  const searchAudit = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21aa", "wcag22aa"])
    .analyze();
  await writeFile(
    resolve(evidence, "search-accessibility.json"),
    JSON.stringify(searchAudit.violations, null, 2),
  );
  assert.deepEqual(
    unexpectedAccessibility(searchAudit.violations).map(({ id }) => id),
    [],
  );
  if (searchAudit.violations.length)
    console.warn(
      "Known Zensical search accessibility findings recorded in search-accessibility.json; the search dialog is not claimed WCAG AA.",
    );
  await page
    .getByRole("link", { name: /Reuse connection secrets/i })
    .first()
    .click();
  await page.waitForURL(/\/guide\/secret-retrieval\//);
  await page.goto(base);
  await accessible();
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
  await page.locator('label[for="__search"]').click();
  await page.getByRole("combobox").fill("Secret retrieval");
  await page
    .getByRole("link", { name: /Reuse connection secrets/i })
    .first()
    .waitFor();
  await page.keyboard.press("Escape");
  await page.goto(base + "launch/");
  await page.waitForURL(/\/intro\//);
  assert.deepEqual(errors, []);
  console.log(
    `Documentation browser checks passed: ${routes.length} routes, search, themes, mobile, accessibility and launch playback. Evidence: ${evidence}`,
  );
} finally {
  await videoBrowser?.close();
  await browser?.close();
  if (server) await new Promise((done) => server.close(done));
}
