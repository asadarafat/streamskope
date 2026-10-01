/* global document, window, innerWidth */
import assert from "node:assert/strict";
import { resolve } from "node:path";
import { firefox } from "@playwright/test";
export async function checkMedia(base, evidence) {
  // Firefox supplies H.264 decoding on the ARM64 test host; do not mock media.
  const videoBrowser = await firefox.launch({ headless: true });
  try {
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
    const restored = await (
      await videoPage.locator(".launch-player").elementHandle()
    ).contentFrame();
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
  } finally {
    await videoBrowser.close();
  }
}
