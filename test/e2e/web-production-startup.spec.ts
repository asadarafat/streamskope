import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { join, resolve } from "node:path";

import { expect, test } from "@playwright/test";
import { build, preview, type PreviewServer } from "vite";

test("boots the production renderer with shared connection choices and no module initialization errors", async ({
  page,
}, info) => {
  test.setTimeout(120_000);
  await mkdir(resolve("dist"), { recursive: true });
  const directory = await mkdtemp(join(resolve("dist"), "renderer-startup-"));
  const errors: string[] = [];
  let server: PreviewServer | undefined;
  page.on("pageerror", (error) => errors.push(error.message));
  try {
    // Execute the exact minified production output. Development transforms do not exercise its chunk graph.
    const configFile = resolve("config/vite.config.ts");
    await build({ configFile, logLevel: "silent", build: { outDir: directory } });
    server = await preview({
      configFile,
      logLevel: "silent",
      build: { outDir: directory },
      preview: { host: "127.0.0.1", port: 0, strictPort: false, open: false },
    });
    const address = server.httpServer.address();
    if (address === null || typeof address === "string")
      throw new Error("The owned production preview did not expose a TCP port.");
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(`http://127.0.0.1:${String(address.port)}/`);
    await expect(page.getByRole("combobox", { name: "Messaging provider" })).toHaveCount(0);
    await expect(page.getByRole("banner", { name: "StreamSkope application bar" })).toBeVisible();
    const profiles = page.getByTestId("connection-profiles-grid");
    await expect(profiles).toBeVisible();
    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await expect(page.getByRole("menuitem", { name: "NATS server", exact: true })).toBeVisible();
    await expect(page.getByRole("menuitem", { name: "Kafka broker", exact: true })).toBeVisible();
    // This smoke owns no messaging host. Real connection and cleanup are qualified separately.
    await page.keyboard.press("Escape");
    await expect(profiles).toBeVisible();
    expect(errors).toEqual([]);
  } finally {
    await info.attach("production-renderer-errors", {
      body: JSON.stringify(errors),
      contentType: "application/json",
    });
    // Close the page before removing the files served to its module loader.
    await page.close();
    if (server !== undefined)
      await new Promise<void>((accept, reject) =>
        server!.httpServer.close((error) => (error === undefined ? accept() : reject(error))),
      );
    await rm(directory, { recursive: true, force: true });
  }
});
