import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { join, resolve } from "node:path";

import { expect, test } from "@playwright/test";
import { build, preview, type PreviewServer } from "vite";

test("boots the production renderer with both provider choices and no module initialization errors", async ({
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
    const provider = page.getByRole("combobox", { name: "Messaging provider" });
    await expect(provider).toBeVisible();
    await expect(provider).toContainText("Kafka");
    await expect(page.getByRole("banner", { name: "StreamSkope application bar" })).toBeVisible();
    await provider.click();
    await expect(page.getByRole("option", { name: "NATS", exact: true })).toBeVisible();
    await expect(page.getByRole("option", { name: "Kafka", exact: true })).toBeVisible();
    // This smoke owns no messaging host. Real provider-switch cleanup is qualified separately.
    await page.keyboard.press("Escape");
    await expect(provider).toContainText("Kafka");
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
