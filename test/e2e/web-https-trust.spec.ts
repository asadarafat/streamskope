import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import { createServer } from "node:net";
import { resolve } from "node:path";
import { promisify } from "node:util";

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/electron/main/kafka-backend";
import { launchWebDevelopment, type RunningWebDevelopment } from "../../src/platform/dev-host";
import { createHttpsTrustFixture } from "../support/https-trust-fixture";
import { loadFixtureConnection } from "../support/kafka-fixture";
import { configureLocalConnection } from "../support/web-profile-workflow";

test.use({ trace: "off" });

async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((done) => server.listen(0, "127.0.0.1", done));
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("Missing fixture port");
  await new Promise<void>((done, reject) =>
    server.close((error) => (error ? reject(error) : done())),
  );
  return address.port;
}

for (const [kind, extraction, width, colorScheme] of [
  ["pem", "raw", 640, "dark"],
  ["pem", "json-pem", 800, "light"],
  ["pkcs12", "raw", 1440, "dark"],
] as const) {
  test(`HTTPS ${kind}/${extraction} review, recovery, test and save at ${width}`, async ({
    page,
  }, info) => {
    test.setTimeout(120000);
    const kafka = await loadFixtureConnection();
    const ca = await readFile(kafka.caPath);
    const bytes =
      kind === "pem"
        ? ca
        : (
            await promisify(execFile)(
              "openssl",
              ["pkcs12", "-export", "-nokeys", "-in", kafka.caPath, "-passout", "pass:password"],
              { encoding: "buffer" },
            )
          ).stdout;
    let mode: "normal" | "hang" | "malformed" | "overflow" = "normal";
    const fixture = await createHttpsTrustFixture((request, response) => {
      if (request.headers.authorization !== "Bearer fixture-api-token") {
        response.writeHead(401);
        response.end("redacted-fixture-body");
        return;
      }
      if (request.url === "/password") {
        response.end('{"password":"password"}');
        return;
      }
      if (mode === "hang") {
        response.writeHead(200);
        response.flushHeaders();
        return;
      }
      if (mode === "malformed") {
        response.end("invalid certificate response");
        return;
      }
      if (mode === "overflow") {
        response.writeHead(200, { "content-length": String(13 * 1024 * 1024) });
        response.end();
        return;
      }
      response.end(
        extraction === "raw" ? bytes : JSON.stringify({ certificate: ca.toString("utf8") }),
      );
    });
    let launch: RunningWebDevelopment | undefined;
    try {
      const backend = createKafkaBackend();
      expect(
        await backend.execute({
          command: "recipes.create",
          id: "fixture-recipe",
          version: HOST_PROTOCOL_VERSION,
          payload: {
            name: "Fixture HTTPS",
            method: "https",
            syntax: "named-v1",
            kind,
            parameters: [],
            timeoutSeconds: 30,
            https: {
              authentication: "bearer",
              material: {
                url: `${fixture.origin}/material`,
                headers: [],
                query: [],
                extraction:
                  extraction === "raw"
                    ? { mode: "raw" }
                    : { mode: "json-pem", pointer: "/certificate" },
              },
              password:
                kind === "pem"
                  ? { source: "none" }
                  : {
                      source: "https",
                      request: {
                        url: `${fixture.origin}/password`,
                        headers: [],
                        query: [],
                        extraction: { mode: "json", pointer: "/password" },
                      },
                    },
            },
          },
        }),
      ).toMatchObject({ ok: true });
      launch = await launchWebDevelopment({
        backend,
        hostPort: await freePort(),
        rendererPort: await freePort(),
        rendererRoot: resolve(process.cwd()),
      });
      await page.setViewportSize({ width, height: 1000 });
      await page.emulateMedia({ colorScheme, reducedMotion: "reduce" });
      await page.goto(launch.browserUrl);
      await configureLocalConnection(page);
      const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
      await editor.getByRole("button", { name: "Secret Retrieval Profile", exact: true }).click();
      await editor.getByRole("combobox", { name: "Use profile", exact: true }).click();
      await page.getByRole("option", { name: "Fixture HTTPS", exact: true }).click();
      await expect(editor.getByText(`API origin: ${fixture.origin}`)).toBeVisible();
      await expect(editor.getByLabel("SSH host", { exact: true })).toHaveCount(0);
      const token = editor.getByLabel("API bearer token", { exact: false });
      await token.fill("fixture-api-token");
      const acquire = editor.getByRole("button", { name: "Retrieve", exact: true });
      if (width === 640) {
        await acquire.click();
        await expect(editor.getByRole("alert").filter({ hasText: "TLS_TRUST" })).toBeVisible();
        expect(fixture.requests).toHaveLength(0);
        await page.screenshot({ path: info.outputPath("independent-api-tls-failure.png") });
      }
      await editor.getByLabel("API TLS trust", { exact: true }).click();
      await page.getByRole("option", { name: "Separate API CA (PEM)", exact: true }).click();
      await editor.getByLabel("API CA file", { exact: true }).setInputFiles({
        name: "fixture-api-ca.pem",
        mimeType: "application/x-pem-file",
        buffer: Buffer.from(fixture.caPem),
      });
      if (width === 640) {
        await token.fill("incorrect-fixture-token");
        await acquire.click();
        await expect(
          editor.getByRole("alert").filter({ hasText: "HTTPS_AUTHENTICATION" }),
        ).toBeVisible();
        await token.fill("fixture-api-token");
        for (const failure of ["malformed", "overflow"] as const) {
          mode = failure;
          await acquire.click();
          await expect(editor.getByRole("alert")).toBeVisible();
          await expect(editor.getByRole("button", { name: "Apply to connection" })).toHaveCount(0);
        }
        mode = "hang";
        await acquire.click();
        await expect(editor.getByRole("button", { name: "Cancel acquisition" })).toBeEnabled();
        await editor.getByRole("button", { name: "Cancel acquisition" }).click();
        await expect(editor.getByText(/Cancellation requested/u)).toBeVisible();
        await expect.poll(() => fixture.sockets.size).toBe(0);
        mode = "normal";
      }
      await acquire.focus();
      await page.keyboard.press("Enter");
      const use = editor.getByRole("button", { name: "Apply to connection" });
      await expect(use).toBeVisible();
      await use.scrollIntoViewIfNeeded();
      expect(
        (await new AxeBuilder({ page }).include('[role="dialog"]').analyze()).violations,
      ).toEqual([]);
      await page.screenshot({ path: info.outputPath("https-certificate-review.png") });
      await use.focus();
      await page.keyboard.press("Enter");
      await editor.getByRole("button", { name: "Test connection", exact: true }).click();
      await expect(
        editor.getByText(
          "Connection test passed. No profile was saved and the active connection was unchanged.",
        ),
      ).toBeVisible();
      await editor.getByRole("button", { name: "Save profile", exact: true }).click();
      await expect(editor).toBeHidden();
      mode = "hang";
      const requests = fixture.requests.length;
      await page.getByRole("button", { name: "Connect profile Local aio" }).click();
      await expect(page.getByLabel("Connection status")).toContainText("Connected", {
        timeout: 15000,
      });
      expect(fixture.requests).toHaveLength(requests);
      await page.screenshot({ path: info.outputPath("https-acquired-kafka-connected.png") });
    } finally {
      await launch?.close();
      await fixture.close();
    }
  });
}
