import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import { utils } from "ssh2";
import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

import { HOST_PROTOCOL_VERSION } from "../../src/features/kafka/contracts";
import { createKafkaBackend } from "../../src/platform/electron/main";
import { launchWebDevelopment, type RunningWebDevelopment } from "../../src/platform/dev-host";
import { loadFixtureConnection } from "../support/kafka-fixture";
import { startControlledSshServer } from "../support/ssh-fixture";
import { configureLocalConnection } from "../support/web-profile-workflow";

test.use({ trace: "off" });

async function reservePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((done) => server.listen(0, "127.0.0.1", done));
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("No test port.");
  await new Promise<void>((done, reject) =>
    server.close((error) => (error ? reject(error) : done())),
  );
  return address.port;
}

for (const [kind, width, colorScheme] of [
  ["pem", 640, "dark"],
  ["pkcs12", 800, "light"],
] as const) {
  test(`reviews, expires, reacquires and tests real ${kind} trust at ${width}`, async ({
    page,
  }, info) => {
    test.setTimeout(90_000);
    const fixture = await loadFixtureConnection();
    const directory = await mkdtemp(join(tmpdir(), "streamskope-trust-browser-"));
    let launch: RunningWebDevelopment | undefined;
    const key = utils.generateKeyPairSync("ed25519");
    let reachedTransfer = (): void => undefined;
    const transferring = new Promise<void>((resolveTransfer) => {
      reachedTransfer = resolveTransfer;
    });
    let closedConnections = 0;
    const transferGate = {
      authorizedPublicKey: key.public,
      hangSftpRead: true,
      onSftpRead: (): void => reachedTransfer(),
      onConnectionClosed: (): void => {
        closedConnections += 1;
      },
    };
    const ssh = await startControlledSshServer(transferGate);
    try {
      let bytes = await readFile(fixture.caPath);
      if (kind === "pkcs12") {
        const output = join(directory, "trust.p12");
        await promisify(execFile)(
          "openssl",
          [
            "pkcs12",
            "-export",
            "-nokeys",
            "-in",
            fixture.caPath,
            "-out",
            output,
            "-passout",
            "pass:password",
          ],
          { timeout: 5_000 },
        );
        bytes = await readFile(output);
      }
      ssh.putFile("/fixture/trust", bytes);
      const backend = createKafkaBackend();
      expect(
        await backend.execute({
          command: "recipes.create",
          id: "browser-recipe",
          version: HOST_PROTOCOL_VERSION,
          payload: {
            name: `Fixture ${kind}`,
            kind,
            method: "ssh",
            syntax: "named-v1",
            parameters: [],
            timeoutSeconds: 30,
            ssh: {
              source: "file",
              value: "/fixture/trust",
              password: { source: kind === "pem" ? "none" : "ask" },
            },
          },
        }),
      ).toMatchObject({ ok: true });
      launch = await launchWebDevelopment({
        backend,
        hostPort: await reservePort(),
        rendererPort: await reservePort(),
        rendererRoot: resolve(process.cwd()),
      });
      await page.setViewportSize({ width, height: 1000 });
      await page.emulateMedia({ colorScheme, reducedMotion: "reduce" });
      await page.goto(launch.browserUrl);
      await configureLocalConnection(page);
      const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
      await editor.getByRole("button", { name: "Secret Retrieval Profile", exact: true }).click();
      await editor.getByRole("combobox", { name: "Use profile", exact: true }).click();
      await page.getByRole("option", { name: `Fixture ${kind}`, exact: true }).click();
      if (kind === "pkcs12")
        await editor.getByLabel(/Acquisition truststore password/u).fill("password");
      await editor.getByRole("textbox", { name: "SSH host", exact: true }).fill(ssh.host);
      await editor.getByRole("spinbutton", { name: "SSH port" }).fill(String(ssh.port));
      await editor.getByRole("textbox", { name: "SSH username" }).fill("operator");
      if (kind === "pem") {
        await editor.getByLabel("SSH authentication", { exact: true }).click();
        await page.getByRole("option", { name: "Local SSH agent", exact: true }).click();
        await expect(
          editor.getByText(/Agent forwarding and automatic password fallback are disabled/u),
        ).toBeVisible();
        await page.screenshot({
          animations: "disabled",
          path: info.outputPath("agent-capability-state.png"),
        });
        await editor.getByLabel("SSH authentication", { exact: true }).click();
        await page.getByRole("option", { name: "Private key", exact: true }).click();
        await editor.getByLabel("SSH private key file", { exact: true }).setInputFiles({
          name: "fixture-key",
          mimeType: "application/octet-stream",
          buffer: Buffer.from(key.private),
        });
        await expect(editor.getByText("Private key loaded for this editor only.")).toBeVisible();
      } else {
        await editor
          .getByRole("textbox", { name: "SSH password", exact: true })
          .fill("ssh-password");
      }
      const acquire = editor.getByRole("button", { name: "Retrieve", exact: true });
      await acquire.focus();
      await page.keyboard.press("Enter");
      const accept = editor.getByRole("button", { name: "Accept identity and acquire" });
      await expect(accept).toBeVisible();
      await expect(accept).toBeInViewport();
      expect(ssh.events.filter((event) => event.startsWith("AUTHENTICATION:"))).toHaveLength(0);
      await page.screenshot({
        animations: "disabled",
        path: info.outputPath(`${kind}-identity-review.png`),
      });
      await accept.focus();
      await page.keyboard.press("Enter");
      await transferring;
      await expect(editor.getByRole("button", { name: "Cancel acquisition" })).toBeEnabled();
      await page.screenshot({
        animations: "disabled",
        path: info.outputPath(`${kind}-transfer-in-progress.png`),
      });
      await editor.getByRole("button", { name: "Cancel acquisition" }).click();
      await expect(
        editor.getByText(
          "Cancellation requested. Existing trust and the active connection are unchanged.",
        ),
      ).toBeVisible();
      await expect.poll(() => closedConnections).toBeGreaterThanOrEqual(2);
      await expect(acquire).toBeEnabled();
      await page.screenshot({
        animations: "disabled",
        path: info.outputPath(`${kind}-transfer-cancelled.png`),
      });
      transferGate.hangSftpRead = false;
      await acquire.click();
      await accept.click();
      await expect(
        editor.getByRole("status", { name: "Remote trust acquisition status" }),
      ).toContainText("trust material acquired");
      expect(
        (await new AxeBuilder({ page }).include('[role="dialog"]').analyze()).violations,
      ).toEqual([]);
      await page.screenshot({
        animations: "disabled",
        path: info.outputPath(`${kind}-certificate-evidence.png`),
      });
      // Browser-clock expiry tests the UI guard; host-clock expiry has independent application tests.
      const now = Date.now();
      await page.clock.setFixedTime(now + 11 * 60_000);
      await editor.getByRole("button", { name: "Apply to connection" }).click();
      await expect(
        editor.getByRole("alert").filter({ hasText: "This candidate expired" }),
      ).toBeVisible();
      await expect(
        editor.getByRole("alert").filter({ hasText: "This candidate expired" }),
      ).toBeInViewport();
      await page.screenshot({
        animations: "disabled",
        path: info.outputPath(`${kind}-expired-candidate.png`),
      });
      await page.clock.setFixedTime(now);
      await editor.getByRole("button", { name: "Discard acquired trust" }).click();
      await acquire.click();
      await accept.click();
      await expect(
        editor.getByRole("status", { name: "Remote trust acquisition status" }),
      ).toContainText("trust material acquired");
      await editor.getByRole("button", { name: "Apply to connection" }).click();
      await editor.getByRole("button", { name: "Secret Retrieval Profile", exact: true }).click();
      if (kind === "pkcs12") {
        const password = editor.getByLabel("Truststore password", { exact: true });
        await expect(password).toHaveAttribute("placeholder", "••••••••");
        await expect(password).toHaveValue("");
        await expect(password).toHaveAccessibleDescription(/Retrieved successfully/u);
        await expect(editor.getByRole("button", { name: "Show truststore password" })).toHaveCount(
          0,
        );
        await password.scrollIntoViewIfNeeded();
        await page.screenshot({
          animations: "disabled",
          path: info.outputPath("retrieved-password-in-connection.png"),
        });
      }
      await editor.getByRole("button", { name: "Test connection", exact: true }).click();
      await expect(
        editor.getByText(
          "Connection test passed. No profile was saved and the active connection was unchanged.",
        ),
      ).toBeVisible();
      await editor
        .getByText(
          "Connection test passed. No profile was saved and the active connection was unchanged.",
        )
        .scrollIntoViewIfNeeded();
      await expect(page.getByLabel("Connection status")).toContainText("Disconnected");
      await page.screenshot({
        animations: "disabled",
        path: info.outputPath(`${kind}-tested-draft.png`),
      });
      await editor.getByRole("button", { name: "Cancel", exact: true }).click();
      await expect(editor).toBeHidden();
      await expect(page.getByRole("button", { name: "Select profile Local aio" })).toHaveCount(0);
      expect(ssh.commands).toEqual([]);
      expect(ssh.removedPaths).toEqual([]);
    } finally {
      await launch?.close();
      await ssh.close();
      await rm(directory, { recursive: true, force: true });
    }
  });
}
