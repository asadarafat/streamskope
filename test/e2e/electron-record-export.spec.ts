import { createHash } from "node:crypto";
import { lstat, mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { expect, test, type ElectronApplication } from "@playwright/test";

import type { RecordExportReceipt } from "../../src/features/kafka/contracts/record-export";
import {
  buildElectronSmoke,
  buildRenderer,
  chooseNextElectronSavePath,
  connectElectronToFixture,
  launchProfileApplication,
} from "../support/electron-application";
import { startStructuredBrowserFixture } from "../support/structured-browser-fixture";

test.use({ trace: "off" });

test("saves a host range export and receipt through native Save with cancellation and exact file hashes", async () => {
  test.setTimeout(180000);
  const root = await mkdtemp(join(tmpdir(), "streamskope-native-export-"));
  const fixture = await startStructuredBrowserFixture();
  let application: ElectronApplication | undefined;
  try {
    await buildElectronSmoke(root);
    const renderer = await buildRenderer(root);
    application = await launchProfileApplication(root, renderer, join(root, "userdata"));
    const page = await application.firstWindow();
    await connectElectronToFixture(page, fixture.config, fixture.connection);
    await page.getByRole("button", { name: fixture.config.topic, exact: true }).click();
    await page.getByRole("button", { name: "Export records" }).click();
    await page.getByRole("menuitem", { name: "Read range…" }).click();
    await page
      .getByRole("dialog", { name: "Export a topic range" })
      .getByRole("button", { name: "Start export" })
      .click();
    const status = page.getByRole("region", { name: "Range export" });
    await expect(status).toContainText("Range export complete", { timeout: 30000 });
    const before = await readdir(root);
    await application.evaluate(({ dialog }) => {
      Object.defineProperty(dialog, "showSaveDialog", {
        configurable: true,
        value: () => Promise.resolve({ canceled: true }),
      });
    });
    await status.getByRole("button", { name: "Download export", exact: true }).click();
    await expect(status).toContainText("save cancelled");
    expect(await readdir(root)).toEqual(before);

    const dataPath = join(root, "records.jsonl");
    await chooseNextElectronSavePath(application, dataPath);
    await status.getByRole("button", { name: "Download export", exact: true }).click();
    await expect(status).toContainText("Export saved.");
    const bytes = await readFile(dataPath);
    expect((await lstat(dataPath)).mode & 0o777).toBe(0o600);
    const rows = bytes
      .toString("utf8")
      .trimEnd()
      .split("\n")
      .map(
        (line) => JSON.parse(line) as { topic: string; structured: { value: { text: string } } },
      );
    expect(rows.length).toBeGreaterThan(0);
    expect(rows.every((row) => row.topic === fixture.config.topic)).toBe(true);
    expect(rows.some((row) => row.structured.value.text === fixture.config.seedPayload)).toBe(true);

    const receiptPath = join(root, "receipt.json");
    await chooseNextElectronSavePath(application, receiptPath);
    await status.getByRole("button", { name: "Download receipt", exact: true }).click();
    await expect(status).toContainText("Receipt saved.");
    const receipt = JSON.parse(await readFile(receiptPath, "utf8")) as RecordExportReceipt;
    expect(receipt.schema).toBe("streamskope.record-export/v1");
    expect(receipt.output.bytes).toBe(bytes.byteLength);
    expect(receipt.output.sha256).toBe(createHash("sha256").update(bytes).digest("hex"));
    expect(receipt.counts.writtenRecords).toBe(rows.length);
    expect(receipt.outcome).toBe("complete");
    expect((await readdir(root)).some((name) => name.endsWith(".partial"))).toBe(false);
    await status.getByRole("button", { name: "Discard export" }).click();
    await expect(status.getByRole("button", { name: "Download export", exact: true })).toHaveCount(
      0,
    );
  } finally {
    await application?.close();
    await fixture.dispose();
    await rm(root, { recursive: true, force: true });
  }
});
