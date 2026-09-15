import { expect, type Page, type TestInfo } from "@playwright/test";

import { startControlledSshServer, type ControlledSshServer } from "./ssh-fixture";
import { openProfileAction } from "./workbench-browser";

export async function reviewSavedRemoteTrust(
  page: Page,
  server: ControlledSshServer,
  info: TestInfo,
): Promise<void> {
  await openProfileAction(page, "Remote acquired aio", "Edit");
  const editor = page.getByRole("dialog", { name: "Edit Kafka profile Remote acquired aio" });
  await editor.getByRole("button", { name: "Secret Retrieval Profile", exact: true }).click();
  await expect(editor.getByRole("textbox", { name: "SSH host", exact: true })).toHaveValue(
    server.host,
  );
  await expect(editor.getByRole("textbox", { name: "SSH password", exact: true })).toHaveValue("");
  await editor.getByRole("textbox", { name: "SSH password", exact: true }).fill("ssh-password");
  await editor.getByLabel(/Acquisition truststore password/u).fill("password");
  await editor.getByRole("button", { name: "Retrieve" }).click();
  await expect(
    editor.getByRole("status", { name: "Remote trust acquisition status" }),
  ).toContainText("JKS trust material acquired");
  await expect(editor.getByRole("button", { name: "Accept identity and acquire" })).toHaveCount(0);
  await page.screenshot({
    animations: "disabled",
    path: info.outputPath("saved-identity-reused.png"),
  });
  await editor.getByRole("button", { name: "Discard acquired trust" }).click();
  await editor.getByRole("button", { name: "Manage retrieval profiles", exact: true }).click();
  const manager = page.getByRole("dialog", { name: "Secret Retrieval Profiles" });
  await manager.getByRole("button", { name: /^SSH truststore/u }).click();
  await manager.getByLabel("Retrieval profile name", { exact: true }).fill("Reviewed truststore");
  await manager.getByRole("button", { name: "Save retrieval profile" }).click();
  await expect(manager.getByRole("status")).toHaveText("Retrieval profile saved.");
  await manager.getByRole("button", { name: "Close retrieval profiles" }).click();
  await editor.getByRole("button", { name: "Review template update" }).click();
  await expect(editor.getByRole("region", { name: "Template update review" })).toBeVisible();
  await page.screenshot({
    animations: "disabled",
    path: info.outputPath("pinned-template-update-review.png"),
  });
  await editor.getByRole("button", { name: "Cancel update" }).click();
  await editor.getByRole("button", { name: "Review template update" }).click();
  await editor.getByRole("button", { name: "Adopt template update" }).click();
  await editor.getByRole("button", { name: "Manage retrieval profiles", exact: true }).click();
  await manager.getByRole("button", { name: /^Reviewed truststore/u }).click();
  await manager.getByRole("button", { name: "Delete retrieval profile" }).click();
  const deletion = page.getByRole("dialog", {
    name: "Delete retrieval profile Reviewed truststore?",
  });
  await expect(deletion).toContainText("Remote acquired aio");
  await page.screenshot({
    animations: "disabled",
    path: info.outputPath("pinned-template-delete-impact.png"),
  });
  await deletion.getByRole("button", { name: "Confirm deletion" }).click();
  await manager.getByRole("button", { name: "Close retrieval profiles" }).click();
  await expect(editor).toContainText("retained");
  // Cancel discards adoption; the persisted revision and trust remain unchanged.
  await editor.getByRole("button", { name: "Cancel", exact: true }).click();
  await server.close();
  const changed = await startControlledSshServer({ port: server.port });
  try {
    await openProfileAction(page, "Remote acquired aio", "Edit");
    await editor.getByRole("button", { name: "Secret Retrieval Profile", exact: true }).click();
    await editor.getByRole("textbox", { name: "SSH password", exact: true }).fill("ssh-password");
    await editor.getByLabel(/Acquisition truststore password/u).fill("password");
    await editor.getByRole("button", { name: "Retrieve" }).click();
    await expect(editor.getByRole("alert").filter({ hasText: "SSH_IDENTITY" })).toBeVisible();
    expect(changed.events.filter((event) => event.startsWith("AUTHENTICATION:"))).toEqual([]);
    await page.screenshot({
      animations: "disabled",
      path: info.outputPath("changed-identity-rejected.png"),
    });
    await editor.getByRole("button", { name: "Reset saved SSH identity" }).click();
    await editor.getByRole("button", { name: "Cancel identity reset" }).click();
    await editor.getByRole("button", { name: "Cancel", exact: true }).click();
  } finally {
    await changed.close();
  }
}

export async function prepareRemoteJksTrust(
  page: Page,
  server: ControlledSshServer,
): Promise<void> {
  const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
  await expect(editor.getByRole("combobox", { name: "Trust material format" })).toHaveText(
    "PEM certificate",
  );
  await editor.getByRole("button", { name: "Secret Retrieval Profile", exact: true }).click();
  await editor.getByRole("combobox", { name: "Use profile", exact: true }).click();
  await page.getByRole("option", { name: "SSH truststore", exact: true }).click();
  await editor
    .getByRole("textbox", { name: /Certificate path/u })
    .fill("/etc/kafka/truststore.jks");
  await editor.getByLabel(/Acquisition truststore password/u).fill("password");
  await editor.getByRole("textbox", { name: "SSH host", exact: true }).fill(server.host);
  await editor.getByRole("spinbutton", { name: "SSH port" }).fill(String(server.port));
  await editor.getByRole("textbox", { name: "SSH username" }).fill("operator");
  await editor.getByRole("textbox", { name: "SSH password", exact: true }).fill("ssh-password");
}

export async function acquireRemoteJksTrust(
  page: Page,
  server: ControlledSshServer,
): Promise<void> {
  const editor = page.getByRole("dialog", { name: "Add Kafka profile" });
  await prepareRemoteJksTrust(page, server);

  await editor.getByRole("button", { name: "Retrieve" }).click();
  await editor.getByRole("button", { name: "Accept identity and acquire" }).click();
  await expect(
    editor.getByRole("status", { name: "Remote trust acquisition status" }),
  ).toContainText("JKS trust material acquired");
  await editor.getByRole("button", { name: "Apply to connection" }).click();
}
