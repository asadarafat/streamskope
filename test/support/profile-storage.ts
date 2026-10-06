import { expect, type Page } from "@playwright/test";

/** Production hosts must report protected storage or an explicit failure for each provider. */
export async function expectProductionProfileStorage(page: Page): Promise<void> {
  const storage = page.getByRole("status", { name: "Profile storage status", exact: true });
  await expect(
    storage.getByText(/^Kafka(?: · OS-protected profiles\.| profile storage unavailable\.)/u),
  ).toBeVisible();
  await expect(
    storage.getByText(
      /^NATS: (?:Durable profiles · credentials protected by the operating system\.|Profile storage unavailable\.)/u,
    ),
  ).toBeVisible();
}
