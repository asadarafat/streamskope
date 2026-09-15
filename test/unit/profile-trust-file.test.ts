// @vitest-environment jsdom
import { expect, it } from "vitest";

import { readTrustFile } from "../../src/kafka/ui/profile-dialog-model";

it("cancels an obsolete authorized file read without publishing its contents", async () => {
  const controller = new AbortController();
  const pending = readTrustFile(new File(["private-ca"], "api.pem"), "pem", controller.signal);
  controller.abort();
  await expect(pending).rejects.toMatchObject({ name: "AbortError" });
  await expect(
    readTrustFile(new File(["private-ca"], "api.pem"), "pem", controller.signal),
  ).rejects.toMatchObject({ name: "AbortError" });
});
