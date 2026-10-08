import { expect, it } from "vitest";

import {
  browserInstallerEvidence,
  validateBrowserInstallerEvidence,
} from "../../tools/package/browser-installer-evidence";

const expected = {
  version: "1.2.3",
  sourceRevision: "a".repeat(40),
  platform: "linux/arm64",
  image: `ghcr.io/asadarafat/streamskope@sha256:${"b".repeat(64)}`,
  execution: { runId: "1234", attempt: 2 },
};
function fixture(): ReturnType<typeof browserInstallerEvidence> {
  return browserInstallerEvidence(
    { ...expected, startedAt: new Date(Date.now() - 1000).toISOString() },
    { GITHUB_RUN_ID: "1234", GITHUB_RUN_ATTEMPT: "1" },
  );
}
it("retains native installer execution provenance and strips unrelated values", () => {
  const receipt = fixture();
  expect(
    validateBrowserInstallerEvidence({ ...receipt, token: "private", host: "private" }, expected),
  ).toEqual(receipt);
});
it.each([
  { schemaVersion: 1 },
  { sourceRevision: "b".repeat(40) },
  { platform: "linux/amd64" },
  { execution: { runId: "other", attempt: 1 } },
  { execution: { runId: "1234", attempt: 3 } },
  { checks: [] },
  { startedAt: "invalid" },
  { completedAt: "2999-01-01T00:00:00.000Z" },
])(
  "rejects native receipts that do not prove this source, platform, checks and run: %j",
  (change) => {
    expect(() => validateBrowserInstallerEvidence({ ...fixture(), ...change }, expected)).toThrow();
  },
);
