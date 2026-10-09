import { expect, it } from "vitest";

import {
  browserInstallerTargets,
  browserInstallerEvidence,
  validateBrowserInstallerEvidence,
} from "../../tools/package/browser-installer-evidence";
import { BROWSER_DATA_COMPATIBILITY } from "../../src/platform/node/browser-data-compatibility";
import {
  browserDataEvidenceFixture,
  browserUpgradeEvidenceFixture,
} from "../support/browser-data-evidence";

const expected = {
  version: "1.2.3",
  sourceRevision: "a".repeat(40),
  platform: "linux/arm64",
  image: `ghcr.io/asadarafat/streamskope@sha256:${"b".repeat(64)}`,
  imageId: `sha256:${"d".repeat(64)}`,
  execution: { runId: "1234", attempt: 2 },
};
function fixture(): ReturnType<typeof browserInstallerEvidence> {
  return browserInstallerEvidence(
    {
      version: expected.version,
      sourceRevision: expected.sourceRevision,
      platform: expected.platform,
      image: expected.image,
      startedAt: new Date(Date.now() - 1000).toISOString(),
      preflight: browserDataEvidenceFixture(expected.version, expected.imageId),
      transition: browserUpgradeEvidenceFixture(expected.platform, expected.imageId),
    },
    { GITHUB_RUN_ID: "1234", GITHUB_RUN_ATTEMPT: "1" },
  );
}
it("retains native installer execution provenance and strips unrelated values", () => {
  const receipt = fixture();
  expect(
    validateBrowserInstallerEvidence({ ...receipt, token: "private", host: "private" }, expected),
  ).toEqual(receipt);
});
it("never accepts an actual locally qualified v2 predecessor as published-release transition authority", () => {
  const receipt = structuredClone(fixture());
  Object.assign(receipt.transition, {
    predecessor: {
      version: "0.10.4-qa.d9a7c7b365eb",
      sourceRevision: "d9a7c7b365eb5bd78157de3ce2055001a53ba38f",
      platform: "linux/arm64",
      image: "streamskope:0.10.4-qa.d9a7c7b365eb",
      imageId: "sha256:ac286c6049e3886ff2c025ab2be853cb8cc5ade189767b35cacc4baa07d79e0e",
    },
  });
  expect(() => validateBrowserInstallerEvidence(receipt, expected)).toThrow(
    "exact predecessor and target",
  );
});
it.each([
  { schemaVersion: 1 },
  { schemaVersion: 2 },
  { schemaVersion: 3 },
  { transition: undefined },
  { deliveryScope: "local-staged" },
  { sourceRevision: "b".repeat(40) },
  { platform: "linux/amd64" },
  { execution: { runId: "other", attempt: 1 } },
  { execution: { runId: "1234", attempt: 3 } },
  { checks: [] },
  { preflight: undefined },
  { startedAt: "invalid" },
  { completedAt: "2999-01-01T00:00:00.000Z" },
])(
  "rejects native receipts that do not prove this source, platform, checks and run: %j",
  (change) => {
    expect(() => validateBrowserInstallerEvidence({ ...fixture(), ...change }, expected)).toThrow();
  },
);

it.each(["image", "release", "missing-document", "snapshot", "extra-private-field"])(
  "rejects native preflight %s that cannot prove the expected image and complete inspection",
  (change) => {
    const receipt = structuredClone(fixture());
    const preflight = receipt.preflight;
    if (change === "image") Object.assign(preflight, { imageId: `sha256:${"e".repeat(64)}` });
    if (change === "release") Object.assign(preflight.inspection, { hostRelease: "v9.9.9" });
    if (change === "missing-document")
      Object.assign(preflight.inspection, { documents: preflight.inspection.documents.slice(1) });
    if (change === "snapshot") Object.assign(preflight, { dataSnapshotSha256: "missing" });
    if (change === "extra-private-field")
      Object.assign(preflight.inspection, { endpoint: "private" });
    expect(() => validateBrowserInstallerEvidence(receipt, expected)).toThrow();
  },
);

it.each(["all-missing", "nats-profiles", "vault"])(
  "refuses eligible %s evidence that did not inspect the native fixture",
  (missing) => {
    const receipt = structuredClone(fixture());
    Object.assign(receipt.preflight.inspection, {
      documents: receipt.preflight.inspection.documents.map((document) =>
        missing === "all-missing" || document.kind === missing
          ? { ...document, state: "missing", count: 0, formats: [], reason: null }
          : document,
      ),
    });
    expect(() => validateBrowserInstallerEvidence(receipt, expected)).toThrow(
      /created vault and encrypted NATS profile/u,
    );
  },
);

it("derives distinct native image authorities from schema4 and refuses historical capability inference", () => {
  const image = "ghcr.io/asadarafat/streamskope:1.2.3";
  const manifest = {
    schemaVersion: 4,
    version: expected.version,
    sourceRevision: expected.sourceRevision,
    dataCompatibility: BROWSER_DATA_COMPATIBILITY,
    registry: {
      schemaVersion: 1,
      version: expected.version,
      sourceRevision: expected.sourceRevision,
      image,
      digest: `sha256:${"a".repeat(64)}`,
      reference: `${image}@sha256:${"a".repeat(64)}`,
      platforms: [
        {
          platform: "linux/amd64",
          imageId: `sha256:${"c".repeat(64)}`,
          manifestDigest: `sha256:${"e".repeat(64)}`,
        },
        {
          platform: "linux/arm64",
          imageId: expected.imageId,
          manifestDigest: `sha256:${"f".repeat(64)}`,
        },
      ],
    },
  };
  expect(
    browserInstallerTargets(manifest, expected.version, expected.sourceRevision).map(
      (item) => item.imageId,
    ),
  ).toEqual([`sha256:${"c".repeat(64)}`, expected.imageId]);
  for (const change of [
    { schemaVersion: 3 },
    { dataCompatibility: undefined },
    { sourceRevision: "f".repeat(40) },
  ])
    expect(() =>
      browserInstallerTargets(
        { ...manifest, ...change },
        expected.version,
        expected.sourceRevision,
      ),
    ).toThrow();
});

it.each([
  "predecessor-source",
  "predecessor-image",
  "target-image",
  "checks",
  "backup",
  "same-inventory",
  "extra-field",
])(
  "rejects native transition %s without independent predecessor and complete backups",
  (change) => {
    const receipt = structuredClone(fixture());
    const transition = receipt.transition;
    if (change === "predecessor-source")
      Object.assign(transition.predecessor, { sourceRevision: expected.sourceRevision });
    if (change === "predecessor-image")
      Object.assign(transition.predecessor, { imageId: expected.imageId });
    if (change === "target-image")
      Object.assign(transition, { targetImageId: transition.predecessor.imageId });
    if (change === "checks") Object.assign(transition, { checks: [] });
    if (change === "backup") Object.assign(transition, { rollbackBackup: undefined });
    if (change === "same-inventory")
      Object.assign(transition, { rollbackBackup: transition.upgradeBackup });
    if (change === "extra-field") Object.assign(transition, { token: "private" });
    expect(() => validateBrowserInstallerEvidence(receipt, expected)).toThrow();
  },
);
