import {
  browserUpgradePredecessor,
  BROWSER_UPGRADE_CHECKS,
  type BrowserUpgradeEvidence,
} from "../../tools/package/browser-upgrade-evidence";
import {
  BROWSER_DATA_COMPATIBILITY,
  BROWSER_DATA_DOCUMENT_KINDS,
  BROWSER_DATA_INSPECTION_LIMITATIONS,
} from "../../src/platform/node/browser-data-compatibility";
import type { BrowserDataPreflightEvidence } from "../../tools/package/browser-installer-evidence";

/** Receipt-validation fixture only; real inspection is covered by native/core integration. */
export function browserDataEvidenceFixture(
  version: string,
  imageId: string,
): BrowserDataPreflightEvidence {
  return {
    imageId,
    dataSnapshotSha256: "c".repeat(64),
    inspection: {
      schemaVersion: 1,
      dataContract: BROWSER_DATA_COMPATIBILITY.contract,
      hostRelease: `v${version}`,
      outcome: "eligible",
      documents: BROWSER_DATA_DOCUMENT_KINDS.map((kind) => {
        const present = ["filesystem", "vault", "nats-profiles"].includes(kind);
        return {
          kind,
          state: present ? "verified" : "missing",
          count: kind === "filesystem" ? 4 : present ? 1 : 0,
          formats: present && kind !== "filesystem" ? [1] : [],
          reason: null,
        };
      }),
      unverified: BROWSER_DATA_INSPECTION_LIMITATIONS,
    },
  };
}

/** Structured validator fixture, never evidence of an executed native transition. */
export function browserUpgradeEvidenceFixture(
  platform: string,
  imageId: string,
): BrowserUpgradeEvidence {
  return {
    predecessor: browserUpgradePredecessor(platform),
    targetImageId: imageId,
    upgradeBackup: { inventorySha256: "1".repeat(64), dataSnapshotSha256: "2".repeat(64) },
    rollbackBackup: { inventorySha256: "3".repeat(64), dataSnapshotSha256: "2".repeat(64) },
    checks: BROWSER_UPGRADE_CHECKS,
  };
}
