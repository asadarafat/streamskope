import { isDeepStrictEqual } from "node:util";

import { REVIEWED_BROWSER_PREDECESSORS } from "../../src/platform/node/browser-data-compatibility";

import type { BrowserInstallerTarget } from "./browser-installer-evidence";

export const BROWSER_UPGRADE_CHECKS = [
  "exact published predecessor",
  "running check preserves unlocked session and data",
  "upgrade owns graceful stop and complete backup",
  "target locked readiness and authenticated profile persistence",
  "target native workers",
  "genuine predecessor query remains unchanged until explicit view migration",
  "saved view and locator metadata survive target vault lock and unlock",
  "incompatible view rollback refuses without changing the running host",
  "explicit operator full-backup recovery preserves changed data and original lease",
  "rollback owns graceful stop and complete backup",
  "predecessor locked readiness and authenticated profile persistence",
  "predecessor native workers",
  "no-argument resume preserves rolled-back release",
  "graceful transition fixture cleanup",
  "exact owned container and network absent after cleanup",
] as const;

export function browserUpgradePredecessor(platform: string): BrowserInstallerTarget {
  if (platform !== "linux/amd64" && platform !== "linux/arm64")
    throw new Error("Browser upgrade qualification requires a reviewed native platform.");
  const baseline = REVIEWED_BROWSER_PREDECESSORS[0]!;
  return {
    version: baseline.version,
    sourceRevision: baseline.sourceRevision,
    platform,
    image: baseline.registryReference,
    imageId: baseline.images[platform === "linux/amd64" ? "amd64" : "arm64"],
  };
}
export interface BrowserBackupEvidence {
  readonly inventorySha256: string;
  readonly dataSnapshotSha256: string;
}
export interface BrowserUpgradeEvidence {
  readonly predecessor: BrowserInstallerTarget;
  readonly targetImageId: string;
  readonly upgradeBackup: BrowserBackupEvidence;
  readonly rollbackBackup: BrowserBackupEvidence;
  readonly checks: typeof BROWSER_UPGRADE_CHECKS;
}
function backup(value: unknown): BrowserBackupEvidence {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Native transition requires complete backup evidence.");
  const record = value as Record<string, unknown>;
  if (
    !isDeepStrictEqual(Object.keys(record).sort(), ["dataSnapshotSha256", "inventorySha256"]) ||
    typeof record.inventorySha256 !== "string" ||
    !/^[a-f0-9]{64}$/u.test(record.inventorySha256) ||
    typeof record.dataSnapshotSha256 !== "string" ||
    !/^[a-f0-9]{64}$/u.test(record.dataSnapshotSha256)
  )
    throw new Error("Native transition backup digests are invalid.");
  return { inventorySha256: record.inventorySha256, dataSnapshotSha256: record.dataSnapshotSha256 };
}
export function validateBrowserUpgradeEvidence(
  value: unknown,
  expected: Pick<BrowserInstallerTarget, "platform" | "imageId">,
): BrowserUpgradeEvidence {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Native qualification requires actual upgrade and rollback evidence.");
  const input = value as Record<string, unknown>;
  const predecessor = browserUpgradePredecessor(expected.platform);
  if (
    !isDeepStrictEqual(Object.keys(input).sort(), [
      "checks",
      "predecessor",
      "rollbackBackup",
      "targetImageId",
      "upgradeBackup",
    ]) ||
    !isDeepStrictEqual(input.predecessor, predecessor) ||
    input.targetImageId !== expected.imageId ||
    !isDeepStrictEqual(input.checks, BROWSER_UPGRADE_CHECKS)
  )
    throw new Error("Native transition evidence does not prove the exact predecessor and target.");
  const upgradeBackup = backup(input.upgradeBackup);
  const rollbackBackup = backup(input.rollbackBackup);
  if (upgradeBackup.inventorySha256 === rollbackBackup.inventorySha256)
    throw new Error("Upgrade and rollback require distinct completed backup inventories.");
  return {
    predecessor,
    targetImageId: expected.imageId,
    upgradeBackup,
    rollbackBackup,
    checks: BROWSER_UPGRADE_CHECKS,
  };
}
