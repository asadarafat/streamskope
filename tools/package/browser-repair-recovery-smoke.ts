import assert from "node:assert/strict";

import {
  verifyTransition,
  type TransitionOptions,
  type LocalPredecessorAssets,
} from "./browser-upgrade-smoke";

/** Exact retained repair-v2 host; independent migration proof cannot replace public delivery. */
export async function verifyLocalRepairRecoveryUpgrade(
  options: TransitionOptions & { readonly predecessorAssets: LocalPredecessorAssets },
): Promise<{
  readonly schemaVersion: 1;
  readonly deliveryScope: "local-staged";
  readonly predecessorScope: "previously-qualified-repair-v2";
  readonly outcome: "passed";
  readonly transition: Awaited<ReturnType<typeof verifyTransition>>;
}> {
  assert.equal(options.target.platform, "linux/arm64");
  for (const digest of [
    options.predecessorAssets.topologySha256,
    options.predecessorAssets.manifestSha256,
  ])
    assert.match(digest, /^[a-f0-9]{64}$/u);
  return {
    schemaVersion: 1,
    deliveryScope: "local-staged",
    predecessorScope: "previously-qualified-repair-v2",
    outcome: "passed",
    transition: await verifyTransition(
      { ...options, repairHistory: false, repairRecovery: { from: 2, to: 3 } },
      {
        predecessor: {
          version: "0.10.8-qa.0aa0d8addfbe",
          sourceRevision: "0aa0d8addfbe8b16ceaf20a322dabfb3d9062ae8",
          platform: "linux/arm64",
          image: "streamskope:0.10.8-qa.0aa0d8addfbe",
          imageId: "sha256:bd2cb21f22ffa3f3cddc6797c056e7218cf753f2bbbe7bbdbb95e248a44583fc",
        },
        localAssets: options.predecessorAssets,
        queryFormat: 4,
      },
    ),
  };
}
