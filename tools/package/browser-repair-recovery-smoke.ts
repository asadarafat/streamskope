import assert from "node:assert/strict";

import {
  verifyTransition,
  type TransitionOptions,
  type LocalPredecessorAssets,
} from "./browser-upgrade-smoke";

/** Exact retained repair-v1 host; independent migration proof cannot replace public delivery. */
export async function verifyLocalRepairRecoveryUpgrade(
  options: TransitionOptions & { readonly predecessorAssets: LocalPredecessorAssets },
): Promise<{
  readonly schemaVersion: 1;
  readonly deliveryScope: "local-staged";
  readonly predecessorScope: "previously-qualified-repair-v1";
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
    predecessorScope: "previously-qualified-repair-v1",
    outcome: "passed",
    transition: await verifyTransition(
      { ...options, repairHistory: false, repairRecovery: true },
      {
        predecessor: {
          version: "0.10.7-qa.d019d1158623",
          sourceRevision: "d019d1158623b85917bf59225582e63f8736d987",
          platform: "linux/arm64",
          image: "streamskope:0.10.7-qa.d019d1158623",
          imageId: "sha256:f11d1c6463b310cb44e8cf65b23f9222024281052b6d1226dc6a7da66c6a2b1d",
        },
        localAssets: options.predecessorAssets,
        queryFormat: 4,
      },
    ),
  };
}
