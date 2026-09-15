import {
  assertRendererMessageRetentionEvidence,
  measureRendererMessageRetention,
} from "../support/renderer-message-retention-measurement";
import { writePerformanceEvidence } from "../../tools/performance-evidence";

async function main(): Promise<void> {
  if (process.versions.node.split(".")[0] !== "24") {
    throw new Error(`Performance gate requires Node 24; received ${process.version}.`);
  }
  if (globalThis.gc === undefined) {
    throw new Error("Renderer retention measurement requires Node --expose-gc.");
  }
  const evidence = measureRendererMessageRetention({
    collectHeapBytes: () => process.memoryUsage().heapUsed,
    forceGarbageCollection: () => {
      globalThis.gc?.();
    },
  });
  let failure: Error | undefined;
  try {
    assertRendererMessageRetentionEvidence(evidence);
  } catch (error: unknown) {
    failure = error instanceof Error ? error : new Error(String(error));
  }
  await writePerformanceEvidence("electron-message-retention.json", {
    check: "bounded-message-retention",
    command: "npm run performance:electron-message-retention",
    evidence,
    ...(failure === undefined ? {} : { failure: failure.message }),
    outcome: failure === undefined ? "passed" : "failed",
    sampleMethod:
      "Ten thousand deterministic records through the production reducer with exposed garbage collection.",
  });
  if (failure !== undefined) {
    throw failure;
  }
}

void main().catch((error: unknown) => {
  const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stderr.write(`Renderer retention performance gate failed: ${detail}\n`);
  process.exitCode = 1;
});
