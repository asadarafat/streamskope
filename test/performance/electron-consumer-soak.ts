import {
  assertSustainedConsumptionEvidence,
  measureSustainedConsumption,
} from "../support/sustained-consumption-measurement";
import { writePerformanceEvidence } from "../../tools/performance-evidence";

async function main(): Promise<void> {
  if (process.versions.node.split(".")[0] !== "24") {
    throw new Error(`Performance gate requires Node 24; received ${process.version}.`);
  }
  const evidence = await measureSustainedConsumption();
  let failure: Error | undefined;
  try {
    assertSustainedConsumptionEvidence(evidence);
  } catch (error: unknown) {
    failure = error instanceof Error ? error : new Error(String(error));
  }
  await writePerformanceEvidence("electron-consumer-soak.json", {
    check: "sustained-consumer-lifecycle",
    command: "npm run performance:electron-consumer-soak",
    evidence,
    ...(failure === undefined ? {} : { failure: failure.message }),
    outcome: failure === undefined ? "passed" : "failed",
    sampleMethod:
      "One hundred thousand deterministic records through the controlled production facade, batching, reducer and stop lifecycle.",
  });
  if (failure !== undefined) {
    throw failure;
  }
}

void main().catch((error: unknown) => {
  const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stderr.write(`Sustained-consumption performance gate failed: ${detail}\n`);
  process.exitCode = 1;
});
