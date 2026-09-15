import { scanRepositoryArtifacts } from "./sensitive-artifact-policy";

void scanRepositoryArtifacts().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : "Unknown artifact-scan failure.";
  process.stderr.write(`Sensitive-artifact scan failed: ${message}\n`);
  process.exitCode = 1;
});
