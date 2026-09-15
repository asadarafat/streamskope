import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

export interface PerformanceEvidenceRuntime {
  readonly arch: string;
  readonly node: string;
  readonly platform: string;
}

export interface PerformanceEvidenceInput<Evidence> {
  readonly capturedAt: string;
  readonly check: string;
  readonly command: string;
  readonly evidence: Evidence;
  readonly failure?: string;
  readonly outcome: "failed" | "passed";
  readonly runtime: PerformanceEvidenceRuntime;
  readonly sampleMethod: string;
}

export interface PerformanceEvidenceDocument<Evidence> extends PerformanceEvidenceInput<Evidence> {
  readonly schemaVersion: 1;
}

export function createPerformanceEvidence<Evidence>(
  input: PerformanceEvidenceInput<Evidence>,
): PerformanceEvidenceDocument<Evidence> {
  return Object.freeze({
    ...input,
    schemaVersion: 1 as const,
  });
}

export async function writePerformanceEvidence<Evidence>(
  fileName: string,
  input: Omit<PerformanceEvidenceInput<Evidence>, "capturedAt" | "runtime">,
): Promise<PerformanceEvidenceDocument<Evidence>> {
  if (!/^[a-z0-9-]+\.json$/u.test(fileName)) {
    throw new Error("Performance evidence file name must be a safe JSON base name.");
  }
  const document = createPerformanceEvidence({
    ...input,
    capturedAt: new Date().toISOString(),
    runtime: {
      arch: process.arch,
      node: process.version,
      platform: process.platform,
    },
  });
  const outputDirectory = resolve(process.cwd(), "dist/performance");
  await mkdir(outputDirectory, { recursive: true });
  await writeFile(
    resolve(outputDirectory, fileName),
    `${JSON.stringify(document, null, 2)}\n`,
    "utf8",
  );
  process.stdout.write(`${JSON.stringify(document, null, 2)}\n`);
  return document;
}
