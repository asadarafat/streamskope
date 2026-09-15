import { readdir, readFile } from "node:fs/promises";
import { relative, resolve } from "node:path";
import { pathToFileURL } from "node:url";

import { ELECTRON_RUNTIME_EFFICIENCY_POLICY } from "./electron-runtime-efficiency-policy";
import { writePerformanceEvidence } from "./performance-evidence";

const BACKGROUND_PRIMITIVE =
  /\b(?:cancelAnimationFrame|clearInterval|clearTimeout|requestAnimationFrame|setInterval|setTimeout)\b/gu;
const FORBIDDEN_POLLING_PRIMITIVE = /\b(?:clearInterval|setInterval)\b/gu;

export interface ProductionBackgroundWorkAudit {
  readonly forbiddenUsages: readonly string[];
  readonly missingOwners: readonly string[];
  readonly ownerFiles: readonly string[];
  readonly undeclaredOwners: readonly string[];
}

async function sourceFiles(root: string): Promise<string[]> {
  const entries = await readdir(root, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    const path = resolve(root, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await sourceFiles(path)));
    } else if (entry.isFile() && (path.endsWith(".ts") || path.endsWith(".tsx"))) {
      files.push(path);
    }
  }
  return files;
}

export async function auditProductionBackgroundWork(
  repositoryRoot: string,
): Promise<ProductionBackgroundWorkAudit> {
  const sourceRoot = resolve(repositoryRoot, "src");
  const declared = new Set(ELECTRON_RUNTIME_EFFICIENCY_POLICY.backgroundWorkOwnerFiles);
  const ownerFiles: string[] = [];
  const forbiddenUsages: string[] = [];
  for (const file of await sourceFiles(sourceRoot)) {
    const content = await readFile(file, "utf8");
    const path = relative(repositoryRoot, file).replaceAll("\\", "/");
    if (BACKGROUND_PRIMITIVE.test(content)) {
      ownerFiles.push(path);
    }
    BACKGROUND_PRIMITIVE.lastIndex = 0;
    if (FORBIDDEN_POLLING_PRIMITIVE.test(content)) {
      forbiddenUsages.push(path);
    }
    FORBIDDEN_POLLING_PRIMITIVE.lastIndex = 0;
  }
  ownerFiles.sort();
  forbiddenUsages.sort();
  const observed = new Set(ownerFiles);
  return {
    forbiddenUsages: Object.freeze(forbiddenUsages),
    missingOwners: Object.freeze([...declared].filter((path) => !observed.has(path)).sort()),
    ownerFiles: Object.freeze(ownerFiles),
    undeclaredOwners: Object.freeze(ownerFiles.filter((path) => !declared.has(path))),
  };
}

async function main(): Promise<void> {
  const report = await auditProductionBackgroundWork(process.cwd());
  const failed =
    report.forbiddenUsages.length > 0 ||
    report.missingOwners.length > 0 ||
    report.undeclaredOwners.length > 0;
  const failure = "Production background-work audit failed.";
  await writePerformanceEvidence("background-work.json", {
    check: "production-background-work",
    command: "npm run performance:background-work",
    evidence: report,
    ...(failed ? { failure } : {}),
    outcome: failed ? "failed" : "passed",
    sampleMethod:
      "Static scan of production TypeScript sources against the canonical timer-owner policy.",
  });
  if (failed) {
    throw new Error("Production background-work audit failed.");
  }
}

const entryPath = process.argv[1];
if (entryPath !== undefined && import.meta.url === pathToFileURL(resolve(entryPath)).href) {
  void main().catch((error: unknown) => {
    const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
    process.stderr.write(`Background-work audit failed: ${detail}\n`);
    process.exitCode = 1;
  });
}
