import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { assertDependencyPolicy } from "./dependency-policy";

async function main(): Promise<void> {
  const [manifest, lock] = await Promise.all([
    readFile(resolve("package.json"), "utf8"),
    readFile(resolve("package-lock.json"), "utf8"),
  ]);
  const result = assertDependencyPolicy(JSON.parse(manifest), JSON.parse(lock));
  process.stdout.write(
    `Verified ${String(result.directDependencyCount)} direct dependencies and ${String(result.packageCount)} locked packages across licenses: ${result.licenses.join(", ")}.\n`,
  );
}

void main().catch((error: unknown) => {
  const detail = error instanceof Error ? (error.stack ?? error.message) : String(error);
  process.stderr.write(`Dependency verification failed: ${detail}\n`);
  process.exitCode = 1;
});
