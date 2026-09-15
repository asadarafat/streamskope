import { appendFile, readFile } from "node:fs/promises";
import { resolve } from "node:path";

interface ProjectManifest {
  readonly version?: unknown;
}

async function main(): Promise<void> {
  const tag = process.env.GITHUB_REF_NAME;
  const commit = process.env.GITHUB_SHA;
  const output = process.env.GITHUB_OUTPUT;
  const manifest = JSON.parse(await readFile(resolve("package.json"), "utf8")) as ProjectManifest;
  if (
    typeof manifest.version !== "string" ||
    !/^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$/u.test(manifest.version) ||
    tag !== `v${manifest.version}` ||
    typeof commit !== "string" ||
    !/^[a-f0-9]{40}$/u.test(commit) ||
    output === undefined
  ) {
    throw new Error("The protected tag, package version, commit, or workflow output is invalid.");
  }
  await appendFile(output, `version=${manifest.version}\n`, "utf8");
  process.stdout.write(`Validated release tag ${tag} at ${commit}.\n`);
}

void main().catch((error: unknown) => {
  const detail = error instanceof Error ? error.message : String(error);
  process.stderr.write(`Release tag validation failed: ${detail}\n`);
  process.exitCode = 1;
});
