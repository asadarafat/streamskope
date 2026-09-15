import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { basename, resolve } from "node:path";

import { validateReleaseArtifactEvidence } from "./release-manifest";

function argumentsByName(arguments_: readonly string[]): ReadonlyMap<string, string> {
  if (arguments_.length % 2 !== 0) {
    throw new Error("Release evidence arguments must use --name value pairs.");
  }
  const values = new Map<string, string>();
  for (let index = 0; index < arguments_.length; index += 2) {
    const name = arguments_[index];
    const value = arguments_[index + 1];
    if (name === undefined || value === undefined || !name.startsWith("--") || values.has(name)) {
      throw new Error("Release evidence arguments are invalid or duplicated.");
    }
    values.set(name, value);
  }
  return values;
}

function required(values: ReadonlyMap<string, string>, name: string): string {
  const value = values.get(name);
  if (value === undefined || value.length === 0) {
    throw new Error(`${name} is required.`);
  }
  return value;
}

async function main(): Promise<void> {
  const values = argumentsByName(process.argv.slice(2));
  const expectedArguments = new Set([
    "--architecture",
    "--artifact",
    "--external-evidence",
    "--output",
    "--platform",
  ]);
  if (
    values.size !== expectedArguments.size ||
    [...values.keys()].some((name) => !expectedArguments.has(name))
  ) {
    throw new Error("Release evidence received an undeclared or missing argument.");
  }
  const artifactPath = resolve(required(values, "--artifact"));
  const content = await readFile(artifactPath);
  const external = required(values, "--external-evidence");
  let externalJson: string;
  if (external.startsWith("env:")) {
    const encoded = process.env[external.slice(4)];
    if (!encoded) throw new Error("External final-artifact verification evidence is required.");
    externalJson = Buffer.from(encoded, "base64").toString("utf8");
  } else {
    externalJson = await readFile(resolve(external), "utf8");
  }
  const evidence = validateReleaseArtifactEvidence(JSON.parse(externalJson) as unknown);
  if (
    evidence.file !== basename(artifactPath) ||
    evidence.sha256 !== createHash("sha256").update(content).digest("hex") ||
    evidence.platform !== required(values, "--platform") ||
    evidence.architecture !== required(values, "--architecture")
  ) {
    throw new Error(
      "External evidence does not match the final artifact digest and native target.",
    );
  }
  const outputPath = resolve(required(values, "--output"));
  await writeFile(outputPath, `${JSON.stringify(evidence, null, 2)}\n`, {
    encoding: "utf8",
    flag: "wx",
    mode: 0o600,
  });
  process.stdout.write(`Validated external final-artifact evidence for ${evidence.file}.\n`);
}

void main().catch((error: unknown) => {
  const detail = error instanceof Error ? error.message : String(error);
  process.stderr.write(`Release evidence failed: ${detail}\n`);
  process.exitCode = 1;
});
