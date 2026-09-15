import { createHash } from "node:crypto";
import { readFile, readdir, writeFile } from "node:fs/promises";
import { basename, join, resolve } from "node:path";

import {
  validateReleaseArtifactEvidence,
  validateReleaseCompatibilityEvidence,
  validateReleaseManifest,
  type ReleaseArtifactEvidence,
} from "./release-manifest";

function argumentsByName(arguments_: readonly string[]): ReadonlyMap<string, string> {
  if (arguments_.length % 2 !== 0) {
    throw new Error("Release manifest arguments must use --name value pairs.");
  }
  const values = new Map<string, string>();
  for (let index = 0; index < arguments_.length; index += 2) {
    const name = arguments_[index];
    const value = arguments_[index + 1];
    if (name === undefined || value === undefined || !name.startsWith("--") || values.has(name)) {
      throw new Error("Release manifest arguments are invalid or duplicated.");
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

async function sha256(path: string): Promise<string> {
  return createHash("sha256")
    .update(await readFile(path))
    .digest("hex");
}

async function platformEvidence(directory: string): Promise<ReleaseArtifactEvidence[]> {
  const names = (await readdir(directory))
    .filter((name) => name.endsWith(".release-evidence.json"))
    .sort();
  const evidence: ReleaseArtifactEvidence[] = [];
  for (const [index, name] of names.entries()) {
    const value = JSON.parse(await readFile(join(directory, name), "utf8")) as unknown;
    const artifact = validateReleaseArtifactEvidence(value, index);
    const actualDigest = await sha256(join(directory, artifact.file));
    if (actualDigest !== artifact.sha256) {
      throw new Error(`Release artifact ${artifact.file} changed after native verification.`);
    }
    evidence.push(artifact);
  }
  return evidence;
}

async function main(): Promise<void> {
  const values = argumentsByName(process.argv.slice(2));
  const expectedArguments = new Set([
    "--attestation-id",
    "--commit",
    "--compatibility-evidence",
    "--evidence-directory",
    "--output",
    "--previous-manifest-sha256",
    "--sbom",
    "--tag",
    "--version",
  ]);
  if (
    values.size !== expectedArguments.size ||
    [...values.keys()].some((name) => !expectedArguments.has(name))
  ) {
    throw new Error("Release manifest received an undeclared or missing argument.");
  }
  const evidenceDirectory = resolve(required(values, "--evidence-directory"));
  const sbomPath = resolve(required(values, "--sbom"));
  const compatibility = validateReleaseCompatibilityEvidence(
    JSON.parse(
      await readFile(resolve(required(values, "--compatibility-evidence")), "utf8"),
    ) as unknown,
  );
  const manifest = validateReleaseManifest({
    artifacts: await platformEvidence(evidenceDirectory),
    channel: "candidate",
    commit: required(values, "--commit"),
    compatibility,
    immutable: true,
    provenance: {
      attestationId: required(values, "--attestation-id"),
      issuer: "github-actions",
      state: "verified",
    },
    rollback: {
      manifestSha256: required(values, "--previous-manifest-sha256"),
      version: compatibility.fromVersion,
    },
    sbom: {
      file: basename(sbomPath),
      format: "spdx-2.3",
      sha256: await sha256(sbomPath),
    },
    schemaVersion: 1,
    tag: required(values, "--tag"),
    version: required(values, "--version"),
  });
  const outputPath = resolve(required(values, "--output"));
  await writeFile(outputPath, `${JSON.stringify(manifest, null, 2)}\n`, {
    encoding: "utf8",
    flag: "wx",
    mode: 0o600,
  });
  process.stdout.write(`Created immutable candidate manifest ${basename(outputPath)}.\n`);
}

void main().catch((error: unknown) => {
  const detail = error instanceof Error ? error.message : String(error);
  process.stderr.write(`Release manifest creation failed: ${detail}\n`);
  process.exitCode = 1;
});
