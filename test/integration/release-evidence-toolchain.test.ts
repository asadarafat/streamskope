import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { promisify } from "node:util";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { fileURLToPath } from "node:url";

import { afterEach, describe, expect, it } from "vitest";

import { validateReleaseManifest } from "../../tools/release-manifest";

const execFileAsync = promisify(execFile);
const repositoryRoot = fileURLToPath(new URL("../..", import.meta.url));
const tsxCli = fileURLToPath(import.meta.resolve("tsx/cli"));
const temporaryDirectories: string[] = [];

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "streamskope-release-evidence-"));
  temporaryDirectories.push(directory);
  return directory;
}

async function runTool(tool: string, arguments_: readonly string[]): Promise<string> {
  const result = await execFileAsync(
    process.execPath,
    [tsxCli, join(repositoryRoot, "tools", tool), ...arguments_],
    {
      cwd: repositoryRoot,
      maxBuffer: 1_048_576,
    },
  );
  return result.stdout;
}

async function writeEvidence(
  directory: string,
  target: {
    readonly architecture: "arm64" | "x64";
    readonly notarization: "not-applicable" | "verified";
    readonly platform: "darwin" | "linux" | "win32";
    readonly signature: "apple-developer-id" | "authenticode" | "openpgp";
  },
): Promise<string> {
  const artifact = join(
    directory,
    `StreamSkope-0.2.0-${target.platform}-${target.architecture}.zip`,
  );
  const evidence = join(
    directory,
    `${target.platform}-${target.architecture}.release-evidence.json`,
  );
  await writeFile(artifact, `synthetic ${target.platform} candidate`);
  const external = join(directory, `${target.platform}.external.json`);
  await writeFile(
    external,
    JSON.stringify({
      architecture: target.architecture,
      file: basename(artifact),
      installation: "passed",
      notarization: target.notarization,
      platform: target.platform,
      sensitiveScan: "passed",
      sha256: createHash("sha256")
        .update(await readFile(artifact))
        .digest("hex"),
      signature: { kind: target.signature, state: "verified" },
      startup: "passed",
    }),
  );
  await runTool("write-platform-release-evidence.ts", [
    "--artifact",
    artifact,
    "--platform",
    target.platform,
    "--architecture",
    target.architecture,
    "--external-evidence",
    external,
    "--output",
    evidence,
  ]);
  return artifact;
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

describe("release evidence toolchain", () => {
  it.each(["digest", "platform", "architecture", "file", "startup", "missing"])(
    "rejects %s mismatches before creating an external evidence report",
    async (failure) => {
      const directory = await temporaryDirectory();
      const artifact = join(directory, "StreamSkope-0.2.0-win32-x64-Setup.exe");
      const external = join(directory, "external.json");
      const output = join(directory, "verified.json");
      await writeFile(artifact, "synthetic artifact");
      const evidence = {
        architecture: "x64",
        file: basename(artifact),
        installation: "passed",
        notarization: "not-applicable",
        platform: "win32",
        sensitiveScan: "passed",
        sha256: createHash("sha256").update("synthetic artifact").digest("hex"),
        signature: { kind: "authenticode", state: "verified" },
        startup: "passed",
      };
      if (failure === "digest") evidence.sha256 = "a".repeat(64);
      if (failure === "platform") {
        evidence.platform = "linux";
        evidence.signature.kind = "openpgp";
      }
      if (failure === "architecture") evidence.architecture = "arm64";
      if (failure === "file") evidence.file = "different.exe";
      if (failure === "startup") evidence.startup = "failed";
      if (failure !== "missing") await writeFile(external, JSON.stringify(evidence));
      await expect(
        runTool("write-platform-release-evidence.ts", [
          "--artifact",
          artifact,
          "--external-evidence",
          external,
          "--platform",
          "win32",
          "--architecture",
          "x64",
          "--output",
          output,
        ]),
      ).rejects.toThrow();
      await expect(readFile(output)).rejects.toMatchObject({ code: "ENOENT" });
    },
  );
  it("assembles synthetic verified evidence and rejects an artifact changed afterward", async () => {
    const directory = await temporaryDirectory();
    const macArtifact = await writeEvidence(directory, {
      architecture: "arm64",
      notarization: "verified",
      platform: "darwin",
      signature: "apple-developer-id",
    });
    await writeEvidence(directory, {
      architecture: "x64",
      notarization: "not-applicable",
      platform: "win32",
      signature: "authenticode",
    });
    await writeEvidence(directory, {
      architecture: "x64",
      notarization: "not-applicable",
      platform: "linux",
      signature: "openpgp",
    });

    const compatibility = join(directory, "n-minus-one.compatibility.json");
    const sbom = join(directory, "streamskope-0.2.0.spdx.json");
    const manifestPath = join(directory, "streamskope-0.2.0.release.json");
    await writeFile(
      compatibility,
      `${JSON.stringify({
        fromVersion: "0.1.0",
        preferences: "passed",
        profiles: "passed",
      })}\n`,
    );
    await writeFile(
      sbom,
      `${JSON.stringify({ dataLicense: "CC0-1.0", spdxVersion: "SPDX-2.3" })}\n`,
    );

    await expect(
      runTool("create-release-manifest.ts", [
        "--evidence-directory",
        directory,
        "--compatibility-evidence",
        compatibility,
        "--sbom",
        sbom,
        "--version",
        "0.2.0",
        "--tag",
        "v0.2.0",
        "--commit",
        "a".repeat(40),
        "--previous-manifest-sha256",
        "b".repeat(64),
        "--attestation-id",
        "synthetic-test-attestation",
        "--output",
        manifestPath,
      ]),
    ).resolves.toContain("Created immutable candidate manifest");

    const manifest = validateReleaseManifest(
      JSON.parse(await readFile(manifestPath, "utf8")) as unknown,
    );
    expect(manifest.artifacts.map((artifact) => artifact.file).sort()).toEqual(
      [
        "StreamSkope-0.2.0-darwin-arm64.zip",
        "StreamSkope-0.2.0-win32-x64.zip",
        "StreamSkope-0.2.0-linux-x64.zip",
      ].sort(),
    );
    expect(manifest.sbom.file).toBe(basename(sbom));

    await writeFile(macArtifact, "mutated after verification");
    await expect(
      runTool("create-release-manifest.ts", [
        "--evidence-directory",
        directory,
        "--compatibility-evidence",
        compatibility,
        "--sbom",
        sbom,
        "--version",
        "0.2.0",
        "--tag",
        "v0.2.0",
        "--commit",
        "a".repeat(40),
        "--previous-manifest-sha256",
        "b".repeat(64),
        "--attestation-id",
        "synthetic-test-attestation",
        "--output",
        join(directory, "mutated.release.json"),
      ]),
    ).rejects.toThrow("changed after native verification");
  });
});
