import { spawn, spawnSync } from "node:child_process";
import { createWriteStream } from "node:fs";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { basename, join, resolve } from "node:path";
import { pipeline } from "node:stream/promises";
import { createGzip } from "node:zlib";
import { fileURLToPath } from "node:url";

import { ciExecution } from "../check/ci-evidence";

import { verifyBrowserContainer } from "./container-smoke";
import { browserReleaseTopology } from "./browser-release";

export { browserReleaseTopology as renderBrowserTopology } from "./browser-release";

const SEMVER = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/u;

function run(command: string, args: readonly string[]): void {
  const result = spawnSync(command, [...args], { stdio: "inherit" });
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0)
    throw new Error(`${command} did not complete (${result.status ?? result.signal}).`);
}
function capture(command: string, args: readonly string[]): string {
  const result = spawnSync(command, [...args], { encoding: "utf8", maxBuffer: 1024 * 1024 });
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) throw new Error(`${command} could not inspect the container build.`);
  return result.stdout.trim();
}
interface ImageInspection {
  readonly Id: string;
  readonly Os: string;
  readonly Architecture: string;
  readonly Config: { readonly Labels: Readonly<Record<string, string>> };
}
function inspect(image: string, version: string, revision: string): ImageInspection {
  const [inspection] = JSON.parse(capture("docker", ["image", "inspect", image])) as [
    ImageInspection,
  ];
  if (inspection.Os !== "linux" || !["amd64", "arm64"].includes(inspection.Architecture))
    throw new Error("Container packaging supports Linux ARM64 and AMD64 images.");
  const expectedArch = process.env.EXPECTED_ARCH;
  if (expectedArch !== undefined && inspection.Architecture !== expectedArch)
    throw new Error("Built container does not match the expected native architecture.");
  if (
    inspection.Config.Labels["org.opencontainers.image.version"] !== version ||
    inspection.Config.Labels["org.opencontainers.image.revision"] !== revision
  )
    throw new Error("Built container metadata does not match the source manifest and revision.");
  return inspection;
}
async function archiveImage(image: string, archive: string): Promise<void> {
  const child = spawn("docker", ["image", "save", image], { stdio: ["ignore", "pipe", "ignore"] });
  const exit = new Promise<void>((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code) =>
      code === 0 ? resolve() : reject(new Error("Container image archive did not complete.")),
    );
  });
  try {
    await Promise.all([pipeline(child.stdout, createGzip(), createWriteStream(archive)), exit]);
  } catch (error) {
    child.kill();
    await rm(archive, { force: true });
    throw error;
  }
}

/** Package a native image; --archive additionally loads and qualifies its portable archive. */
export async function packageBrowserContainer(args: readonly string[] = []): Promise<void> {
  if (args.length > 1 || (args.length === 1 && args[0] !== "--archive"))
    throw new Error("Usage: npm run package -- container [--archive]");
  const manifest = JSON.parse(await readFile("package.json", "utf8")) as { version?: unknown };
  if (typeof manifest.version !== "string" || !SEMVER.test(manifest.version))
    throw new Error("Container packaging requires a SemVer source manifest.");
  const revision = capture("git", ["rev-parse", "HEAD"]);
  if (
    !/^[0-9a-f]{40}$/u.test(revision) ||
    (process.env.STREAMSKOPE_SOURCE_REVISION !== undefined &&
      process.env.STREAMSKOPE_SOURCE_REVISION !== revision)
  )
    throw new Error("Container source revision must match the selected Git commit.");
  const expectedArch = process.env.EXPECTED_ARCH;
  if (expectedArch !== undefined && !["amd64", "arm64"].includes(expectedArch))
    throw new Error("Container EXPECTED_ARCH must be amd64 or arm64.");
  const image = `streamskope:${manifest.version.replace("+", "_")}`;
  run("docker", [
    "buildx",
    "build",
    "--load",
    "--file",
    resolve("deployment/container/Dockerfile"),
    "--build-arg",
    `STREAMSKOPE_VERSION=${manifest.version}`,
    "--build-arg",
    `STREAMSKOPE_SOURCE_SHA=${revision}`,
    "--tag",
    image,
    ".",
  ]);
  const inspection = inspect(image, manifest.version, revision);
  if (args[0] === "--archive") {
    const startedAt = new Date().toISOString();
    const evidenceDirectory = resolve(".artifacts/ci");
    const evidenceFile = join(
      evidenceDirectory,
      `browser-data-preflight-${inspection.Architecture}.json`,
    );
    await rm(evidenceFile, { force: true });
    const output = resolve("dist/container-package");
    await mkdir(output, { recursive: true });
    const archive = join(
      output,
      `StreamSkope-${manifest.version}-container-linux-${inspection.Architecture}.tar.gz`,
    );
    const receipt = join(output, `container-linux-${inspection.Architecture}.json`);
    await rm(receipt, { force: true });
    await archiveImage(image, archive);
    run("docker", ["image", "load", "--input", archive]);
    if (inspect(image, manifest.version, revision).Id !== inspection.Id)
      throw new Error("Loaded container archive does not match the built image.");
    const preflight = await verifyBrowserContainer(image, {
      version: manifest.version,
      sourceRevision: revision,
      image,
      imageId: inspection.Id,
      platform: `${inspection.Os}/${inspection.Architecture}`,
    });
    await writeFile(
      join(output, `streamskope-${manifest.version}.clab.yml`),
      browserReleaseTopology(await readFile("streamskope.clab.yml", "utf8"), manifest.version),
    );
    await writeFile(
      receipt,
      JSON.stringify(
        {
          version: manifest.version,
          sourceRevision: revision,
          image,
          imageId: inspection.Id,
          platform: `${inspection.Os}/${inspection.Architecture}`,
          archive: basename(archive),
        },
        null,
        2,
      ) + "\n",
    );
    await mkdir(evidenceDirectory, { recursive: true });
    await writeFile(
      evidenceFile,
      `${JSON.stringify(
        {
          schemaVersion: 1,
          outcome: "passed",
          deliveryScope: "local-staged",
          version: manifest.version,
          sourceRevision: revision,
          platform: `${inspection.Os}/${inspection.Architecture}`,
          image: inspection.Id,
          execution: ciExecution(),
          startedAt,
          completedAt: new Date().toISOString(),
          preflight,
        },
        null,
        2,
      )}\n`,
    );
    process.stdout.write(`Built browser archive: ${archive}\n`);
  }
  process.stdout.write(`Built browser image: ${image}\n`);
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  void packageBrowserContainer(process.argv.slice(2)).catch((error: unknown) => {
    process.stderr.write(
      `Container packaging failed: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  });
