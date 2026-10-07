import { spawn } from "node:child_process";
import { mkdir, mkdtemp, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

import { readBoundedFile } from "../../src/platform/node/bounded-file";

import { validateBrowserImageArchive } from "./browser-release";
import {
  BROWSER_IMAGE_ARCHITECTURES,
  BROWSER_IMAGE_REPOSITORY,
  browserRegistryDigest,
  browserRegistryIdentity,
  browserRegistryObject,
  parseBrowserRegistryMetadata,
  type BrowserImageArchitecture,
  type BrowserRegistryMetadata,
} from "./browser-registry-metadata";

export interface BrowserRegistryOptions {
  readonly stagingRoot: string;
  readonly version: string;
  readonly sourceCommit: string;
  readonly runId: string;
  readonly outputFile: string;
}

export interface BrowserDockerResult {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
}

/** Tests substitute command results, never registry qualification or archive inspection. */
export type BrowserDockerRunner = (arguments_: readonly string[]) => Promise<BrowserDockerResult>;

interface NativeImage {
  readonly architecture: BrowserImageArchitecture;
  readonly archive: string;
  readonly imageId: string;
}

export interface BrowserRegistryCandidate {
  readonly reference: string;
  readonly metadata: BrowserRegistryMetadata;
}

const FORMAT_MANIFEST = "{{json .Manifest}}";
const FORMAT_IMAGE = "{{json .Image}}";
const FORMAT_LOCAL = "{{json .}}";
const INDEX_TYPES = [
  "application/vnd.oci.image.index.v1+json",
  "application/vnd.docker.distribution.manifest.list.v2+json",
];
const MANIFEST_TYPES = [
  "application/vnd.oci.image.manifest.v1+json",
  "application/vnd.docker.distribution.manifest.v2+json",
];

const runDocker: BrowserDockerRunner = async (arguments_) => {
  return new Promise((fulfill, reject) => {
    const child = spawn("docker", [...arguments_], { stdio: ["ignore", "pipe", "pipe"] });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    let bytes = 0;
    let failed = false;
    const fail = (): void => {
      if (failed) return;
      failed = true;
      clearTimeout(timer);
      child.kill("SIGKILL");
      reject(new Error("Docker registry command exceeded its execution or output bound."));
    };
    const capture = (chunks: Buffer[], chunk: Buffer): void => {
      bytes += chunk.length;
      if (bytes > 2 * 1024 * 1024) fail();
      else chunks.push(chunk);
    };
    const timer = setTimeout(fail, 10 * 60_000);
    child.stdout.on("data", (chunk: Buffer) => capture(stdout, chunk));
    child.stderr.on("data", (chunk: Buffer) => capture(stderr, chunk));
    child.once("error", () => {
      clearTimeout(timer);
      failed = true;
      reject(new Error("Docker registry command could not start."));
    });
    child.once("close", (code) => {
      clearTimeout(timer);
      if (!failed)
        fulfill({
          exitCode: code ?? 1,
          stdout: Buffer.concat(stdout).toString("utf8"),
          stderr: Buffer.concat(stderr).toString("utf8"),
        });
    });
  });
};

async function command(
  runner: BrowserDockerRunner,
  arguments_: readonly string[],
): Promise<string> {
  const result = await runner(arguments_);
  if (result.exitCode !== 0)
    throw new Error("Docker registry command failed; publication has not been qualified.");
  return result.stdout;
}

function json(text: string): Record<string, unknown> {
  try {
    return browserRegistryObject(JSON.parse(text));
  } catch {
    throw new Error("Docker returned invalid browser registry metadata.");
  }
}

function missingManifest(result: BrowserDockerResult, reference: string): boolean {
  // An authentication, network, or tool failure must never authorize replacing a tag.
  return (
    result.exitCode !== 0 &&
    !/(?:unauthorized|denied|forbidden|\b401\b|\b403\b)/iu.test(result.stderr) &&
    (/(?:manifest unknown|manifest_unknown)/iu.test(result.stderr) ||
      result.stderr.includes(`${reference}: not found`))
  );
}

async function descriptor(
  runner: BrowserDockerRunner,
  reference: string,
  allowMissing = false,
): Promise<Record<string, unknown> | null> {
  const result = await runner([
    "buildx",
    "imagetools",
    "inspect",
    reference,
    "--format",
    FORMAT_MANIFEST,
  ]);
  if (allowMissing && missingManifest(result, reference)) return null;
  if (result.exitCode !== 0)
    throw new Error("Registry inspection failed; an existing tag cannot safely be assumed absent.");
  const manifest = json(result.stdout);
  browserRegistryDigest(manifest.digest);
  return manifest;
}

function verifyConfig(
  value: Record<string, unknown>,
  native: NativeImage,
  options: BrowserRegistryOptions,
  local: boolean,
): void {
  const labels = browserRegistryObject(
    browserRegistryObject(value[local ? "Config" : "config"]).Labels,
  );
  if (
    value[local ? "Os" : "os"] !== "linux" ||
    value[local ? "Architecture" : "architecture"] !== native.architecture ||
    (local && value.Id !== native.imageId) ||
    labels["org.opencontainers.image.version"] !== options.version ||
    labels["org.opencontainers.image.revision"] !== options.sourceCommit
  )
    throw new Error(
      "Registry image does not match its qualified native archive, platform, or source.",
    );
}

async function verifyNative(
  runner: BrowserDockerRunner,
  reference: string,
  manifest: Record<string, unknown>,
  native: NativeImage,
  options: BrowserRegistryOptions,
): Promise<string> {
  if (typeof manifest.mediaType !== "string" || !MANIFEST_TYPES.includes(manifest.mediaType))
    throw new Error("A native browser tag must identify one image manifest, not an index.");
  const digest = browserRegistryDigest(manifest.digest);
  const immutable = `${BROWSER_IMAGE_REPOSITORY}@${digest}`;
  const raw = json(await command(runner, ["buildx", "imagetools", "inspect", immutable, "--raw"]));
  if (browserRegistryDigest(browserRegistryObject(raw.config).digest) !== native.imageId)
    throw new Error("Registry native config digest does not match the qualified archive image ID.");
  verifyConfig(
    json(
      await command(runner, [
        "buildx",
        "imagetools",
        "inspect",
        immutable,
        "--format",
        FORMAT_IMAGE,
      ]),
    ),
    native,
    options,
    false,
  );
  // Reinspect the tag after immutable validation to detect a concurrent replacement.
  if ((await descriptor(runner, reference))!.digest !== digest)
    throw new Error("Registry native tag changed while it was being qualified.");
  return digest;
}

async function qualifyArchives(options: BrowserRegistryOptions): Promise<NativeImage[]> {
  browserRegistryIdentity(options.version, options.sourceCommit);
  if (!/^[1-9][0-9]{0,19}$/u.test(options.runId))
    throw new Error("Browser registry publication requires the numeric workflow run ID.");
  const staging = resolve(options.stagingRoot);
  const output = resolve(options.outputFile);
  if (output === staging || output.startsWith(`${staging}${sep}`))
    throw new Error(
      "Browser registry receipts must be written outside the native archive staging root.",
    );
  return Promise.all(
    BROWSER_IMAGE_ARCHITECTURES.map(async (architecture) => {
      const directory = join(options.stagingRoot, `browser-linux-${architecture}`);
      const metadata: unknown = JSON.parse(
        (
          await readBoundedFile(
            join(directory, `container-linux-${architecture}.json`),
            128 * 1024,
            { rejectSymlinks: true },
          )
        ).toString("utf8"),
      );
      const archive = join(
        directory,
        `StreamSkope-${options.version}-container-linux-${architecture}.tar.gz`,
      );
      const receipt = await validateBrowserImageArchive(
        archive,
        metadata,
        options.version,
        options.sourceCommit,
        architecture,
      );
      return { architecture, archive, imageId: receipt.imageId };
    }),
  );
}

function candidateReference(options: BrowserRegistryOptions): string {
  const tag = `candidate-${options.version}-${options.sourceCommit.slice(0, 12)}-${options.runId}`;
  if (tag.length > 128) throw new Error("Browser candidate registry tag is too long.");
  return `${BROWSER_IMAGE_REPOSITORY}:${tag}`;
}

async function verifyIndex(
  runner: BrowserDockerRunner,
  reference: string,
  manifest: Record<string, unknown>,
  natives: readonly NativeImage[],
  options: BrowserRegistryOptions,
  expectedPlatforms?: BrowserRegistryMetadata["platforms"],
): Promise<BrowserRegistryMetadata> {
  if (
    manifest.schemaVersion !== 2 ||
    typeof manifest.mediaType !== "string" ||
    !INDEX_TYPES.includes(manifest.mediaType) ||
    !Array.isArray(manifest.manifests) ||
    manifest.manifests.length !== 2
  )
    throw new Error("Browser registry index must contain exactly the two qualified native images.");
  const children = manifest.manifests.map(browserRegistryObject);
  const platforms: BrowserRegistryMetadata["platforms"][number][] = [];
  for (const native of natives) {
    const matches = children.filter((child) => {
      const platform = browserRegistryObject(child.platform);
      return (
        platform.os === "linux" &&
        platform.architecture === native.architecture &&
        (platform.variant === undefined ||
          (native.architecture === "arm64" && platform.variant === "v8"))
      );
    });
    if (matches.length !== 1)
      throw new Error("Browser registry index has missing, duplicate, or unsupported platforms.");
    const child = matches[0]!;
    const digest = browserRegistryDigest(child.digest);
    const childReference = `${BROWSER_IMAGE_REPOSITORY}@${digest}`;
    const nativeDigest = await verifyNative(
      runner,
      childReference,
      (await descriptor(runner, childReference))!,
      native,
      options,
    );
    if (nativeDigest !== digest)
      throw new Error("Browser registry index does not identify the qualified native manifest.");
    const expected = expectedPlatforms?.find(
      (platform) => platform.platform === `linux/${native.architecture}`,
    );
    if (
      expectedPlatforms !== undefined &&
      (expected?.manifestDigest !== digest || expected.imageId !== native.imageId)
    )
      throw new Error("Browser registry index differs from its candidate receipt.");
    platforms.push({
      platform: `linux/${native.architecture}`,
      manifestDigest: digest,
      imageId: native.imageId,
    });
  }
  const digest = browserRegistryDigest(manifest.digest);
  if ((await descriptor(runner, reference))!.digest !== digest)
    throw new Error("Browser registry index changed while it was being qualified.");
  const image = `${BROWSER_IMAGE_REPOSITORY}:${options.version}`;
  return parseBrowserRegistryMetadata(
    {
      schemaVersion: 1,
      version: options.version,
      sourceRevision: options.sourceCommit,
      image,
      reference: `${image}@${digest}`,
      digest,
      platforms,
    },
    options.version,
    options.sourceCommit,
  );
}

/** Upload only validated archives; a candidate tag is not a published version. */
export async function publishBrowserRegistryCandidate(
  options: BrowserRegistryOptions,
  runner: BrowserDockerRunner = runDocker,
): Promise<BrowserRegistryCandidate> {
  const natives = await qualifyArchives(options);
  const stable = `${BROWSER_IMAGE_REPOSITORY}:${options.version}`;
  const existingStable = await descriptor(runner, stable, true);
  if (existingStable !== null) await verifyIndex(runner, stable, existingStable, natives, options);
  const platforms: BrowserRegistryMetadata["platforms"][number][] = [];
  for (const native of natives) {
    const reference = `${BROWSER_IMAGE_REPOSITORY}:${options.version}-${native.architecture}`;
    let existing = await descriptor(runner, reference, true);
    if (existing === null) {
      await command(runner, ["image", "load", "--input", native.archive]);
      verifyConfig(
        json(
          await command(runner, [
            "image",
            "inspect",
            `streamskope:${options.version}`,
            "--format",
            FORMAT_LOCAL,
          ]),
        ),
        native,
        options,
        true,
      );
      // Tag the inspected config ID, rather than a mutable local image name.
      await command(runner, ["image", "tag", native.imageId, reference]);
      // An immediate second check avoids replacing a tag created during archive load.
      existing = await descriptor(runner, reference, true);
      if (existing === null) {
        await command(runner, ["image", "push", reference]);
        existing = await descriptor(runner, reference);
      }
    }
    const digest = await verifyNative(runner, reference, existing!, native, options);
    platforms.push({
      platform: `linux/${native.architecture}`,
      manifestDigest: digest,
      imageId: native.imageId,
    });
  }
  const reference = candidateReference(options);
  let index = await descriptor(runner, reference, true);
  if (index === null) {
    await command(runner, [
      "buildx",
      "imagetools",
      "create",
      "--tag",
      reference,
      ...platforms.map((platform) => `${BROWSER_IMAGE_REPOSITORY}@${platform.manifestDigest}`),
    ]);
    index = await descriptor(runner, reference);
  }
  return {
    reference,
    metadata: await verifyIndex(runner, reference, index!, natives, options, platforms),
  };
}

async function anonymousPulls(
  runner: BrowserDockerRunner,
  candidate: BrowserRegistryCandidate,
  natives: readonly NativeImage[],
  options: BrowserRegistryOptions,
): Promise<void> {
  const configuration = await mkdtemp(join(tmpdir(), "streamskope-public-pull-"));
  try {
    await writeFile(join(configuration, "config.json"), "{}\n", { mode: 0o600 });
    for (const native of natives) {
      const platform = candidate.metadata.platforms.find(
        (entry) => entry.platform === `linux/${native.architecture}`,
      )!;
      // Classic Docker stores cannot bind one index digest to two platform images.
      // Pull the index once and its verified second child directly; both are anonymous.
      const immutable = `${BROWSER_IMAGE_REPOSITORY}@${native.architecture === "amd64" ? candidate.metadata.digest : platform.manifestDigest}`;
      const prefix = ["--config", configuration];
      const result = await runner([
        ...prefix,
        "image",
        "pull",
        "--platform",
        `linux/${native.architecture}`,
        immutable,
      ]);
      if (result.exitCode !== 0)
        throw new Error(
          "Anonymous browser image pull failed. Make the GHCR package public, then rerun this workflow; no stable version has been promoted.",
        );
      verifyConfig(
        json(
          await command(runner, [
            ...prefix,
            "image",
            "inspect",
            immutable,
            "--format",
            FORMAT_LOCAL,
          ]),
        ),
        native,
        options,
        true,
      );
    }
  } finally {
    await rm(configuration, { force: true, recursive: true });
  }
}

async function writeReceipt(path: string, receipt: BrowserRegistryMetadata): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const temporary = await mkdtemp(join(dirname(path), ".browser-registry-"));
  try {
    const file = join(temporary, "registry.json");
    await writeFile(file, `${JSON.stringify(receipt, null, 2)}\n`, { flag: "wx", mode: 0o644 });
    await rename(file, path);
  } finally {
    await rm(temporary, { force: true, recursive: true });
  }
}

/** Only an anonymously pullable, exactly qualified candidate may receive the version tag. */
export async function promoteBrowserRegistryCandidate(
  options: BrowserRegistryOptions,
  candidate: BrowserRegistryCandidate,
  runner: BrowserDockerRunner = runDocker,
): Promise<BrowserRegistryMetadata> {
  const natives = await qualifyArchives(options);
  const metadata = parseBrowserRegistryMetadata(
    candidate.metadata,
    options.version,
    options.sourceCommit,
  );
  if (candidate.reference !== candidateReference(options))
    throw new Error("Browser registry candidate does not match this workflow run.");
  const immutableCandidate = `${BROWSER_IMAGE_REPOSITORY}@${metadata.digest}`;
  const current = await verifyIndex(
    runner,
    immutableCandidate,
    (await descriptor(runner, immutableCandidate))!,
    natives,
    options,
    metadata.platforms,
  );
  if (current.digest !== metadata.digest)
    throw new Error("Browser candidate digest changed before promotion.");
  await anonymousPulls(runner, candidate, natives, options);
  const existing = await descriptor(runner, metadata.image, true);
  if (existing !== null) {
    if (existing.digest !== metadata.digest)
      throw new Error(
        "Browser release version already exists with a different digest; it will not be overwritten.",
      );
  } else {
    // The release workflow must serialize publishers: registries have no create-only tag API.
    await command(runner, [
      "buildx",
      "imagetools",
      "create",
      "--tag",
      metadata.image,
      immutableCandidate,
    ]);
  }
  const final = await verifyIndex(
    runner,
    metadata.image,
    (await descriptor(runner, metadata.image))!,
    natives,
    options,
    metadata.platforms,
  );
  if (final.digest !== metadata.digest)
    throw new Error("Promoted browser version does not retain the qualified candidate digest.");
  await writeReceipt(options.outputFile, final);
  return final;
}

async function main(): Promise<void> {
  const [action, stagingRoot, version, sourceCommit, runId, outputFile, ...extra] =
    process.argv.slice(2);
  if (
    action !== "publish" ||
    stagingRoot === undefined ||
    version === undefined ||
    sourceCommit === undefined ||
    runId === undefined ||
    outputFile === undefined ||
    extra.length !== 0
  )
    throw new Error(
      "Usage: browser-registry.ts publish <staging-root> <version> <source-commit> <run-id> <registry.json>",
    );
  const options = { stagingRoot, version, sourceCommit, runId, outputFile };
  const candidate = await publishBrowserRegistryCandidate(options);
  const receipt = await promoteBrowserRegistryCandidate(options, candidate);
  process.stdout.write(`Qualified public browser image ${receipt.reference}.\n`);
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  void main().catch((error: unknown) => {
    process.stderr.write(
      `${error instanceof Error ? error.message : "Browser registry publication failed."}\n`,
    );
    process.exitCode = 1;
  });
